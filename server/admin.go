package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	"connectrpc.com/connect/v2"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// requireAdmin rejects a non-admin caller (the auth interceptor pins an empty
// tenant for the admin token).
func requireAdmin(ctx context.Context) error {
	if tenantOf(ctx) == "" {
		return nil
	}
	return connect.NewError(connect.CodePermissionDenied, "admin token required")
}

// ListTenants lists tenants.
func (s *Server) ListTenants(ctx context.Context, _ *agentv1.ListTenantsRequest) (*agentv1.ListTenantsResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.ListTenants()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.Tenant, 0, len(rows))
	for _, r := range rows {
		out = append(out, tenantToProto(r))
	}
	return &agentv1.ListTenantsResponse{Tenants: out}, nil
}

// CreateTenant creates a tenant + mints its access token (the ONLY way to
// create a tenant; there is no implicit/bootstrap tenant).
func (s *Server) CreateTenant(ctx context.Context, req *agentv1.CreateTenantRequest) (*agentv1.CreateTenantResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if req.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "id is required")
	}
	if err := s.db.CreateTenant(req.Id, req.Name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	plaintext := mintToken()
	if _, err := s.db.CreateToken(req.Id, sha256Hex(plaintext), "tenant"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// Seed ALL deployment-pinned defaults for the runtime-created tenant (boot
	// seeding only covers tenants known at startup). Without this a fresh
	// tenant has no provider and its first turn fails with "no model selected".
	if s.seedTenant != nil {
		s.seedTenant(req.Id)
	}
	row, _, _ := s.db.GetTenant(req.Id)
	return &agentv1.CreateTenantResponse{Tenant: tenantToProto(*row), Token: plaintext}, nil
}

// UpdateTenant patches a tenant.
func (s *Server) UpdateTenant(ctx context.Context, req *agentv1.UpdateTenantRequest) (*agentv1.UpdateTenantResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	var name *string
	if req.Name != nil {
		name = req.Name
	}
	var disabled *bool
	if req.Disabled != nil {
		disabled = req.Disabled
	}
	if err := s.db.UpdateTenant(req.Id, name, disabled); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row, ok, _ := s.db.GetTenant(req.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "tenant not found")
	}
	return &agentv1.UpdateTenantResponse{Tenant: tenantToProto(*row)}, nil
}

// DeleteTenant removes a tenant.
func (s *Server) DeleteTenant(ctx context.Context, req *agentv1.DeleteTenantRequest) (*agentv1.DeleteTenantResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := s.db.DeleteTenant(req.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.DeleteTenantResponse{Ok: true}, nil
}

// IssueTenantToken mints a new token (plaintext shown once).
func (s *Server) IssueTenantToken(ctx context.Context, req *agentv1.IssueTenantTokenRequest) (*agentv1.IssueTenantTokenResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	plaintext := mintToken()
	id, err := s.db.CreateToken(req.TenantId, sha256Hex(plaintext), req.Label)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.IssueTenantTokenResponse{
		Token:     &agentv1.TenantToken{TokenId: id, TenantId: req.TenantId, Label: req.Label},
		Plaintext: plaintext,
	}, nil
}

// ListTenantTokens lists a tenant's tokens.
func (s *Server) ListTenantTokens(ctx context.Context, req *agentv1.ListTenantTokensRequest) (*agentv1.ListTenantTokensResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.ListTokens(req.TenantId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.TenantToken, 0, len(rows))
	for _, r := range rows {
		out = append(out, tokenToProto(r))
	}
	return &agentv1.ListTenantTokensResponse{Tokens: out}, nil
}

// RevokeTenantToken revokes a token.
func (s *Server) RevokeTenantToken(ctx context.Context, req *agentv1.RevokeTenantTokenRequest) (*agentv1.RevokeTenantTokenResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	if err := s.db.RevokeToken(req.TokenId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RevokeTenantTokenResponse{Ok: true}, nil
}

// RotateTenantToken revokes the old token and mints a new one.
func (s *Server) RotateTenantToken(ctx context.Context, req *agentv1.RotateTenantTokenRequest) (*agentv1.RotateTenantTokenResponse, error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	rows, _ := s.db.ListTokens("")
	var tenantID, label string
	for _, r := range rows {
		if r.TokenID == req.TokenId {
			tenantID, label = r.TenantID, r.Label
		}
	}
	if tenantID == "" {
		// Look it up directly across tenants is not exposed; require the caller
		// to pass a known token id (the old token is revoked regardless).
		return nil, connect.NewError(connect.CodeNotFound, "token not found")
	}
	if err := s.db.RevokeToken(req.TokenId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	plaintext := mintToken()
	id, err := s.db.CreateToken(tenantID, sha256Hex(plaintext), label)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RotateTenantTokenResponse{
		Token:     &agentv1.TenantToken{TokenId: id, TenantId: tenantID, Label: label},
		Plaintext: plaintext,
	}, nil
}

func tenantToProto(t store.Tenant) *agentv1.Tenant {
	return &agentv1.Tenant{
		Id: t.ID, Name: t.Name, Disabled: t.Disabled,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
	}
}

func tokenToProto(t store.TenantToken) *agentv1.TenantToken {
	out := &agentv1.TenantToken{
		TokenId: t.TokenID, TenantId: t.TenantID, Label: t.Label,
		CreatedAt: t.CreatedAt, Revoked: t.RevokedAt != nil && *t.RevokedAt != "",
	}
	if t.LastUsedAt != nil {
		out.LastUsedAt = *t.LastUsedAt
	}
	return out
}

// mintToken mints a fresh opaque token (32 random bytes, base64url, no padding).
func mintToken() string {
	var b [32]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
