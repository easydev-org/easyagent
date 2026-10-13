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

// adminScope returns the caller's scope and whether the caller is an admin at
// all. scope "*" = platform (every subtree); any other value = a delegated
// admin limited to that subtree root.
func adminScope(ctx context.Context) (string, bool) {
	s := scopeOf(ctx)
	if s == "" {
		return "", false
	}
	return s, true
}

// scopeAllows reports whether an admin scope may manage tenant id.
func (s *Server) scopeAllows(ctx context.Context, tenantID string) (bool, error) {
	scope, ok := adminScope(ctx)
	if !ok {
		return false, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	if scope == "*" {
		return true, nil
	}
	in, err := s.db.TenantInScope(scope, tenantID)
	if err != nil {
		return false, connect.NewError(connect.CodeInternal, err.Error())
	}
	return in, nil
}

// ListTenants lists tenants.
func (s *Server) ListTenants(ctx context.Context, _ *agentv1.ListTenantsRequest) (*agentv1.ListTenantsResponse, error) {
	scope, ok := adminScope(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	var rows []store.Tenant
	var err error
	if scope == "*" {
		rows, err = s.db.ListTenants()
	} else {
		rows, err = s.db.ListTenantsInScope(scope)
	}
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
	scope, ok := adminScope(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	var id string
	var parent *string
	if scope == "*" {
		// Platform admin: caller supplies the id; the tenant is a root.
		if req.Id == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, "id is required")
		}
		id = req.Id
	} else {
		// Delegated admin: the id is SERVER-GENERATED under the scope root
		// (`<scope>-<short-random>`), so two downstreams can each create a
		// "proj" without a global-id collision. The caller supplies only a name.
		parent = &scope
		id = scope + "-" + shortID()
	}
	if err := s.db.CreateTenantScoped(id, req.Name, parent, scopeOrDefault(scope, id)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	plaintext := mintToken()
	if _, err := s.db.CreateToken(id, sha256Hex(plaintext), "tenant"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// Seed ALL deployment-pinned defaults for the runtime-created tenant (boot
	// seeding only covers tenants known at startup). Without this a fresh
	// tenant has no provider and its first turn fails with "no model selected".
	if s.seedTenant != nil {
		s.seedTenant(id)
	}
	row, _, _ := s.db.GetTenant(id)
	return &agentv1.CreateTenantResponse{Tenant: tenantToProto(*row), Token: plaintext}, nil
}

// scopeOrDefault returns the subtree root for a new tenant: the scope when the
// creator is delegated, else the tenant's own id (it is a root).
func scopeOrDefault(scope, id string) string {
	if scope == "*" || scope == "" {
		return id
	}
	return scope
}

// UpdateTenant patches a tenant.
func (s *Server) UpdateTenant(ctx context.Context, req *agentv1.UpdateTenantRequest) (*agentv1.UpdateTenantResponse, error) {
	if ok, err := s.scopeAllows(ctx, req.Id); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
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
	if ok, err := s.scopeAllows(ctx, req.Id); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
	}
	if err := s.db.DeleteTenant(req.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.DeleteTenantResponse{Ok: true}, nil
}

// IssueTenantToken mints a new token (plaintext shown once).
func (s *Server) IssueTenantToken(ctx context.Context, req *agentv1.IssueTenantTokenRequest) (*agentv1.IssueTenantTokenResponse, error) {
	if ok, err := s.scopeAllows(ctx, req.TenantId); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
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
	if ok, err := s.scopeAllows(ctx, req.TenantId); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
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
	if _, ok := adminScope(ctx); !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	tok, ok, err := s.db.TokenByID(req.TokenId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "token not found")
	}
	if ok, err := s.scopeAllows(ctx, tok.TenantID); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
	}
	if err := s.db.RevokeToken(req.TokenId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RevokeTenantTokenResponse{Ok: true}, nil
}

// RotateTenantToken revokes the old token and mints a new one.
func (s *Server) RotateTenantToken(ctx context.Context, req *agentv1.RotateTenantTokenRequest) (*agentv1.RotateTenantTokenResponse, error) {
	tok, ok, err := s.db.TokenByID(req.TokenId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "token not found")
	}
	tenantID, label := tok.TenantID, tok.Label
	if ok, err := s.scopeAllows(ctx, tenantID); err != nil {
		return nil, err
	} else if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "tenant is outside your scope")
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
	out := &agentv1.Tenant{
		Id: t.ID, Name: t.Name, Disabled: t.Disabled,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
		ScopeId: t.ScopeID,
	}
	if t.ParentID != nil {
		out.ParentId = *t.ParentID
	}
	return out
}

// ---- Delegated admin tokens (re-delegation) ----

// CreateAdminToken mints a delegated-admin token for a subtree. A platform
// admin may scope it to ANY tenant; a delegated admin only to its OWN scope.
func (s *Server) CreateAdminToken(ctx context.Context, req *agentv1.CreateAdminTokenRequest) (*agentv1.CreateAdminTokenResponse, error) {
	scope, ok := adminScope(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	target := req.ScopeTenantId
	if target == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "scope_tenant_id is required")
	}
	if scope != "*" && target != scope {
		return nil, connect.NewError(connect.CodePermissionDenied, "cannot delegate outside your scope")
	}
	if exists, err := s.db.TenantExists(target); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !exists {
		return nil, connect.NewError(connect.CodeNotFound, "scope tenant not found")
	}
	plaintext := mintToken()
	id, err := s.db.CreateAdminToken(target, sha256Hex(plaintext), req.Label)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.CreateAdminTokenResponse{
		Token:     &agentv1.AdminToken{TokenId: id, ScopeTenantId: target, Label: req.Label},
		Plaintext: plaintext,
	}, nil
}

// ListAdminTokens lists the delegated admin tokens under a scope.
func (s *Server) ListAdminTokens(ctx context.Context, req *agentv1.ListAdminTokensRequest) (*agentv1.ListAdminTokensResponse, error) {
	scope, ok := adminScope(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	target := req.ScopeTenantId
	if scope != "*" && target != scope {
		return nil, connect.NewError(connect.CodePermissionDenied, "cannot list outside your scope")
	}
	rows, err := s.db.ListAdminTokens(target)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.AdminToken, 0, len(rows))
	for _, r := range rows {
		out = append(out, adminTokenToProto(r))
	}
	return &agentv1.ListAdminTokensResponse{Tokens: out}, nil
}

// RevokeAdminToken revokes a delegated admin token (scope-checked).
func (s *Server) RevokeAdminToken(ctx context.Context, req *agentv1.RevokeAdminTokenRequest) (*agentv1.RevokeAdminTokenResponse, error) {
	scope, ok := adminScope(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodePermissionDenied, "admin token required")
	}
	tok, ok, err := s.db.AdminTokenByID(req.TokenId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "admin token not found")
	}
	found := tok.ScopeTenant
	if scope != "*" && found != scope {
		return nil, connect.NewError(connect.CodePermissionDenied, "token is outside your scope")
	}
	if err := s.db.RevokeAdminToken(req.TokenId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RevokeAdminTokenResponse{Ok: true}, nil
}

func adminTokenToProto(t store.AdminToken) *agentv1.AdminToken {
	out := &agentv1.AdminToken{
		TokenId: t.TokenID, ScopeTenantId: t.ScopeTenant, Label: t.Label,
		CreatedAt: t.CreatedAt, Revoked: t.RevokedAt != nil && *t.RevokedAt != "",
	}
	if t.LastUsedAt != nil {
		out.LastUsedAt = *t.LastUsedAt
	}
	return out
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

// shortID is a short, URL-safe random suffix for a server-generated child
// tenant id (so two downstreams can each create a "proj").
func shortID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// mintToken mints a fresh opaque token (32 random bytes, base64url, no padding).
func mintToken() string {
	var b [32]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}
