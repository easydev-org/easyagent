package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"strings"

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

// Login verifies a (username, token) pair. The admin username is the configured
// platform admin name; a tenant username is the tenant id (or its name). A
// delegated admin token logs in under its subtree root's name/id.
func (s *Server) Login(ctx context.Context, req *agentv1.LoginRequest) (*agentv1.LoginResponse, error) {
	username := strings.TrimSpace(req.Username)
	token := req.Token
	if token == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "token is required")
	}
	// Platform admin: username must match the configured admin name.
	if s.cfg.AdminToken != "" && constantTimeEqual(token, s.cfg.AdminToken) {
		if !strings.EqualFold(username, s.cfg.AdminUsername) {
			return nil, connect.NewError(connect.CodeUnauthenticated, "invalid username or token")
		}
		return &agentv1.LoginResponse{Role: "admin", Scope: "*"}, nil
	}
	sha := sha256Hex(token)
	// Delegated admin token: the username is the subtree root (id or name).
	if scope, atok, ok, err := s.db.AdminTokenBySHA(sha); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if ok {
		_ = s.db.TouchAdminToken(atok.TokenID)
		if !s.usernameMatchesTenant(username, scope) {
			return nil, connect.NewError(connect.CodeUnauthenticated, "invalid username or token")
		}
		// Mint the SESSION credential the client will actually present: the
		// username+token pair is only ever exchanged here, so a guessable token
		// never travels on ordinary RPCs.
		sess, err := s.mintSession("admin", "", scope, username)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		return &agentv1.LoginResponse{Role: "admin", Scope: scope, SessionToken: sess}, nil
	}
	// Tenant token: the username is the tenant id or name.
	ten, tok, ok, err := s.db.TenantByTokenSHA(sha)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok || ten.Disabled {
		return nil, connect.NewError(connect.CodeUnauthenticated, "invalid username or token")
	}
	if !strings.EqualFold(username, ten.ID) && !strings.EqualFold(username, ten.Name) {
		return nil, connect.NewError(connect.CodeUnauthenticated, "invalid username or token")
	}
	_ = s.db.TouchToken(tok.TokenID)
	name := ten.Name
	if name == "" {
		name = ten.ID
	}
	sess, err := s.mintSession("tenant", ten.ID, "", username)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.LoginResponse{Role: "tenant", Tenant: ten.ID, TenantName: name, SessionToken: sess}, nil
}

// mintSession issues the random, GLOBALLY UNIQUE credential a client presents
// on every later RPC. Only its sha256 is stored.
func (s *Server) mintSession(role, tenantID, scope, label string) (string, error) {
	plaintext := mintToken()
	if _, err := s.db.CreateSessionToken(sha256Hex(plaintext), role, tenantID, scope, label); err != nil {
		return "", err
	}
	return plaintext, nil
}

// claimTokenLabel validates a label that will BE the credential, and returns it.
//
// The label is now the secret a human types at login, and the login lookup is by
// token alone (store.TenantByTokenSHA / AdminTokenBySHA both query sha256 and
// take the first row). Two tenants sharing a label would therefore resolve to
// whichever row came first — whoever logs in second would land in the OTHER
// tenant's account. Requiring global uniqueness is what makes a label-as-secret
// safe; there is deliberately no strength rule, because the label is expected to
// be short and human-chosen.
func (s *Server) claimTokenLabel(ctx context.Context, label string) (string, error) {
	label = strings.TrimSpace(label)
	if label == "" {
		return "", connect.NewError(connect.CodeInvalidArgument,
			"label is required: it is the token you will type at login")
	}
	// Collide with a TENANT token?
	if _, _, ok, err := s.db.TenantByTokenSHA(sha256Hex(label)); err != nil {
		return "", connect.NewError(connect.CodeInternal, err.Error())
	} else if ok {
		return "", connect.NewError(connect.CodeAlreadyExists,
			"this label is already a token; choose a different one (labels are the login secret and must be unique)")
	}
	// Collide with a DELEGATED-ADMIN token?
	if _, _, ok, err := s.db.AdminTokenBySHA(sha256Hex(label)); err != nil {
		return "", connect.NewError(connect.CodeInternal, err.Error())
	} else if ok {
		return "", connect.NewError(connect.CodeAlreadyExists,
			"this label is already a sub-admin token; choose a different one")
	}
	return label, nil
}

// usernameMatchesTenant reports whether username is the tenant's id or name.
func (s *Server) usernameMatchesTenant(username, id string) bool {
	if strings.EqualFold(username, id) {
		return true
	}
	if t, ok, _ := s.db.GetTenant(id); ok && t.Name != "" && strings.EqualFold(username, t.Name) {
		return true
	}
	return false
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
	// Resolve the subtree root this new tenant belongs to.
	rootScope := ""
	var parent *string
	if req.ParentId != "" {
		// Creating a USER inside an existing domain: the parent must be within
		// the caller's scope (a delegated admin: its own scope only).
		if scope != "*" && req.ParentId != scope {
			return nil, connect.NewError(connect.CodePermissionDenied, "cannot create outside your scope")
		}
		pt, ok, err := s.db.GetTenant(req.ParentId)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		if !ok {
			return nil, connect.NewError(connect.CodeNotFound, "parent tenant not found")
		}
		rootScope = pt.ScopeID
		if rootScope == "" {
			rootScope = pt.ID
		}
		parent = &req.ParentId
	} else if scope != "*" {
		// Delegated admin creating a root of... no: it creates a user under its
		// own scope root.
		rootScope = scope
		parent = &scope
	}
	// The id: a platform admin may supply it; otherwise (a delegated admin, or
	// an omitted id) it is SERVER-GENERATED under the subtree root.
	id := req.Id
	if scope != "*" && id != "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "a delegated admin does not supply the id")
	}
	if id == "" {
		if rootScope == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, "id is required")
		}
		id = rootScope + "-" + shortID()
	}
	if err := s.db.CreateTenantScoped(id, req.Name, parent, rootScope); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// The token a human will type is the tenant's own id (e.g. "acme"): the
	// label IS the credential, so there is no second secret to lose.
	plaintext := id
	if _, err := s.db.CreateToken(id, sha256Hex(plaintext), plaintext); err != nil {
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
	// The LABEL is the credential. It must be globally unique because the login
	// lookup is by token alone (see store.TenantByTokenSHA) — two tenants sharing
	// a label would resolve to whichever row came first.
	plaintext, err := s.claimTokenLabel(ctx, req.Label)
	if err != nil {
		return nil, err
	}
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

// SetTenantTokenLabel renames a tenant token (scope-checked).
func (s *Server) SetTenantTokenLabel(ctx context.Context, req *agentv1.SetTenantTokenLabelRequest) (*agentv1.SetTenantTokenLabelResponse, error) {
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
	if err := s.db.RenameToken(req.TokenId, req.Label); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	tok, _, _ = s.db.TokenByID(req.TokenId)
	return &agentv1.SetTenantTokenLabelResponse{Token: tokenToProto(*tok)}, nil
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
	// Rotating a label-as-credential token KEEPS the label (it is what the user
	// knows) and replaces the row. The old row was just deleted, so the label is
	// free again.
	plaintext, err := s.claimTokenLabel(ctx, label)
	if err != nil {
		return nil, err
	}
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
	// The LABEL is the credential (see claimTokenLabel): it must be unique
	// across every tenant and admin token, because login resolves by token alone.
	plaintext, err := s.claimTokenLabel(ctx, req.GetLabel())
	if err != nil {
		return nil, err
	}
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
