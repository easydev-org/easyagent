package server

import (
	"context"
	"strings"
	"testing"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// Delegated administration (P1): a subtree admin manages only its own subtree;
// the platform admin sees everything. Ported from the reference's
// delegation.test.ts.

func TestDelegationScopeNarrowsListTenants(t *testing.T) {
	ts, _ := newTestServer(t)
	admin := adminWithToken(ts, adminToken)
	ctx := context.Background()

	// Platform admin creates two subtree roots.
	if _, err := admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downA", Name: "A"}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downB", Name: "B"}); err != nil {
		t.Fatal(err)
	}
	// Platform admin mints a delegated admin token for downA's subtree.
	at, err := admin.CreateAdminToken(ctx, &agentv1.CreateAdminTokenRequest{ScopeTenantId: "downA", Label: "sub"})
	if err != nil {
		t.Fatal(err)
	}
	if at.Plaintext == "" || at.Token.ScopeTenantId != "downA" {
		t.Fatalf("createAdminToken = %+v", at)
	}

	sub := adminWithToken(ts, at.Plaintext)
	// The delegated admin creates a tenant: the id is SERVER-GENERATED under
	// its scope, and it is NOT allowed to pass an explicit id.
	c, err := sub.CreateTenant(ctx, &agentv1.CreateTenantRequest{Name: "proj"})
	if err != nil {
		t.Fatal(err)
	}
	childID := c.Tenant.Id
	if !strings.HasPrefix(childID, "downA-") {
		t.Fatalf("child id %q is not scoped under downA", childID)
	}
	if c.Tenant.ScopeId != "downA" || c.Tenant.ParentId != "downA" {
		t.Fatalf("child scope/parent = %q/%q, want downA/downA", c.Tenant.ScopeId, c.Tenant.ParentId)
	}

	// The delegated admin's list is narrowed to its subtree.
	subList, err := sub.ListTenants(ctx, &agentv1.ListTenantsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tn := range subList.Tenants {
		if tn.ScopeId != "downA" {
			t.Fatalf("delegated admin saw out-of-scope tenant %q (scope %q)", tn.Id, tn.ScopeId)
		}
	}
	if len(subList.Tenants) != 2 { // downA itself + the child
		t.Fatalf("delegated list = %d, want 2", len(subList.Tenants))
	}

	// The platform admin sees everything.
	all, _ := admin.ListTenants(ctx, &agentv1.ListTenantsRequest{})
	if len(all.Tenants) < 3 {
		t.Fatalf("platform list = %d, want >= 3", len(all.Tenants))
	}
}

func TestDelegationScopeBlocksForeignTenants(t *testing.T) {
	ts, _ := newTestServer(t)
	admin := adminWithToken(ts, adminToken)
	ctx := context.Background()
	_, _ = admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downA", Name: "A"})
	_, _ = admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downB", Name: "B"})
	at, _ := admin.CreateAdminToken(ctx, &agentv1.CreateAdminTokenRequest{ScopeTenantId: "downA", Label: "subA"})
	sub := adminWithToken(ts, at.Plaintext)

	// Managing a tenant OUTSIDE the scope is PermissionDenied.
	if _, err := sub.UpdateTenant(ctx, &agentv1.UpdateTenantRequest{Id: "downB"}); err == nil {
		t.Fatal("delegated admin modified a foreign tenant")
	}
	if _, err := sub.DeleteTenant(ctx, &agentv1.DeleteTenantRequest{Id: "downB"}); err == nil {
		t.Fatal("delegated admin deleted a foreign tenant")
	}
	if _, err := sub.IssueTenantToken(ctx, &agentv1.IssueTenantTokenRequest{TenantId: "downB"}); err == nil {
		t.Fatal("delegated admin issued a token for a foreign tenant")
	}
	// Re-delegation outside the scope is rejected.
	if _, err := sub.CreateAdminToken(ctx, &agentv1.CreateAdminTokenRequest{ScopeTenantId: "downB"}); err == nil {
		t.Fatal("delegated admin re-delegated outside its scope")
	}
	// Inside the scope is fine.
	if _, err := sub.UpdateTenant(ctx, &agentv1.UpdateTenantRequest{Id: "downA"}); err != nil {
		t.Fatalf("delegated admin could not manage its own scope: %v", err)
	}
}

func TestDelegatedAdminTokenAuth(t *testing.T) {
	ts, db := newTestServer(t)
	admin := adminWithToken(ts, adminToken)
	ctx := context.Background()
	_, _ = admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downA"})
	at, _ := admin.CreateAdminToken(ctx, &agentv1.CreateAdminTokenRequest{ScopeTenantId: "downA", Label: "subA"})

	// The delegated token resolves to role admin (GetIdentity).
	id, err := clientFor(t, ts, at.Plaintext).GetIdentity(ctx, &agentv1.GetIdentityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if id.Role != "admin" {
		t.Fatalf("delegated token role = %q, want admin", id.Role)
	}
	// A tenant token still CANNOT reach AdminService.
	tok := seedTenant(t, db, "plain")
	ten := adminWithToken(ts, tok)
	if _, err := ten.ListTenants(ctx, &agentv1.ListTenantsRequest{}); err == nil {
		t.Fatal("a tenant token reached AdminService")
	}
}

func TestRevokeAdminToken(t *testing.T) {
	ts, _ := newTestServer(t)
	admin := adminWithToken(ts, adminToken)
	ctx := context.Background()
	_, _ = admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "downA"})
	at, _ := admin.CreateAdminToken(ctx, &agentv1.CreateAdminTokenRequest{ScopeTenantId: "downA", Label: "subA"})

	sub := adminWithToken(ts, at.Plaintext)
	if _, err := sub.ListTenants(ctx, &agentv1.ListTenantsRequest{}); err != nil {
		t.Fatalf("delegated token should work before revoke: %v", err)
	}
	if _, err := admin.RevokeAdminToken(ctx, &agentv1.RevokeAdminTokenRequest{TokenId: at.Token.TokenId}); err != nil {
		t.Fatal(err)
	}
	// After revocation the token no longer authenticates as admin.
	if _, err := sub.ListTenants(ctx, &agentv1.ListTenantsRequest{}); err == nil {
		t.Fatal("revoked admin token still authorized")
	}
}

// scope_id backfill: every pre-existing tenant becomes its own root.
func TestTenantScopeBackfill(t *testing.T) {
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTenant("root1", "R1"); err != nil {
		t.Fatal(err)
	}
	tt, ok, _ := db.GetTenant("root1")
	if !ok || tt.ScopeID != "root1" || tt.ParentID != nil {
		t.Fatalf("root tenant scope = %q parent=%v, want root1/nil", tt.ScopeID, tt.ParentID)
	}
}
