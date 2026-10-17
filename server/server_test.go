package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

const adminToken = "admin-secret"

// seededFor records the tenants CreateTenant seeded (set by newTestServer).
var seededFor []string

func newTestServer(t *testing.T) (*httptest.Server, *store.DB) {
	ts, db, _ := newTestServerWithSrv(t)
	return ts, db
}

// newTestServerWithSrv also returns the *Server so a test can install stubs.
func newTestServerWithSrv(t *testing.T) (*httptest.Server, *store.DB, *Server) {
	t.Helper()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	kv := agent.NewMemoryKV()
	presets := agent.NewPresets(kv)
	presets.SeedSystem("default", agent.PresetRow{ID: "default", SystemPrompt: "You are a helpful assistant."})
	hub := NewEventHub()
	rt := agent.NewRuntime(agent.RuntimeDeps{
		DB: db, Lease: agent.NewMemoryLease(), Emit: hub.Emitter("", ""), Presets: agent.PresetStore{P: presets},
	})
	srv := New(Deps{
		DB: db, Runtime: rt, Hub: hub,
		Config:   Config{AuthMode: "required", AdminToken: adminToken, DefaultTenant: "default"},
		Presets:  presets,
		ConfigKV: agent.NewConfig(kv),
	})
	// A runtime-created tenant is seeded (mirrors the TS seedTenantDefaults):
	// record the tenants the seeder was invoked for.
	seededFor = nil
	srv.SetTenantSeeder(func(tenant string) { seededFor = append(seededFor, tenant) })

	mux := http.NewServeMux()
	cs := connect.NewServer(srv.AuthInterceptor())
	srv.Register(cs)
	connecthttp.Mount(mux, cs, connecthttp.WithReadMaxBytes(0))
	ts := httptest.NewUnstartedServer(mux)
	proto := new(http.Protocols)
	proto.SetHTTP1(true)
	proto.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = proto
	ts.Start()
	t.Cleanup(ts.Close)
	return ts, db, srv
}

func clientFor(t *testing.T, ts *httptest.Server, token string) agentv1connect.AgentServiceClient {
	t.Helper()
	hc := &http.Client{}
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			if token != "" {
				var info *connect.CallInfo
				ctx, info = connect.NewClientContext(ctx)
				info.RequestHeader().Set("Authorization", "Bearer "+token)
			}
			return next(ctx, spec)
		}
	}
	return agentv1connect.NewAgentServiceClient(
		connect.NewClient(connecthttp.NewTransport(hc, ts.URL, connecthttp.WithReadMaxBytes(0)), interceptor),
	)
}

// seedTenant creates a tenant + returns a token for it.
func seedTenant(t *testing.T, db *store.DB, id string) string {
	t.Helper()
	if err := db.CreateTenant(id, id); err != nil {
		t.Fatal(err)
	}
	tok := "tok-" + id
	if _, err := db.CreateToken(id, sha256Hex(tok), "test"); err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestRPCAuthAndHealth(t *testing.T) {
	ts, db := newTestServer(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)

	if _, err := c.Health(context.Background(), &agentv1.HealthRequest{}); err != nil {
		t.Fatalf("health: %v", err)
	}
	id, err := c.GetIdentity(context.Background(), &agentv1.GetIdentityRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if id.Tenant != "acme" || id.Role != "tenant" {
		t.Errorf("identity = %+v", id)
	}

	// no token -> unauthenticated
	anon := clientFor(t, ts, "")
	if _, err := anon.ListSessions(context.Background(), &agentv1.ListSessionsRequest{}); err == nil {
		t.Error("anonymous call should be rejected")
	}
	// bad token -> unauthenticated
	bad := clientFor(t, ts, "nope")
	if _, err := bad.ListSessions(context.Background(), &agentv1.ListSessionsRequest{}); err == nil {
		t.Error("invalid token should be rejected")
	}
}

func TestRPCSessionCRUD(t *testing.T) {
	ts, db := newTestServer(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s1", Model: "gw/m"}); err != nil {
		t.Fatal(err)
	}
	// duplicate -> AlreadyExists
	if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s1"}); err == nil {
		t.Error("duplicate create should fail")
	}
	got, err := c.GetSession(ctx, &agentv1.GetSessionRequest{Id: "s1"})
	if err != nil || got.Session.Name != "s1" || got.Session.Model != "gw/m" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	list, err := c.ListSessions(ctx, &agentv1.ListSessionsRequest{})
	if err != nil || len(list.Sessions) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	// rename
	if _, err := c.Rename(ctx, &agentv1.RenameRequest{Id: "s1", Name: "s2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSession(ctx, &agentv1.GetSessionRequest{Id: "s1"}); err == nil {
		t.Error("old name should be gone after rename")
	}
	if _, err := c.GetSession(ctx, &agentv1.GetSessionRequest{Id: "s2"}); err != nil {
		t.Error("renamed session should exist")
	}
	// delete
	if _, err := c.DeleteSession(ctx, &agentv1.DeleteSessionRequest{Id: "s2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSession(ctx, &agentv1.GetSessionRequest{Id: "s2"}); err == nil {
		t.Error("deleted session should be gone")
	}
}

func TestRPCProvidersMaskAndCatalog(t *testing.T) {
	ts, db := newTestServer(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	cat, err := c.ListProvidersCatalog(ctx, &agentv1.ListProvidersCatalogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cat.ApiTypes[agent.APITypeV4]; !ok {
		t.Errorf("catalog missing v4 api type: %+v", cat.ApiTypes)
	}
	// register a provider with a key
	if _, err := c.RegisterProvider(ctx, &agentv1.RegisterProviderRequest{Provider: &agentv1.Provider{
		ProviderId: "gw", ApiType: agent.APITypeV4, Capability: "text",
		BaseUrl: "http://gw/v4/ai", ApiKey: "sk-super-secret-key",
		Models: []*agentv1.ProviderModel{{Id: "m1", Name: "M1", ContextLimit: 1000, ModelType: "text"}},
	}}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListProviders(ctx, &agentv1.ListProvidersRequest{})
	if err != nil || len(list.Providers) != 1 {
		t.Fatalf("list providers = %+v, %v", list, err)
	}
	p := list.Providers[0]
	if p.ApiKey == "sk-super-secret-key" || p.ApiKey == "" {
		t.Errorf("api key must be masked, got %q", p.ApiKey)
	}
	// re-saving the MASK must round-trip the stored key (unchanged)
	if _, err := c.RegisterProvider(ctx, &agentv1.RegisterProviderRequest{Provider: &agentv1.Provider{
		ProviderId: "gw", ApiType: agent.APITypeV4, Capability: "text",
		BaseUrl: "http://gw/v4/ai", ApiKey: p.ApiKey,
		Models: []*agentv1.ProviderModel{{Id: "m1", Name: "M1", ContextLimit: 1000, ModelType: "text"}},
	}}); err != nil {
		t.Fatal(err)
	}
	row, ok, _ := db.GetSession("acme", "x") // touch the db to ensure it is the same handle
	_ = row
	_ = ok
	models, err := c.ListModels(ctx, &agentv1.ListModelsRequest{ProviderId: "gw"})
	if err != nil || len(models.Models) != 1 || models.Models[0].Id != "m1" {
		t.Fatalf("models = %+v, %v", models, err)
	}
	// a provider lookup must never surface the raw key
	rows, _ := db.ListProviders("acme")
	for _, r := range rows {
		if r.APIKey != "sk-super-secret-key" {
			t.Errorf("stored key changed by mask round-trip: %q", r.APIKey)
		}
	}
}

func TestRPCAdminTenants(t *testing.T) {
	ts, _ := newTestServer(t)
	admin := clientForAdmin(t, ts)
	ctx := context.Background()

	created, err := admin.CreateTenant(ctx, &agentv1.CreateTenantRequest{Id: "beta", Name: "Beta"})
	if err != nil {
		t.Fatal(err)
	}
	// The tenant seeder must have run for the new tenant.
	found := false
	for _, id := range seededFor {
		if id == "beta" {
			found = true
		}
	}
	if !found {
		t.Errorf("CreateTenant did not seed the tenant: %v", seededFor)
	}
	if created.Token == "" {
		t.Error("bootstrap token must be returned once")
	}
	// the minted token authenticates as the new tenant
	c := clientFor(t, ts, created.Token)
	id, err := c.GetIdentity(ctx, &agentv1.GetIdentityRequest{})
	if err != nil || id.Tenant != "beta" {
		t.Fatalf("identity via bootstrap token = %+v, %v", id, err)
	}
	// tenants list (admin)
	list, err := admin.ListTenants(ctx, &agentv1.ListTenantsRequest{})
	if err != nil || len(list.Tenants) != 1 {
		t.Fatalf("tenants = %+v, %v", list, err)
	}
	// a tenant token must NOT reach AdminService
	if _, err := adminWithToken(ts, created.Token).ListTenants(ctx, &agentv1.ListTenantsRequest{}); err == nil {
		t.Error("tenant token must not reach AdminService")
	}
}

func clientForAdmin(t *testing.T, ts *httptest.Server) agentv1connect.AdminServiceClient {
	t.Helper()
	return adminWithToken(ts, adminToken)
}

func adminWithToken(ts *httptest.Server, token string) agentv1connect.AdminServiceClient {
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			var info *connect.CallInfo
			ctx, info = connect.NewClientContext(ctx)
			info.RequestHeader().Set("Authorization", "Bearer "+token)
			return next(ctx, spec)
		}
	}
	return agentv1connect.NewAdminServiceClient(
		connect.NewClient(connecthttp.NewTransport(&http.Client{}, ts.URL, connecthttp.WithReadMaxBytes(0)), interceptor),
	)
}

var _ = fmt.Sprintf
var _ = llm.New
