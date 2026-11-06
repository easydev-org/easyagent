package server

import (
	"context"
	"strings"
	"testing"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * A SESSION ALWAYS GETS A MODEL — from the caller, the tenant's knob, or a registered provider.
 *
 * THE FAILURE THIS PREVENTS IS SILENT AT CREATION AND MISLEADING LATER. A row stored with `model = ''` lists
 * normally, opens normally, and fails on its first turn with "no model configured for session …" — which reads as
 * a runtime problem rather than as something the create call should have refused.
 *
 * THREE SOURCES, IN ORDER, and the first answer wins.
 */
func TestCreateSessionResolvesAModel(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	// A TEXT PROVIDER, because that is what step 3 falls back to.
	if err := db.UpsertProvider(store.Provider{
		Tenant: "acme", ProviderID: "gw", Capability: "text",
		Models: `[{"id":"m1","name":"M1","model_type":"text"}]`,
	}); err != nil {
		t.Fatal(err)
	}

	// 1. THE CALLER'S CHOICE WINS.
	res, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "a", Model: "chosen/model"})
	if err != nil {
		t.Fatal(err)
	}
	if row, _, _ := db.GetSession("acme", res.SessionId); row.Model != "chosen/model" {
		t.Errorf("model = %q, want the one the caller asked for", row.Model)
	}

	// 3. NOTHING ASKED FOR → the provider's first text model, as `provider/model`.
	res, err = c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "b"})
	if err != nil {
		t.Fatal(err)
	}
	row, _, _ := db.GetSession("acme", res.SessionId)
	if row.Model != "gw/m1" {
		t.Errorf("model = %q, want the fallback gw/m1", row.Model)
	}
}

// A NON-TEXT PROVIDER IS NOT A FALLBACK: its models cannot serve a turn.
func TestCreateSessionIgnoresNonTextProviders(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)

	// An image provider sorted FIRST, so a naive "take the first provider" would pick it.
	if err := db.UpsertProvider(store.Provider{
		Tenant: "acme", ProviderID: "aaa-image", Capability: "image",
		Models: `[{"id":"dalle"}]`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertProvider(store.Provider{
		Tenant: "acme", ProviderID: "zzz-text", Capability: "text",
		Models: `[{"id":"m1"}]`,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := c.CreateSession(context.Background(), &agentv1.CreateSessionRequest{Name: "s"})
	if err != nil {
		t.Fatal(err)
	}
	row, _, _ := db.GetSession("acme", res.SessionId)
	if row.Model != "zzz-text/m1" {
		t.Errorf("model = %q, want the text provider's model", row.Model)
	}
}

/**
 * A TENANT WITH NOTHING TO RUN REFUSES AT CREATION, and says what is missing.
 *
 * The alternative — storing the row and failing on the first turn — is the failure shape this whole change exists
 * to remove: silent where it is decided, misleading where it surfaces.
 */
func TestCreateSessionRefusesWhenNoModelExists(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)

	_, err := c.CreateSession(context.Background(), &agentv1.CreateSessionRequest{Name: "s"})
	if err == nil {
		t.Fatal("a tenant with no provider and no default created a session anyway")
	}
	// THE MESSAGE NAMES THE FIX, not the symptom.
	for _, want := range []string{"default_model", "text model"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not mention %q", err.Error(), want)
		}
	}
}

/**
 * THE STORE REFUSES A MODEL-LESS ROW, whatever calls it.
 *
 * The server resolves a model; the store is the choke point every creator passes through — create, fork, a
 * migration, a future RPC. An invariant one caller enforces is a convention, and the row it lets through is one
 * that cannot answer.
 */
func TestStoreRefusesASessionWithoutAModel(t *testing.T) {
	_, d, _ := newTestServerWith(t, nil)
	err := d.CreateSession("t", &store.SessionInput{Name: "s"})
	if err == nil {
		t.Fatal("the store wrote a session with no model")
	}
	if !strings.Contains(err.Error(), "model") {
		t.Errorf("refusal %q does not name the missing field", err.Error())
	}
}
