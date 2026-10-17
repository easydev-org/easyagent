package server

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// stubFields is a ConfigFieldSource over a static list (the bundled source is
// the only production one; this lets a test declare a secret knob).
type stubFields struct{ fields []bundled.ConfigField }

func (s stubFields) ConfigFieldsFor(tool string) []bundled.ConfigField {
	var out []bundled.ConfigField
	for _, f := range s.fields {
		out = append(out, f)
	}
	return out
}
func (s stubFields) ConfigFields() []bundled.ConfigField { return s.fields }

// stubTools is a ToolSource with no tools (ListExtensions still emits the
// bundled entry's config surface).
type stubTools struct{}

func (stubTools) Tools() []agent.ToolDef { return nil }

// TestExtensionConfigSecretWithheld is the core guarantee: a secret knob's
// VALUE never appears in any client response — only that it is set.
func TestExtensionConfigSecretWithheld(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)

	// The server's bundled field source declares one ordinary + one secret knob.
	srv.SetBundledConfigFields(stubFields{fields: []bundled.ConfigField{
		{Name: "poll-interval", Type: "number", Kind: bundled.ConfigKindValue},
		{Name: "gh-token", Type: "string", Kind: bundled.ConfigKindSecret},
	}})

	ctx := context.Background()
	// Set both values (as the UI would).
	if _, err := c.SetExtensionConfig(ctx, &agentv1.SetExtensionConfigRequest{
		ExtId: "bundled", Name: "poll-interval",
		Value: structpb.NewStringValue("42"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetExtensionConfig(ctx, &agentv1.SetExtensionConfigRequest{
		ExtId: "bundled", Name: "gh-token",
		Value: structpb.NewStringValue("ghp_SUPERSECRET"),
	}); err != nil {
		t.Fatal(err)
	}

	// GetExtensionConfig: the ordinary value is present; the secret's is NOT,
	// only its name in secret_set.
	res, err := c.GetExtensionConfig(ctx, &agentv1.GetExtensionConfigRequest{ExtId: "bundled"})
	if err != nil {
		t.Fatal(err)
	}
	if v := res.Values["poll-interval"]; v == nil || v.GetStringValue() != "42" {
		t.Errorf("ordinary knob = %v, want 42", v)
	}
	if v, ok := res.Values["gh-token"]; ok {
		t.Fatalf("secret value LEAKED: %v", v)
	}
	found := false
	for _, n := range res.SecretSet {
		if n == "gh-token" {
			found = true
		}
	}
	if !found {
		t.Errorf("secret_set = %v, want to contain gh-token", res.SecretSet)
	}
}

// TestListExtensionsHidesSecretDefault: the declared default of a secret knob
// must never be surfaced (it would be broadcast in the manifest).
func TestListExtensionsHidesSecretDefault(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	srv.SetBundledTools(stubTools{})
	srv.SetBundledConfigFields(stubFields{fields: []bundled.ConfigField{
		{Name: "api-key", Type: "string", Kind: bundled.ConfigKindSecret, Default: "should-not-appear"},
		{Name: "region", Type: "string", Kind: bundled.ConfigKindValue, Default: "us"},
	}})

	res, err := c.ListExtensions(context.Background(), &agentv1.ListExtensionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ext := range res.Extensions {
		if ext.ExtId != "bundled" {
			continue
		}
		for _, f := range ext.ConfigFields {
			if f.Name == "api-key" && f.Default != nil {
				t.Fatalf("secret default leaked: %v", f.Default)
			}
			if f.Name == "region" && (f.Default == nil || f.Default.GetStringValue() != "us") {
				t.Errorf("ordinary default = %v, want us", f.Default)
			}
		}
	}
}
