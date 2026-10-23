package bus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/store"
)

// These tests pin the SHALLOWNESS of the agent's permission check. It must answer "could
// this possibly be allowed" and never "is this call covered" — the second needs the call's
// target, which only the extension can resolve. Getting this wrong in the other direction
// (checking params here) would mean the agent knowing what a "repository" is.

func executorWith(defs map[string]agent.ToolDef, grants map[string][]store.GrantSnapshot) *ToolExecutor {
	e := NewToolExecutor(nil)
	e.defs = defs
	e.grants = func(ctx context.Context, tenant, session string) (map[string][]store.GrantSnapshot, error) {
		return grants, nil
	}
	return e
}

func TestRequiresOfFindsDeclaredKeys(t *testing.T) {
	e := executorWith(map[string]agent.ToolDef{
		"git-mr-merge": {Name: "git-mr-merge", ExtID: "gitea", Requires: []string{"mr.merge"}},
	}, nil)

	got := e.requiresOf("git-mr-merge")
	if len(got) != 1 || got[0] != "mr.merge" {
		t.Fatalf("requiresOf = %v", got)
	}
	// A tool with no requirements returns nil, so the caller skips the check entirely.
	if got := e.requiresOf("git-read"); got != nil {
		t.Errorf("unexpected requirements for an unguarded tool: %v", got)
	}
}

// The model may call the qualified form; a different spelling must not sidestep the check.
func TestRequiresOfAcceptsQualifiedName(t *testing.T) {
	e := executorWith(map[string]agent.ToolDef{
		"git-mr-merge": {Name: "git-mr-merge", ExtID: "gitea", Requires: []string{"mr.merge"}},
	}, nil)
	if got := e.requiresOf("gitea-git-mr-merge"); len(got) != 1 || got[0] != "mr.merge" {
		t.Fatalf("qualified name bypassed the requirement lookup: %v", got)
	}
}

func TestSessionGrantsFiltersByExtension(t *testing.T) {
	e := executorWith(nil, map[string][]store.GrantSnapshot{
		"gitea":   {{ExtID: "gitea", Capability: "mr.merge", Params: `{"repo":"a/*"}`}},
		"sandbox": {{ExtID: "sandbox", Capability: "exec"}},
	})
	got := e.sessionGrants(context.Background(), "t", "s", "gitea")
	if len(got) != 1 || got[0].Capability != "mr.merge" {
		t.Fatalf("got %+v", got)
	}
	// An extension the session holds nothing for yields nothing, not an error.
	if got := e.sessionGrants(context.Background(), "t", "s", "unknown"); len(got) != 0 {
		t.Errorf("expected no grants for an unrelated extension")
	}
}

// No source, or a source that errors, must yield NO grants. Fail-closed: a deployment that
// cannot look up authorisation must not hand out capability.
func TestSessionGrantsFailsClosed(t *testing.T) {
	e := NewToolExecutor(nil)
	if got := e.sessionGrants(context.Background(), "t", "s", "gitea"); len(got) != 0 {
		t.Error("a nil source must yield no grants")
	}

	e.grants = func(ctx context.Context, tenant, session string) (map[string][]store.GrantSnapshot, error) {
		return nil, context.DeadlineExceeded
	}
	if got := e.sessionGrants(context.Background(), "t", "s", "gitea"); len(got) != 0 {
		t.Error("a failing source must yield no grants")
	}
}

// Params must reach the extension byte-for-byte: rewriting them would change what the user
// authorised.
func TestGrantsToWirePreservesParams(t *testing.T) {
	wire := grantsToWire([]store.GrantSnapshot{
		{Capability: "mr.merge", Params: `{"repo":"agentdev/mrflow","branch":"main"}`},
	})
	if len(wire) != 1 || wire[0].GetKey() != "mr.merge" {
		t.Fatalf("wire = %+v", wire)
	}
	got := wire[0].GetParams().AsMap()
	if got["repo"] != "agentdev/mrflow" || got["branch"] != "main" {
		t.Errorf("params not preserved: %+v", got)
	}
}

func TestGrantsToWireBooleanCapability(t *testing.T) {
	wire := grantsToWire([]store.GrantSnapshot{{Capability: "exec", Params: ""}})
	if len(wire) != 1 {
		t.Fatalf("wire = %+v", wire)
	}
	// A boolean capability has no params; an empty struct is the right shape.
	if m := wire[0].GetParams().AsMap(); len(m) != 0 {
		t.Errorf("expected no params, got %+v", m)
	}
}

// A malformed params blob is passed as empty rather than dropped: the grant stays visible,
// and the extension's matching simply will not cover anything with it.
func TestGrantsToWireUnparseableParams(t *testing.T) {
	wire := grantsToWire([]store.GrantSnapshot{{Capability: "mr.merge", Params: "{not json"}})
	if len(wire) != 1 {
		t.Fatalf("an unparseable grant must still be forwarded, got %+v", wire)
	}
	if m := wire[0].GetParams().AsMap(); len(m) != 0 {
		t.Errorf("expected empty params, got %+v", m)
	}
}

// The refusal message has to be actionable: it names the capability and says how to fix it.
func TestPermissionDeniedErrorMessage(t *testing.T) {
	err := &PermissionDeniedError{Tool: "git-mr-merge", ExtID: "gitea", Capability: "mr.merge"}
	msg := err.Error()
	for _, want := range []string{"git-mr-merge", "mr.merge", "gitea", "grant"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

var _ = json.Marshal
