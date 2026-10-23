package store

import (
	"errors"
	"testing"
)

func newGrantDB(t *testing.T) *DB {
	t.Helper()
	d := newTestDB(t)
	if err := d.CreateSession("t1", SessionInput{Name: "s1", Model: "p/m"}); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateSession("t1", SessionInput{Name: "s2", Model: "p/m"}); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestAddAndListGrants(t *testing.T) {
	d := newGrantDB(t)
	if _, err := d.AddGrant("t1", "s1", SessionGrant{
		ExtID: "gitea", Capability: "mr.merge", Params: `{"repo":"agentdev/mrflow"}`, Risk: "destructive",
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListGrants("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d grants, want 1", len(rows))
	}
	// Params round-trip EXACTLY: the agent must never rewrite what the extension declared.
	if rows[0].Params != `{"repo":"agentdev/mrflow"}` {
		t.Errorf("params = %q", rows[0].Params)
	}
	if rows[0].ID == "" {
		t.Error("a grant needs an id, or it cannot be revoked individually")
	}
	if rows[0].GrantedBy != "user" {
		t.Errorf("granted_by = %q, want user", rows[0].GrantedBy)
	}
}

// Several grants of the SAME capability with different params — the whole reason params
// exist. Each is separately revocable.
func TestSameCapabilityDifferentParams(t *testing.T) {
	d := newGrantDB(t)
	a, _ := d.AddGrant("t1", "s1", SessionGrant{ExtID: "gitea", Capability: "mr.merge", Params: `{"repo":"agentdev/mrflow"}`})
	b, _ := d.AddGrant("t1", "s1", SessionGrant{ExtID: "gitea", Capability: "mr.merge", Params: `{"repo":"scratch/*"}`})

	rows, _ := d.ListGrants("t1", "s1")
	if len(rows) != 2 {
		t.Fatalf("got %d grants, want 2 independent instances", len(rows))
	}
	if a.ID == b.ID {
		t.Fatal("two grants must not share an id")
	}

	// Revoking one leaves the other: this is what "revoke that repository" means.
	if err := d.RevokeGrant("t1", "s1", a.ID); err != nil {
		t.Fatal(err)
	}
	rows, _ = d.ListGrants("t1", "s1")
	if len(rows) != 1 || rows[0].ID != b.ID {
		t.Fatalf("after revoking one, got %d grant(s)", len(rows))
	}
}

func TestRevokeIsSoftDelete(t *testing.T) {
	d := newGrantDB(t)
	g, _ := d.AddGrant("t1", "s1", SessionGrant{ExtID: "gitea", Capability: "mr.merge"})
	if err := d.RevokeGrant("t1", "s1", g.ID); err != nil {
		t.Fatal(err)
	}
	// Gone from the live list...
	rows, _ := d.ListGrants("t1", "s1")
	if len(rows) != 0 {
		t.Fatalf("revoked grant still listed")
	}
	// ...but the row survives for audit, with a timestamp.
	var all []SessionGrant
	if err := d.g.Where("tenant = ? AND session = ?", "t1", "s1").Find(&all).Error; err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].RevokedAt == nil {
		t.Fatalf("revoking must keep the row with revoked_at set, got %+v", all)
	}
}

func TestRevokeUnknownReportsNotFound(t *testing.T) {
	d := newGrantDB(t)
	// Distinguishing "revoked it" from "there was nothing" matters: a UI that silently
	// succeeds on a stale id tells the user a permission is gone when it never existed.
	if err := d.RevokeGrant("t1", "s1", "gr-nope"); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("err = %v, want ErrGrantNotFound", err)
	}
}

func TestCopyGrantsFromIsIndependent(t *testing.T) {
	d := newGrantDB(t)
	if _, err := d.AddGrant("t1", "s1", SessionGrant{
		ExtID: "gitea", Capability: "mr.merge", Params: `{"repo":"agentdev/*"}`,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := d.CopyGrantsFrom("t1", "s1", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("copied %d, want 1", n)
	}

	rows, _ := d.ListGrants("t1", "s2")
	if len(rows) != 1 {
		t.Fatalf("child has %d grants", len(rows))
	}
	// Provenance must say it was inherited, not granted by hand.
	if rows[0].GrantedBy != "auto:fork:s1" {
		t.Errorf("granted_by = %q, want auto:fork:s1", rows[0].GrantedBy)
	}

	// Revoking in the child must NOT touch the parent.
	if err := d.RevokeGrant("t1", "s2", rows[0].ID); err != nil {
		t.Fatal(err)
	}
	parent, _ := d.ListGrants("t1", "s1")
	if len(parent) != 1 {
		t.Fatal("revoking in the fork must not revoke in the parent")
	}
}

func TestCopyGrantsFromEmpty(t *testing.T) {
	d := newGrantDB(t)
	n, err := d.CopyGrantsFrom("t1", "s1", "s2")
	if err != nil || n != 0 {
		t.Fatalf("copying from an empty session = %d, %v", n, err)
	}
}

func TestGrantsByExtGroups(t *testing.T) {
	d := newGrantDB(t)
	d.AddGrant("t1", "s1", SessionGrant{ExtID: "gitea", Capability: "mr.merge"})
	d.AddGrant("t1", "s1", SessionGrant{ExtID: "gitea", Capability: "repo.delete"})
	d.AddGrant("t1", "s1", SessionGrant{ExtID: "sandbox", Capability: "exec"})

	byExt, err := d.GrantsByExt("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(byExt["gitea"]) != 2 || len(byExt["sandbox"]) != 1 {
		t.Fatalf("grouping = %+v", byExt)
	}
}

// The agent's entire check: does the session hold ANY grant for each required key.
func TestHasAnyCapability(t *testing.T) {
	grants := []GrantSnapshot{
		{Capability: "mr.merge", Params: `{"repo":"agentdev/*"}`},
	}
	if missing, ok := HasAnyCapability(grants, []string{"mr.merge"}); !ok {
		t.Errorf("should hold mr.merge, missing=%q", missing)
	}
	if missing, ok := HasAnyCapability(grants, []string{"repo.delete"}); ok || missing != "repo.delete" {
		t.Errorf("should lack repo.delete, got ok=%v missing=%q", ok, missing)
	}
	// No requirements means nothing to check.
	if _, ok := HasAnyCapability(nil, nil); !ok {
		t.Error("a tool with no requirements must pass")
	}
	// ALL keys must be held, not just one.
	if _, ok := HasAnyCapability(grants, []string{"mr.merge", "repo.delete"}); ok {
		t.Error("holding one of two required keys must not pass")
	}
}

// The agent must NOT look at params. A grant with narrow params still satisfies the
// presence check — coverage is the extension's call.
func TestHasAnyCapabilityIgnoresParams(t *testing.T) {
	grants := []GrantSnapshot{{Capability: "mr.merge", Params: `{"repo":"only/this-one"}`}}
	if _, ok := HasAnyCapability(grants, []string{"mr.merge"}); !ok {
		t.Error("the presence check must ignore params entirely")
	}
}

func TestSortedCapabilities(t *testing.T) {
	got := SortedCapabilities([]GrantSnapshot{
		{Capability: "mr.merge"}, {Capability: "exec"}, {Capability: "mr.merge"},
	})
	if len(got) != 2 || got[0] != "exec" || got[1] != "mr.merge" {
		t.Fatalf("got %v, want [exec mr.merge]", got)
	}
}
