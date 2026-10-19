package store

import (
	"testing"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	return d
}

func ptr(s string) *string { return &s }

func TestSessionCRUDAndCounters(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t1", SessionInput{Name: "s1", Model: "p/m", Preset: "default"}); err != nil {
		t.Fatal(err)
	}
	s, ok, err := d.GetSession("t1", "s1")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if s.Model != "p/m" {
		t.Errorf("model = %q", s.Model)
	}
	// AppendMessageTip bumps message_seq and returns the NEW value.
	seq, err := d.AppendMessageTip("t1", "s1", "m1")
	if err != nil || seq != 1 {
		t.Fatalf("append seq = %d, %v", seq, err)
	}
	seq, _ = d.AppendMessageTip("t1", "s1", "m2")
	if seq != 2 {
		t.Errorf("append seq = %d, want 2", seq)
	}
	// MarkRead is monotonic and clamped to message_seq.
	if v, _ := d.MarkRead("t1", "s1", 1); v != 1 {
		t.Errorf("markRead 1 = %d", v)
	}
	if v, _ := d.MarkRead("t1", "s1", 0); v != 1 {
		t.Errorf("markRead 0 must not lower: %d", v)
	}
	if v, _ := d.MarkRead("t1", "s1", 999); v != 2 {
		t.Errorf("markRead clamp = %d, want 2", v)
	}
	// tip reflects the last append.
	if tip, _ := d.SessionTip("t1", "s1"); tip != "m2" {
		t.Errorf("tip = %q", tip)
	}
	// SetTip does NOT bump message_seq.
	if _, err := d.SetTip("t1", "s1", ptr("m1")); err != nil {
		t.Fatal(err)
	}
	s, _, _ = d.GetSession("t1", "s1")
	if s.MessageSeq != 2 {
		t.Errorf("message_seq after SetTip = %d, want 2", s.MessageSeq)
	}
}

func TestMessageChain(t *testing.T) {
	d := newTestDB(t)
	// chain: m1 <- m2 <- m3
	for i, id := range []string{"m1", "m2", "m3"} {
		var prev *string
		if i > 0 {
			prev = ptr([]string{"m1", "m2"}[i-1])
		}
		if _, err := d.InsertMessage("t1", id, "assistant", prev, ""); err != nil {
			t.Fatal(err)
		}
	}
	// idempotent insert
	created, err := d.InsertMessage("t1", "m3", "assistant", ptr("m2"), "")
	if err != nil || created {
		t.Errorf("dup insert created=%v err=%v", created, err)
	}
	chain, err := d.Chain("t1", "m3", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 || chain[0].ID != "m1" || chain[2].ID != "m3" {
		t.Errorf("chain = %+v", chain)
	}
	// limit keeps the NEWEST `limit` rows, oldest-first
	chain, _ = d.Chain("t1", "m3", 2, "")
	if len(chain) != 2 || chain[0].ID != "m2" {
		t.Errorf("limited chain = %+v", chain)
	}
	// incremental delta from m1
	delta, _ := d.DeltaSince("t1", "m3", "m1", 10)
	if !delta.AnchorReached || len(delta.Messages) != 2 {
		t.Errorf("delta = %+v", delta)
	}
	// anchor not on chain -> resync
	delta, _ = d.DeltaSince("t1", "m3", "ghost", 10)
	if delta.AnchorReached || !delta.ReachedRoot {
		t.Errorf("ghost anchor delta = %+v", delta)
	}
	// chain membership
	if ok, _ := d.IsInChain("t1", "m3", "m1"); !ok {
		t.Error("m1 should be in chain")
	}
	if ok, _ := d.IsInChain("t1", "m3", "nope"); ok {
		t.Error("nope should not be in chain")
	}
}

func TestForkBaseSkipsUserPrompt(t *testing.T) {
	d := newTestDB(t)
	_, _ = d.InsertMessage("t1", "p", "user", nil, "user")
	_, _ = d.InsertMessage("t1", "a1", "assistant", ptr("p"), "")
	// tip is the user prompt -> fork base is its PARENT (nil here)
	base, err := d.ForkBase("t1", "p")
	if err != nil {
		t.Fatal(err)
	}
	if base != nil {
		t.Errorf("fork base for user tip = %v, want nil", *base)
	}
	// tip is an assistant step -> anchor as-is
	base, _ = d.ForkBase("t1", "a1")
	if base == nil || *base != "a1" {
		t.Errorf("fork base for assistant tip = %v", base)
	}
}

func TestMailboxTwoPhaseDrain(t *testing.T) {
	d := newTestDB(t)
	// idempotent enqueue by producer id
	if err := d.EnqueueWithID("t1", "mb1", "s1", "trigger", map[string]any{"text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	if err := d.EnqueueWithID("t1", "mb1", "s1", "trigger", map[string]any{"text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	_, _ = d.Enqueue("t1", "s1", "event", map[string]any{"k": 1}, "")

	pending, _ := d.HasPending("t1", "s1")
	if !pending {
		t.Fatal("expected pending")
	}
	rows, err := d.DrainAll("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("drained %d, want 2", len(rows))
	}
	// the drain is atomic: a second drain finds nothing.
	rows2, _ := d.DrainAll("t1", "s1")
	if len(rows2) != 0 {
		t.Errorf("second drain = %d, want 0", len(rows2))
	}
	// consumed rows are retained for audit
	var n int64
	d.G().Model(&Mailbox{}).Where("status = 'consumed'").Count(&n)
	if n != 2 {
		t.Errorf("consumed rows = %d, want 2", n)
	}
}

func TestProviderShadowing(t *testing.T) {
	d := newTestDB(t)
	if err := d.UpsertProvider(Provider{Tenant: "global", ProviderID: "gw", BaseURL: "https://global"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpsertProvider(Provider{Tenant: "t1", ProviderID: "gw", BaseURL: "https://t1"}); err != nil {
		t.Fatal(err)
	}
	rows, err := d.ListProviders("t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BaseURL != "https://t1" {
		t.Errorf("tenant row should shadow global: %+v", rows)
	}
	// a tenant without an override sees the global row
	rows, _ = d.ListProviders("t2")
	if len(rows) != 1 || rows[0].BaseURL != "https://global" {
		t.Errorf("global row expected: %+v", rows)
	}
}

func TestFileDedupPerTenant(t *testing.T) {
	d := newTestDB(t)
	a, err := d.UpsertFile(AgentFile{Code: "c1", Tenant: "t1", SHA256: "sha", Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.UpsertFile(AgentFile{Code: "c2", Tenant: "t1", SHA256: "sha", Name: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Code != b.Code {
		t.Errorf("dedup failed: %q vs %q", a.Code, b.Code)
	}
	// same bytes under another tenant resolve to an independent code
	c, _ := d.UpsertFile(AgentFile{Code: "c3", Tenant: "t2", SHA256: "sha", Name: "c"})
	if c.Code != "c3" {
		t.Errorf("cross-tenant dedup leaked: %q", c.Code)
	}
}

func TestTenantsAndTokens(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateTenant("t1", "Tenant One"); err != nil {
		t.Fatal(err)
	}
	sha := "deadbeef"
	tokID, err := d.CreateToken("t1", sha, "default")
	if err != nil {
		t.Fatal(err)
	}
	ten, tok, ok, err := d.TenantByTokenSHA(sha)
	if err != nil || !ok || ten.ID != "t1" || tok.TokenID != tokID {
		t.Fatalf("resolve token: ok=%v ten=%v tok=%v err=%v", ok, ten, tok, err)
	}
	if err := d.RevokeToken(tokID); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := d.TenantByTokenSHA(sha); ok {
		t.Error("revoked token must not resolve")
	}
}

func TestKnownTenants(t *testing.T) {
	d := newTestDB(t)
	_ = d.CreateSession("a", SessionInput{Name: "s"})
	_ = d.CreateTenant("b", "")
	_ = d.UpsertProvider(Provider{Tenant: "c", ProviderID: "p"})
	got, err := d.KnownTenants()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"a": true, "b": true, "c": true}
	if len(got) != len(want) {
		t.Fatalf("known tenants = %v", got)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("unexpected tenant %q", g)
		}
	}
}
