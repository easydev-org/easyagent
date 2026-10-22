package store

import (
	"fmt"
	"math/rand"
	"testing"
)

// Ported from the TS agent's chain-properties.test.ts: the prev_id chain walk
// backs every message read, so its invariants must hold for arbitrarily shaped
// chains — including ones written out of order (concurrent/idempotent writers).

func buildChain(t *testing.T, d *DB, tenant string, roles []string, insertOrder []int) (tip string, logical []string) {
	t.Helper()
	logical = make([]string, len(roles))
	for i := range roles {
		logical[i] = fmt.Sprintf("m%d", i)
	}
	for _, i := range insertOrder {
		var prev *string
		if i > 0 {
			prev = &logical[i-1]
		}
		if _, err := d.InsertMessage(tenant, logical[i], roles[i], prev, ""); err != nil {
			t.Fatalf("insert m%d: %v", i, err)
		}
	}
	return logical[len(logical)-1], logical
}

func shuffle(n int, seed int64) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

func rolesFor(n int, seed int64) []string {
	kinds := []string{"user", "assistant", "event", "compaction"}
	r := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	for i := range out {
		out[i] = kinds[r.Intn(len(kinds))]
	}
	return out
}

// TestChainProperties checks the chain invariants against many randomly shaped
// chains (seeded, so a failure is reproducible).
func TestChainProperties(t *testing.T) {
	for seed := int64(1); seed <= 60; seed++ {
		d := newTestDB(t)
		n := 1 + int(seed%60)
		roles := rolesFor(n, seed)
		tip, logical := buildChain(t, d, "t", roles, shuffle(n, seed))

		got, err := d.Chain("t", tip, 1000, "")
		if err != nil {
			t.Fatalf("seed %d: chain: %v", seed, err)
		}
		if len(got) != n {
			t.Fatalf("seed %d: chain length = %d, want %d", seed, len(got), n)
		}
		for i, m := range got {
			if m.ID != logical[i] {
				t.Fatalf("seed %d: chain[%d] = %s, want %s", seed, i, m.ID, logical[i])
			}
		}

		for _, id := range logical {
			ok, err := d.IsInChain("t", tip, id)
			if err != nil || !ok {
				t.Fatalf("seed %d: IsInChain(%s) = %v,%v; want true", seed, id, ok, err)
			}
		}
		if ok, _ := d.IsInChain("t", tip, "not-in-chain"); ok {
			t.Fatalf("seed %d: IsInChain(foreign) = true", seed)
		}
	}
}

// TestDeltaSinceAnchor checks the incremental read: from a mid-chain anchor it
// returns exactly the newer segment; from an off-chain anchor it signals resync.
func TestDeltaSinceAnchor(t *testing.T) {
	d := newTestDB(t)
	n := 20
	roles := rolesFor(n, 7)
	tip, logical := buildChain(t, d, "t", roles, shuffle(n, 7))

	anchor := logical[9]
	delta, err := d.DeltaSince("t", tip, anchor, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !delta.AnchorReached {
		t.Fatalf("anchor not reached: %+v", delta)
	}
	if len(delta.Messages) != n-10 {
		t.Fatalf("delta len = %d, want %d", len(delta.Messages), n-10)
	}
	if delta.Messages[0].ID != logical[10] {
		t.Fatalf("delta[0] = %s, want %s", delta.Messages[0].ID, logical[10])
	}

	foreign, _ := d.DeltaSince("t", tip, "ghost", 1000)
	if foreign.AnchorReached || !foreign.ReachedRoot {
		t.Fatalf("foreign anchor: %+v", foreign)
	}
}

// TestInsertMessageIdempotent checks the id is the single logical identity:
// a second insert with the same id is a no-op (ported from
// messages-idempotent.test.ts — a real duplicate-prompt regression).
func TestInsertMessageIdempotent(t *testing.T) {
	d := newTestDB(t)
	first, err := d.InsertMessage("t", "user-1", "user", nil, "")
	if err != nil || !first {
		t.Fatalf("first insert: created=%v err=%v; want true", first, err)
	}
	second, err := d.InsertMessage("t", "user-1", "user", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if second {
		t.Fatal("second insert created a duplicate row")
	}
	chain, _ := d.Chain("t", "user-1", 50, "")
	if len(chain) != 1 {
		t.Fatalf("chain length = %d, want 1", len(chain))
	}
	if ok, _ := d.InsertMessage("t", "user-2", "user", ptr("user-1"), ""); !ok {
		t.Fatal("distinct id was rejected")
	}
}

// TestChainPagination checks limit + before cursor paging (newest→oldest).
func TestChainPagination(t *testing.T) {
	d := newTestDB(t)
	n := 10
	roles := rolesFor(n, 3)
	tip, logical := buildChain(t, d, "t", roles, shuffle(n, 3))

	page1, err := d.Chain("t", tip, 4, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 4 || page1[3].ID != logical[n-1] {
		t.Fatalf("page1 = %d msgs", len(page1))
	}
	page2, err := d.Chain("t", tip, 4, page1[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 4 {
		t.Fatalf("page2 len = %d, want 4", len(page2))
	}
	if page2[len(page2)-1].ID != logical[n-5] {
		t.Fatalf("page2 oldest = %s, want %s", page2[len(page2)-1].ID, logical[n-5])
	}
}

// TestForkBaseAnchorsAtParent checks a fork from a user tip anchors at its
// parent (the child must not inherit the prompt) — ported from fork-base.test.ts.
func TestForkBaseAnchorsAtParent(t *testing.T) {
	d := newTestDB(t)
	_, _ = d.InsertMessage("t", "u1", "user", nil, "")
	_, _ = d.InsertMessage("t", "a1", "assistant", ptr("u1"), "")
	_, _ = d.InsertMessage("t", "u2", "user", ptr("a1"), "")

	base, err := d.ForkBase("t", "u2")
	if err != nil || base == nil || *base != "a1" {
		t.Fatalf("ForkBase(u2) = %v,%v; want a1", base, err)
	}
	base, _ = d.ForkBase("t", "a1")
	if base == nil || *base != "a1" {
		t.Fatalf("ForkBase(a1) = %v; want a1", base)
	}
}

// TestChainDepthLimit checks the handler semantics ported from
// message-chain-limit.test.ts: `limit` is a DEPTH cap (newest N), and
// `limit <= 0` means "no depth cap" (the FULL chain).
func TestChainDepthLimit(t *testing.T) {
	d := newTestDB(t)
	n := 12
	roles := rolesFor(n, 11)
	tip, logical := buildChain(t, d, "t", roles, shuffle(n, 11))

	// A positive limit caps the depth to the NEWEST `limit` messages.
	capped, err := d.Chain("t", tip, 5, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(capped) != 5 {
		t.Fatalf("capped len = %d, want 5", len(capped))
	}
	if capped[len(capped)-1].ID != logical[n-1] {
		t.Fatalf("capped newest = %s, want %s", capped[len(capped)-1].ID, logical[n-1])
	}

	// limit <= 0 means no cap -> the full chain.
	full, err := d.Chain("t", tip, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != n {
		t.Fatalf("full len = %d, want %d", len(full), n)
	}
}

// TestUnreadDerivation checks unread = max(0, message_seq - read_seq), the
// shared read watermark (ported from message-seq.test.ts).
func TestUnreadDerivation(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t", SessionInput{Name: "s", Model: "p/m"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		// message_seq counts the CHAIN, so the rows have to exist: the counter is
		// what the unread badge is derived from, and a number the reader cannot
		// reproduce is exactly the bug this replaced.
		id := fmt.Sprintf("m%d", i)
		var prev *string
		if i > 0 {
			p := fmt.Sprintf("m%d", i-1)
			prev = &p
		}
		if _, err := d.InsertMessage("t", id, "user", prev, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := d.AppendMessageTip("t", "s", id); err != nil {
			t.Fatal(err)
		}
	}
	s, _, _ := d.GetSession("t", "s")
	if s.MessageSeq != 3 || s.ReadSeq != 0 {
		t.Fatalf("seq=%d read=%d, want 3/0", s.MessageSeq, s.ReadSeq)
	}
	if unread := s.MessageSeq - s.ReadSeq; unread != 3 {
		t.Fatalf("unread = %d, want 3", unread)
	}
	// MarkRead advances the shared watermark; unread drops accordingly.
	if _, err := d.MarkRead("t", "s", 2); err != nil {
		t.Fatal(err)
	}
	s, _, _ = d.GetSession("t", "s")
	if unread := s.MessageSeq - s.ReadSeq; unread != 1 {
		t.Fatalf("unread after markread = %d, want 1", unread)
	}
}
