package store

import (
	"testing"

	"gorm.io/gorm"
)

// TestChainCTELongChain builds a long chain and asserts the recursive-CTE read
// returns every message in order (the CTE is now the ONLY implementation).
func TestChainCTELongChain(t *testing.T) {
	d := newTestDB(t)
	const n = 300
	roles := make([]string, n)
	for i := range roles {
		roles[i] = "user"
	}
	tip, logical := buildChain(t, d, "t", roles, seq(n))

	got, err := d.Chain("t", tip, n, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("chain length = %d, want %d", len(got), n)
	}
	for i, m := range got {
		if m.ID != logical[i] {
			t.Fatalf("chain[%d] = %s, want %s", i, m.ID, logical[i])
		}
	}

	anchor := logical[n/2]
	delta, err := d.DeltaSince("t", tip, anchor, n)
	if err != nil {
		t.Fatal(err)
	}
	if !delta.AnchorReached || len(delta.Messages) != n-n/2-1 {
		t.Fatalf("delta: reached=%v len=%d want %d", delta.AnchorReached, len(delta.Messages), n-n/2-1)
	}

	for _, id := range logical {
		if ok, _ := d.IsInChain("t", tip, id); !ok {
			t.Fatalf("IsInChain(%s) = false", id)
		}
	}
	if ok, _ := d.IsInChain("t", tip, "nope"); ok {
		t.Fatal("IsInChain(foreign) = true")
	}
}

// TestChainIsSingleQuery counts DB round-trips: the recursive CTE must read a
// long chain in a CONSTANT number of queries (not one per hop).
func TestChainIsSingleQuery(t *testing.T) {
	d := newTestDB(t)
	const n = 500
	roles := make([]string, n)
	for i := range roles {
		roles[i] = "user"
	}
	tip, _ := buildChain(t, d, "t", roles, seq(n))

	var count int
	inc := func(*gorm.DB) { count++ }
	d.g.Callback().Raw().Before("gorm:raw").Register("cq_raw", inc)
	d.g.Callback().Row().Before("gorm:row").Register("cq_row", inc)
	d.g.Callback().Query().Before("gorm:query").Register("cq_query", inc)

	got, err := d.Chain("t", tip, n, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	if count == 0 {
		t.Fatal("query counter never fired (test is not measuring anything)")
	}
	if count > 3 {
		t.Fatalf("Chain issued %d queries for a %d-long chain (want a constant number, not one per hop)", count, n)
	}
	t.Logf("Chain of %d messages used %d query(s)", n, count)
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// TestIsInChainSemantics pins the exact answers after the LIMIT 1 rewrite:
// true for the tip and every ancestor, false for a stranger and for a
// descendant (a message that came AFTER the queried start).
func TestIsInChainSemantics(t *testing.T) {
	d := newTestDB(t)
	const n = 40
	roles := make([]string, n)
	for i := range roles {
		roles[i] = "user"
	}
	tip, logical := buildChain(t, d, "t", roles, seq(n))

	// Every member is reachable from the tip.
	for _, id := range logical {
		ok, err := d.IsInChain("t", tip, id)
		if err != nil {
			t.Fatalf("IsInChain(%s): %v", id, err)
		}
		if !ok {
			t.Errorf("IsInChain(%s) = false, want true", id)
		}
	}
	// The tip itself (short-circuit at depth 0).
	if ok, _ := d.IsInChain("t", tip, tip); !ok {
		t.Error("IsInChain(tip, tip) = false")
	}
	// A stranger.
	if ok, _ := d.IsInChain("t", tip, "no-such-message"); ok {
		t.Error("IsInChain(stranger) = true")
	}
	// Walking from a MID-chain message must NOT reach a later one.
	mid := logical[n/2]
	later := logical[n/2+1]
	if ok, _ := d.IsInChain("t", mid, later); ok {
		t.Errorf("IsInChain(%s -> %s) = true; a descendant is not an ancestor", mid, later)
	}
	if ok, _ := d.IsInChain("t", mid, logical[0]); !ok {
		t.Errorf("IsInChain(%s -> root) = false, want true", mid)
	}
}
