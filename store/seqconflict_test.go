package store

import (
	"testing"
)

/**
 * THE SAME ID TWICE IS STILL A NO-OP — that is what `DoNothing` is for, and the fix above must not
 * have broken it. A redelivered mailbox message would otherwise start failing.
 */
func TestReinsertingTheSameIDIsStillIdempotent(t *testing.T) {
	d := newTestDB(t)
	if _, err := d.InsertMessage("t", "m1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	created, err := d.InsertMessage("t", "m1", "user", nil, "")
	if err != nil {
		t.Fatalf("a retry must not fail: %v", err)
	}
	if created {
		t.Fatal("a retry reported that it created the row")
	}

	// And on a non-root message, where the predecessor lookup also runs.
	if _, err := d.InsertMessage("t", "m2", "assistant", ptr("m1"), ""); err != nil {
		t.Fatal(err)
	}
	created, err = d.InsertMessage("t", "m2", "assistant", ptr("m1"), "")
	if err != nil || created {
		t.Fatalf("retry of a chained message: created=%v err=%v", created, err)
	}
}

/**
 * A FORK'S TWO CHAINS LEGALLY OCCUPY THE SAME POSITION.
 *
 * Parent and child share every message up to the fork point, then DIVERGE: each appends to the same
 * predecessor and both want `prev.seq + 1`. That is not a conflict, it is what forking IS — and
 * `subsession-create` does it on every call.
 *
 * An earlier version of this change put a unique index on `(tenant, prev_id, seq)` and reported the
 * second writer as an error. The test for it passed, because it asserted the very behaviour that
 * would have broken every fork. The constraint is gone; this test pins down why.
 */
func TestForkDivergenceIsLegitimate(t *testing.T) {
	d := newTestDB(t)
	parent := &SessionInput{Name: "parent", Model: "m"}
	if err := d.CreateSession("t", parent); err != nil {
		t.Fatal(err)
	}
	if _, err := d.InsertMessage("t", "m1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := d.InsertMessage("t", "m2", "assistant", ptr("m1"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AppendMessageTip("t", parent.ID, "m2"); err != nil {
		t.Fatal(err)
	}
	// The child forks AT m2 and shares the chain up to there.
	child := &SessionInput{Name: "child", Model: "m", TipID: ptr("m2")}
	if err := d.CreateSession("t", child); err != nil {
		t.Fatal(err)
	}

	// Both append to the same predecessor. This must SUCCEED on both sides.
	if _, err := d.InsertMessage("t", "p3", "user", ptr("m2"), ""); err != nil {
		t.Fatalf("the parent could not append: %v", err)
	}
	if _, err := d.InsertMessage("t", "c3", "user", ptr("m2"), ""); err != nil {
		t.Fatalf("the child could not append; subsession-create would fail: %v", err)
	}
	// Each session advances its OWN tip — the parent to p3, the child to c3.
	if _, err := d.AppendMessageTip("t", parent.ID, "p3"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AppendMessageTip("t", child.ID, "c3"); err != nil {
		t.Fatal(err)
	}

	// Each session reads its OWN chain-end. Both share m1 and m2 — that is what the fork inherited
	// — and then diverge: the parent's third message is p3, the child's third is c3, and neither
	// sees the other's.
	ptip, _, _ := d.GetSession("t", parent.ID)
	ctip, _, _ := d.GetSession("t", child.ID)
	pChain, _ := d.Chain("t", *ptip.TipID, 100, "")
	cChain, _ := d.Chain("t", *ctip.TipID, 100, "")
	if got := idsOf(pChain); len(got) != 3 || got[2] != "p3" {
		t.Fatalf("parent chain = %v, want [m1 m2 p3]", got)
	}
	if got := idsOf(cChain); len(got) != 3 || got[2] != "c3" {
		t.Fatalf("child chain = %v, want [m1 m2 c3]", got)
	}
	// Their thirds are BOTH position 3 — the pair (session, seq) is what distinguishes them.
	if pChain[2].Seq != 3 || cChain[2].Seq != 3 {
		t.Fatalf("divergent messages got seqs %d and %d; both should be 3",
			pChain[2].Seq, cChain[2].Seq)
	}
}

func idsOf(ms []Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}
