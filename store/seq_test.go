package store

import "testing"

/**
 * SEQ IS THE POSITION IN THE CHAIN, AND IT NEVER MOVES.
 *
 * `depth` was recomputed on every read as `len(chain) - 1 - i`, so appending one message shifted
 * every depth in the conversation. That is fine for "the last 5" and wrong for "message 12": the
 * number a caller saved last turn addressed different content the next.
 */
func TestSeqIsStableAcrossAppends(t *testing.T) {
	d := newTestDB(t)
	if _, err := d.InsertMessage("t", "m1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	m1, _, _ := d.GetMessage("t", "m1")
	if m1.Seq != 1 {
		t.Fatalf("root seq = %d, want 1", m1.Seq)
	}
	if _, err := d.InsertMessage("t", "m2", "assistant", ptr("m1"), ""); err != nil {
		t.Fatal(err)
	}
	// The FIRST message's number must not change when the second arrives.
	again, _, _ := d.GetMessage("t", "m1")
	if again.Seq != 1 {
		t.Fatalf("appending moved the first message from 1 to %d", again.Seq)
	}
	m2, _, _ := d.GetMessage("t", "m2")
	if m2.Seq != 2 {
		t.Fatalf("second seq = %d, want 2", m2.Seq)
	}
}

/**
 * A FORK SHARES THE CHAIN, SO THE CHILD CONTINUES THE PARENT'S NUMBERING.
 *
 * `ForkSession` copies the TIP, not the messages: parent and child reach the same rows. So the
 * child's first new message is seq(tip)+1 — not 1, which would collide with messages it can already
 * see through the shared chain.
 */
func TestForkedChildContinuesTheSeq(t *testing.T) {
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
	// Fork: the child starts AT the parent's tip.
	child := &SessionInput{Name: "child", Model: "m", TipID: ptr("m2")}
	if err := d.CreateSession("t", child); err != nil {
		t.Fatal(err)
	}

	// The child's first message continues from m2.
	if _, err := d.InsertMessage("t", "c1", "user", ptr("m2"), ""); err != nil {
		t.Fatal(err)
	}
	c1, _, _ := d.GetMessage("t", "c1")
	if c1.Seq != 3 {
		t.Fatalf("child's first message seq = %d, want 3 (parent tip was 2)", c1.Seq)
	}
	// And the messages it inherited keep their numbers.
	m1, _, _ := d.GetMessage("t", "m1")
	if m1.Seq != 1 {
		t.Fatalf("fork renumbered an inherited message: %d", m1.Seq)
	}
}

/**
 * AN UNDO MOVES THE TIP BACK AND LEAVES THE ROWS, SO THE ABANDONED MESSAGES KEEP THEIR NUMBERS AND
 * SIMPLY STOP BEING REACHABLE.
 *
 * That is what an undo means: the conversation no longer includes them. Renumbering the survivors
 * would be worse — a position a caller held would address different content.
 */
func TestUndoDoesNotRenumber(t *testing.T) {
	d := newTestDB(t)
	sess := &SessionInput{Name: "s", Model: "m"}
	if err := d.CreateSession("t", sess); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"m1", "m2", "m3"} {
		var prev *string
		if i > 0 {
			prev = ptr([]string{"m1", "m2", "m3"}[i-1])
		}
		if _, err := d.InsertMessage("t", id, "user", prev, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.AppendMessageTip("t", sess.ID, "m3"); err != nil {
		t.Fatal(err)
	}

	// Undo to m2.
	if _, err := d.SetTip("t", sess.ID, ptr("m2")); err != nil {
		t.Fatal(err)
	}
	chain, err := d.Chain("t", "m2", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 2 {
		t.Fatalf("chain after undo has %d messages, want 2", len(chain))
	}
	// The survivors keep their numbers.
	for i, m := range chain {
		if m.Seq != int64(i+1) {
			t.Errorf("message %s seq = %d, want %d", m.ID, m.Seq, i+1)
		}
	}
	// The abandoned row still exists with its number — it is simply unreachable.
	m3, ok, _ := d.GetMessage("t", "m3")
	if !ok {
		t.Fatal("undo deleted the row; it is supposed to keep it")
	}
	if m3.Seq != 3 {
		t.Fatalf("abandoned message lost its number: %d", m3.Seq)
	}
}

/**
 * A MISSING PREDECESSOR IS REFUSED.
 *
 * Appending to a message that does not exist means the caller holds a stale tip. Starting a second
 * chain at position 1 would silently corrupt every later number, so the insert fails instead.
 */
func TestInsertRefusesAnUnknownPredecessor(t *testing.T) {
	d := newTestDB(t)
	if _, err := d.InsertMessage("t", "m1", "user", ptr("does-not-exist"), ""); err == nil {
		t.Fatal("an unknown predecessor was accepted; the chain would fork at position 1")
	}
}
