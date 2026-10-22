package store

import "testing"

// TestAppendMessageTipIsIdempotentPerMessage is the regression test for a
// message_seq that drifts away from the real message count.
//
// persistStep runs several times per step (each wave of parts is written as it
// arrives), and it advances the tip each time. Bumping the counter unconditionally
// inflated message_seq far past the number of messages — a 15-message session
// reached 59 — and message_seq is what the unread watermark is compared against,
// so the badge and mark-read both go wrong.
func TestAppendMessageTipIsIdempotentPerMessage(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t1", SessionInput{Name: "s1", Model: "m"}); err != nil {
		t.Fatal(err)
	}

	// The same message becoming the tip three times (three waves of parts).
	// The row must exist: message_seq counts the chain, so re-setting the same tip
	// reports the same length rather than advancing a counter.
	if _, err := d.InsertMessage("t1", "m1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		seq, err := d.AppendMessageTip("t1", "s1", "m1")
		if err != nil {
			t.Fatal(err)
		}
		if seq != 1 {
			t.Fatalf("call %d: seq = %d, want 1 (re-setting the same tip must not advance)", i+1, seq)
		}
	}

	// A genuinely new tip advances the count.
	prev := "m1"
	if _, err := d.InsertMessage("t1", "m2", "user", &prev, ""); err != nil {
		t.Fatal(err)
	}
	seq, err := d.AppendMessageTip("t1", "s1", "m2")
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Fatalf("seq = %d after a new tip, want 2", seq)
	}
}

// TestUndoThenAppendMovesTipBack tests the flow the console drives: revert to an
// earlier message, then send a new one.
//
// Undo sets the tip to the reverted message's prev_id. The next send must move the
// tip forward again — if it does not, the session keeps rendering the old chain
// and the new message is invisible even though its row exists.
func TestUndoThenAppendMovesTipBack(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t1", SessionInput{Name: "s1", Model: "m"}); err != nil {
		t.Fatal(err)
	}

	// u1 -> a1 (the chain before reverting)
	if _, err := d.InsertMessage("t1", "u1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AppendMessageTip("t1", "s1", "u1"); err != nil {
		t.Fatal(err)
	}
	a1 := "u1"
	if _, err := d.InsertMessage("t1", "a1", "assistant", &a1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AppendMessageTip("t1", "s1", "a1"); err != nil {
		t.Fatal(err)
	}

	// Revert to a1: the tip becomes a1's prev (u1).
	m, ok, err := d.GetMessage("t1", "a1")
	if err != nil || !ok {
		t.Fatalf("get a1: ok=%v err=%v", ok, err)
	}
	if _, err := d.SetTip("t1", "s1", m.PrevID); err != nil {
		t.Fatal(err)
	}
	if tip, _ := d.SessionTip("t1", "s1"); tip != "u1" {
		t.Fatalf("after revert tip = %q, want u1", tip)
	}

	// A new message after the revert must become the tip.
	u2prev := "u1"
	if _, err := d.InsertMessage("t1", "u2", "user", &u2prev, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := d.AppendMessageTip("t1", "s1", "u2"); err != nil {
		t.Fatal(err)
	}
	if tip, _ := d.SessionTip("t1", "s1"); tip != "u2" {
		t.Fatalf("after sending again tip = %q, want u2 — the new message is otherwise invisible", tip)
	}

	// And the reverted message is no longer reachable from the tip.
	chain, err := d.Chain("t1", "u2", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range chain {
		if msg.ID == "a1" {
			t.Fatal("the reverted message is still on the chain")
		}
	}
}
