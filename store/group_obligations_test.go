package store

import "testing"

/**
 * THE INTENT CONSTANTS MUST MATCH WHAT THE TOOL LAYER WRITES.
 *
 * `bundled` restates "request" and "reply" as its own constants rather than importing this package for two strings —
 * bundled is imported BY agentbus, and the dependency has to stay one-way. The duplication is safe only while
 * something checks it, which is this test: a rename on either side becomes a failure here rather than a message that
 * silently stops opening or closing debts.
 */
func TestIntentConstantsAreStable(t *testing.T) {
	if GroupIntentRequest != "request" {
		t.Errorf("GroupIntentRequest = %q", GroupIntentRequest)
	}
	if GroupIntentReply != "reply" {
		t.Errorf("GroupIntentReply = %q", GroupIntentReply)
	}
}

/**
 * A REPLY TO THE ASKER SETTLES; A REPLY TO ANYONE ELSE DOES NOT.
 *
 * THE ADDRESSING IS THE RULE, and it is what makes "must reply once" mean something: an answer to the group is seen
 * by everyone and confirms nothing to anyone in particular.
 */
func TestFulfilIsScopedToTheAsker(t *testing.T) {
	d := newTestDB(t)
	in := &SessionInput{Name: "c", Model: "p/m"}
	if err := d.CreateSession("t1", in); err != nil {
		t.Fatal(err)
	}
	other := &SessionInput{Name: "o", Model: "p/m"}
	if err := d.CreateSession("t1", other); err != nil {
		t.Fatal(err)
	}
	g := &Group{Name: "g"}
	if err := d.CreateGroup("t1", g, in.ID, []string{other.ID}, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := d.OpenReplyObligations("t1", g.ID, in.ID, []string{other.ID}, 1, "m1", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}

	// A REPLY TO SOMEBODY ELSE SETTLES NOTHING.
	if n, err := d.FulfilReplyObligations("t1", g.ID, other.ID, "someone-else", "2026-01-01T00:01:00Z"); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Errorf("a reply to a third party settled %d obligations, want 0", n)
	}
	if open, _ := d.ObligationsForGroup("t1", g.ID); len(open) != 1 {
		t.Fatalf("obligations = %d, want 1 still open", len(open))
	}

	// A REPLY TO THE ASKER SETTLES IT.
	if n, err := d.FulfilReplyObligations("t1", g.ID, other.ID, in.ID, "2026-01-01T00:02:00Z"); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Errorf("settled %d, want 1", n)
	}
	if open, _ := d.ObligationsForGroup("t1", g.ID); len(open) != 0 {
		t.Errorf("obligations = %+v, want none", open)
	}
}
