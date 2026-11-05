package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
)

// The resume watchdog re-triggers ONLY sessions whose last turn ended with a
// resumable reason (length / retryable), and nothing else.

// newResumeWatch builds the watcher AND the queue it will write to. A resume is a message like any other,
// so a test that wants to see one has to look in the same place the runtime does.
func newResumeWatch(t *testing.T) (*ResumeWatch, *store.DB, *mailbox.Memory) {
	t.Helper()
	q := mailbox.NewMemory()
	d := testStore(t)
	// A lease that never claims: Wake returns immediately WITHOUT draining,
	// so the enqueued trigger stays observable (the real Wake consumes it).
	rt := NewRuntime(RuntimeDeps{Mailbox: q, DB: d, Lease: &neverClaimLease{}})
	return NewResumeWatch(rt, "t", 0), d, q
}

// mkSession creates a session and returns the ID the watchdog must use; `name`
// is only the display label.
func mkSession(t *testing.T, d *store.DB, name, reason string) string {
	t.Helper()
	in := &store.SessionInput{Name: name, Model: "gw/m"}
	if err := d.CreateSession("t", in); err != nil {
		t.Fatal(err)
	}
	if reason != "" {
		if err := d.SetTurnEnd("t", in.ID, reason); err != nil {
			t.Fatal(err)
		}
	}
	return in.ID
}

// pendingTrigger reports whether a session has an unconsumed `trigger` whose
// source is the resume watchdog.
/**
 * pendingTrigger asks the QUEUE, not the database.
 *
 * THE TWO USED TO BE THE SAME THING, and the test silently kept passing after they stopped being: it
 * drained a table nothing wrote to and got an empty list, which reads as "the watcher did not resume" —
 * the answer the negative cases want and the positive ones do not.
 */
func pendingTrigger(t *testing.T, q *mailbox.Memory, name string) bool {
	t.Helper()
	msgs, err := q.Drain(context.Background(), "t", name)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.Type == "trigger" && m.Source == "system:resume" {
			return true
		}
	}
	return false
}

func TestResumeWatchResumesOnlyResumable(t *testing.T) {
	cases := []struct {
		reason    string
		resumable bool
	}{
		{"length", true},
		{"retryable", true},
		{"stop", false},
		{"max-steps", false},
		{"interrupted", false},
		{"locklost", false},
		{"overflow", false},
		{"error", false},
		{"", false},
	}
	for _, c := range cases {
		w, d, q := newResumeWatch(t)
		sid := mkSession(t, d, "s", c.reason)
		w.Sweep(context.Background())
		got := pendingTrigger(t, q, sid)
		if got != c.resumable {
			t.Errorf("reason %q: resumed=%v, want %v", c.reason, got, c.resumable)
		}
	}
}

// A resumable session with no model is skipped (nothing to run).
func TestResumeWatchSkipsNoModel(t *testing.T) {
	w, d, q := newResumeWatch(t)
	in := &store.SessionInput{Name: "s"} // no model
	if err := d.CreateSession("t", in); err != nil {
		t.Fatal(err)
	}
	_ = d.SetTurnEnd("t", in.ID, "length")
	w.Sweep(context.Background())
	if pendingTrigger(t, q, in.ID) {
		t.Fatal("a model-less session should not be resumed")
	}
}

// The no-progress backoff: the same tip across sweeps backs off after maxNoProg.
func TestResumeWatchBackoff(t *testing.T) {
	w, _, _ := newResumeWatch(t)
	if w.backedOff("s", "tip1") {
		t.Fatal("first sighting should not be backed off")
	}
	for i := 0; i < w.maxNoProg; i++ {
		w.backedOff("s", "tip1")
	}
	if !w.backedOff("s", "tip1") {
		t.Fatal("repeated identical tip should back off")
	}
	if w.backedOff("s", "tip2") {
		t.Fatal("a new tip should reset the backoff")
	}
}

// neverClaimLease never wins the lease, so Wake is a no-op (the trigger stays
// in the mailbox for the test to observe).
type neverClaimLease struct{}

func (neverClaimLease) Claim(context.Context, string, string) (bool, error) { return false, nil }
func (neverClaimLease) Renew(context.Context, string, string) (bool, error) { return false, nil }
func (neverClaimLease) Release(context.Context, string, string) error       { return nil }

// resumeText differs by reason.
func TestResumeText(t *testing.T) {
	if resumeText("length") == resumeText("retryable") {
		t.Fatal("length and retryable should have distinct nudges")
	}
}
