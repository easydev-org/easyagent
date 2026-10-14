package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/store"
)

// The resume watchdog re-triggers ONLY sessions whose last turn ended with a
// resumable reason (length / retryable), and nothing else.

func newResumeWatch(t *testing.T) (*ResumeWatch, *store.DB) {
	t.Helper()
	d := testStore(t)
	// A lease that never claims: Wake returns immediately WITHOUT draining,
	// so the enqueued trigger stays observable (the real Wake consumes it).
	rt := NewRuntime(RuntimeDeps{DB: d, Lease: &neverClaimLease{}})
	return NewResumeWatch(rt, "t", 0), d
}

func mkSession(t *testing.T, d *store.DB, name, reason string) {
	t.Helper()
	if err := d.CreateSession("t", store.SessionInput{Name: name, Model: "gw/m"}); err != nil {
		t.Fatal(err)
	}
	if reason != "" {
		if err := d.SetTurnEnd("t", name, reason); err != nil {
			t.Fatal(err)
		}
	}
}

// pendingTrigger reports whether a session has an unconsumed `trigger` whose
// source is the resume watchdog.
func pendingTrigger(t *testing.T, d *store.DB, name string) bool {
	t.Helper()
	rows, err := d.DrainAll("t", name)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range rows {
		if m.MsgType == "trigger" && m.Source == "system:resume" {
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
		w, d := newResumeWatch(t)
		mkSession(t, d, "s", c.reason)
		w.Sweep(context.Background())
		got := pendingTrigger(t, d, "s")
		if got != c.resumable {
			t.Errorf("reason %q: resumed=%v, want %v", c.reason, got, c.resumable)
		}
	}
}

// A resumable session with no model is skipped (nothing to run).
func TestResumeWatchSkipsNoModel(t *testing.T) {
	w, d := newResumeWatch(t)
	if err := d.CreateSession("t", store.SessionInput{Name: "s"}); err != nil { // no model
		t.Fatal(err)
	}
	_ = d.SetTurnEnd("t", "s", "length")
	w.Sweep(context.Background())
	if pendingTrigger(t, d, "s") {
		t.Fatal("a model-less session should not be resumed")
	}
}

// The no-progress backoff: the same tip across sweeps backs off after maxNoProg.
func TestResumeWatchBackoff(t *testing.T) {
	w, _ := newResumeWatch(t)
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
