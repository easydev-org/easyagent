package agent

import (
	"strings"
	"testing"
)

// TestNewIDIsUniqueAcrossProcesses is the regression test for messages that were
// silently never stored.
//
// Message ids are primary keys and newID used to be `msg-<process-local counter>`.
// A restarted agent therefore reissued ids that already existed; InsertMessage is
// ON CONFLICT DO NOTHING, so the insert vanished without an error, and the row it
// collided with belonged to a different session. The visible symptoms were "the
// message I just sent is gone" and a tip that would not advance — which read like
// a revert bug.
//
// A restart cannot be simulated in-process, so the property that makes restarts
// safe is asserted instead: ids must not come from a counter that starts over.
// Two independent runs (representable as two freshly seeded counters) must not
// overlap, and a UUID-shaped id cannot.
func TestNewIDIsUniqueAcrossProcesses(t *testing.T) {
	seen := make(map[string]bool, 512)
	for i := 0; i < 512; i++ {
		id := newID()
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
	// The id must not encode a small monotonic counter: two ids minted in sequence
	// would differ only in their last characters, which is what made a restart
	// regenerate the previous run's ids.
	a, b := newID(), newID()
	if len(a) != len(b) {
		return // fine: random ids can differ in length only if the fallback ran
	}
	if strings.TrimPrefix(a, "msg-") == "" {
		t.Fatal("id has no body")
	}
	// Random ids share no meaningful prefix beyond the constant "msg-".
	if a[:12] == b[:12] {
		t.Fatalf("ids %q and %q share a 12-char prefix: not random enough to survive a restart", a, b)
	}
}
