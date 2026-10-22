package agent

import "testing"

// TestDeltaPartsGroupsByID is the regression test for the console rendering one
// block per streamed fragment.
//
// The root cause was in STORAGE, not rendering: every delta was appended as its
// own element, so a step with 163 reasoning fragments was persisted as 163
// separate reasoning parts and the console dutifully drew 163 blocks. The grouping
// key is the delta's part id — the same key the live client uses.
func TestDeltaPartsGroupsByID(t *testing.T) {
	var d deltaParts
	// One part, three fragments.
	d.add("step-1", "Hel")
	d.add("step-1", "lo ")
	d.add("step-1", "world")
	if got := d.Texts(); len(got) != 1 || got[0] != "Hello world" {
		t.Fatalf("same id must fold into one part, got %q", got)
	}

	// A new id starts a new part.
	d.add("step-2", "second")
	got := d.Texts()
	if len(got) != 2 || got[1] != "second" {
		t.Fatalf("a new id must start a new part, got %q", got)
	}
}

// TestDeltaPartsEmptyIDJoinsCurrent: producers that emit no id must not explode
// the output into one block per frame — that is the exact shape being fixed.
func TestDeltaPartsEmptyIDJoinsCurrent(t *testing.T) {
	var d deltaParts
	d.add("", "a")
	d.add("", "b")
	d.add("", "c")
	if got := d.Texts(); len(got) != 1 || got[0] != "abc" {
		t.Fatalf("id-less deltas must accumulate, got %q", got)
	}
}

// TestDeltaPartsSkipsEmptyText keeps blank frames from opening a part.
func TestDeltaPartsSkipsEmptyText(t *testing.T) {
	var d deltaParts
	d.add("s", "")
	d.add("s", "x")
	d.add("s", "")
	if got := d.Texts(); len(got) != 1 || got[0] != "x" {
		t.Fatalf("empty text must not create or extend a part, got %q", got)
	}
}
