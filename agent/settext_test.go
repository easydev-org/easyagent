package agent

import (
	"fmt"
	"strings"
	"testing"
)

// TestSetTextReplacesRatherThanAppends is the regression test for a tool card that
// showed every line twice, in two overlapping runs.

// A streaming tool puts its output into the stream as it produces it, then RETURNS the
// same text as its result. Appending the result to the stream doubled it; the trim then
// kept the first 250 lines of the first copy and the last 250 of the second, so the card
// showed e.g. FT1..FT249 followed by FT652..FT900 — visibly two different ranges.
func TestSetTextReplacesRatherThanAppends(t *testing.T) {
	r := newResultStream()
	for i := 1; i <= 900; i++ {
		r.Append(fmt.Sprintf("FT%d", i))
	}
	// The same text arrives as the tool's result.
	var b strings.Builder
	for i := 1; i <= 900; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "FT%d", i)
	}
	r.SetText(b.String())

	got := r.Snapshot()
	if len(got) != 900 {
		t.Fatalf("stream holds %d lines after SetText, want 900 (not 1800)", len(got))
	}
	// No duplicates, and still in order.
	seen := map[string]bool{}
	for i, l := range got {
		if seen[l] {
			t.Fatalf("duplicate line %q at index %d", l, i)
		}
		seen[l] = true
		if want := fmt.Sprintf("FT%d", i+1); l != want {
			t.Fatalf("line %d = %q, want %q", i, l, want)
		}
	}
}

// After a SetText the trimmed result must be a coherent head+tail of ONE range.
func TestSetTextThenTrimKeepsOneRange(t *testing.T) {
	r := newResultStream()
	for i := 1; i <= 900; i++ {
		r.Append(fmt.Sprintf("FT%d", i))
	}
	var b strings.Builder
	for i := 1; i <= 900; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "FT%d", i)
	}
	r.SetText(b.String())
	out := trimLinesForResult(r.Snapshot(), "en")

	if len(out) != resultKeepLines+1 {
		t.Fatalf("trimmed to %d lines, want %d plus marker", len(out), resultKeepLines)
	}
	if out[0] != "FT1" {
		t.Errorf("head starts at %q, want FT1", out[0])
	}
	if out[resultHalfLines-1] != fmt.Sprintf("FT%d", resultHalfLines) {
		t.Errorf("head ends at %q, want FT%d", out[resultHalfLines-1], resultHalfLines)
	}
	// The tail must continue the SAME run: 900-250+1 .. 900.
	wantTailFirst := fmt.Sprintf("FT%d", 900-resultHalfLines+1)
	if out[resultHalfLines+1] != wantTailFirst {
		t.Errorf("tail starts at %q, want %q — a different range means the stream was "+
			"built from two copies", out[resultHalfLines+1], wantTailFirst)
	}
	if out[len(out)-1] != "FT900" {
		t.Errorf("tail ends at %q, want FT900", out[len(out)-1])
	}
}
