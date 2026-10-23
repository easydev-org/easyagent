package agent

import (
	"fmt"
	"strings"
	"testing"
)

// TestTrimKeepsHeadAndTail: a finished result keeps its first and last halves and
// says how much went missing.
//
// Both ends, not just the tail: a build's errors are at the top and its outcome at the
// bottom, so keeping one end loses half the diagnosis. Streaming is unaffected — this
// only applies once the tool is done.
func TestTrimKeepsHeadAndTail(t *testing.T) {
	lines := make([]string, 1200)
	for i := range lines {
		lines[i] = fmt.Sprintf("line%d", i+1)
	}

	out := trimLinesForResult(lines, "en")
	if len(out) != resultKeepLines+1 {
		t.Fatalf("kept %d lines, want %d plus the marker", len(out), resultKeepLines)
	}
	if out[0] != "line1" {
		t.Errorf("first kept line = %q, want line1", out[0])
	}
	if out[resultHalfLines-1] != fmt.Sprintf("line%d", resultHalfLines) {
		t.Errorf("last head line = %q", out[resultHalfLines-1])
	}
	// The tail must be the REAL end, not the middle.
	if out[len(out)-1] != "line1200" {
		t.Errorf("last kept line = %q, want line1200", out[len(out)-1])
	}
	if !strings.Contains(out[resultHalfLines], "700") {
		t.Errorf("marker %q should state the omitted count (1200-500=700)", out[resultHalfLines])
	}
}

func TestTrimLeavesSmallResultsAlone(t *testing.T) {
	lines := []string{"a", "b", "c"}
	out := trimLinesForResult(lines, "en")
	if len(out) != 3 || strings.Contains(strings.Join(out, ""), "omitted") {
		t.Fatalf("a short result must pass through untouched, got %v", out)
	}
}

// At exactly the limit nothing is dropped: the boundary must not produce a marker
// saying zero lines were omitted.
func TestTrimBoundaryExactlyAtLimit(t *testing.T) {
	lines := make([]string, resultKeepLines)
	for i := range lines {
		lines[i] = fmt.Sprintf("l%d", i)
	}
	out := trimLinesForResult(lines, "en")
	if len(out) != resultKeepLines {
		t.Fatalf("got %d lines, want %d", len(out), resultKeepLines)
	}
	if strings.Contains(strings.Join(out, "\n"), "omitted") {
		t.Error("no marker expected when nothing was omitted")
	}
}

// The marker is agent-generated prose, so it follows the session locale.
func TestOmittedMarkerFollowsLocale(t *testing.T) {
	en := omittedMarker(700, "en")
	zh := omittedMarker(700, "zh-CN")
	if !strings.Contains(en, "700") || !strings.Contains(en, "omitted") {
		t.Errorf("en marker = %q", en)
	}
	if !strings.Contains(zh, "700") || !strings.Contains(zh, "省略") {
		t.Errorf("zh marker = %q", zh)
	}
}

// TrimToLimit must reduce the STREAM, not a copy: the terminal snapshot is emitted
// from the stream's own lines, and that frame is what the console persists.
func TestTrimToLimitMutatesTheStream(t *testing.T) {
	r := newResultStream()
	for i := 0; i < 1200; i++ {
		r.Append(fmt.Sprintf("line%d", i+1))
	}
	if got := len(r.Snapshot()); got != 1200 {
		t.Fatalf("precondition: stream holds %d lines", got)
	}
	r.TrimToLimit("en")
	if got := len(r.Snapshot()); got != resultKeepLines+1 {
		t.Fatalf("stream still holds %d lines after trim", got)
	}
	// Idempotent: the terminal frame is emitted after Close, and a retry must not
	// trim again or duplicate the marker.
	first := r.TrimToLimit("en")
	second := r.TrimToLimit("en")
	if first != second {
		t.Errorf("trim is not idempotent")
	}
}
