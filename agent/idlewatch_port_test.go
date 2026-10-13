package agent

import (
	"testing"

	"github.com/easydev-org/easyagent/store"
)

// Ported from the TS agent's idlewatch.test.ts: only the TIP matters, and it
// counts as "stuck" only when it is an assistant step whose last meaningful
// part is a tool_result with no text after it.

func mkAssistant(t *testing.T, d *store.DB, tenant, id string, prev *string, text string, toolResults int) {
	t.Helper()
	if _, err := d.InsertMessage(tenant, id, "assistant", prev, ""); err != nil {
		t.Fatal(err)
	}
	seq := 0
	for i := 0; i < toolResults; i++ {
		if err := d.InsertPart(tenant, id, "tool_result", seq, map[string]any{"call_id": "c"}); err != nil {
			t.Fatal(err)
		}
		seq++
	}
	if text != "" {
		if err := d.InsertPart(tenant, id, "text", seq, map[string]any{"text": text}); err != nil {
			t.Fatal(err)
		}
	}
}

func newIdleWatch(t *testing.T) (*IdleWatch, *store.DB) {
	t.Helper()
	d := testStore(t)
	rt := NewRuntime(RuntimeDeps{DB: d})
	return NewIdleWatch(rt, "t", 0), d
}

func TestIdleWatchStuckOnToolResult(t *testing.T) {
	w, d := newIdleWatch(t)

	// A tip with a tool_result and NO text -> stuck.
	mkAssistant(t, d, "t", "a1", nil, "", 2)
	if !w.stuckOnToolResult("a1") {
		t.Fatal("assistant ending on tool_result should be stuck")
	}

	// Whitespace-only text still counts as no text -> stuck.
	mkAssistant(t, d, "t", "a2", nil, "   \n", 1)
	if !w.stuckOnToolResult("a2") {
		t.Fatal("whitespace-only text should be treated as no text")
	}

	// Trailing text -> NOT stuck.
	mkAssistant(t, d, "t", "a3", nil, "here is the result", 1)
	if w.stuckOnToolResult("a3") {
		t.Fatal("assistant with trailing text should not be stuck")
	}

	// No tool parts -> NOT stuck.
	mkAssistant(t, d, "t", "a4", nil, "done", 0)
	if w.stuckOnToolResult("a4") {
		t.Fatal("assistant with no tool parts should not be stuck")
	}

	// A user tip -> NOT stuck.
	if _, err := d.InsertMessage("t", "u1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	if w.stuckOnToolResult("u1") {
		t.Fatal("user tip should not be stuck")
	}

	// Missing message -> NOT stuck.
	if w.stuckOnToolResult("ghost") {
		t.Fatal("missing message should not be stuck")
	}
}

// Only the TIP is inspected, not earlier steps.
func TestIdleWatchOnlyInspectsTip(t *testing.T) {
	w, d := newIdleWatch(t)
	mkAssistant(t, d, "t", "a1", nil, "", 1)          // earlier dangling tool
	mkAssistant(t, d, "t", "a2", ptr("a1"), "sum", 0) // tip with text
	if w.stuckOnToolResult("a2") {
		t.Fatal("tip with text should not be stuck even if an earlier step dangled")
	}
}

// The no-progress backoff: same tip across sweeps -> skip after maxNoProg.
func TestIdleWatchBackoff(t *testing.T) {
	w, _ := newIdleWatch(t)
	// First sighting records the tip (not yet backed off).
	if w.backedOff("s", "tip1") {
		t.Fatal("first sighting should not be backed off")
	}
	for i := 0; i < w.maxNoProg; i++ {
		w.backedOff("s", "tip1")
	}
	if !w.backedOff("s", "tip1") {
		t.Fatal("repeated identical tip should back off")
	}
	// A new tip resets the counter.
	if w.backedOff("s", "tip2") {
		t.Fatal("a new tip should reset the backoff")
	}
}

func ptr(s string) *string { return &s }
