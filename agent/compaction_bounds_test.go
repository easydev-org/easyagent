package agent

import (
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// Boundary tests for the compaction primitives, ported from the reference
// agent's compaction / compact-budget suites. They lock in the pure-function
// behaviour that the end-to-end tests only exercise indirectly.

func entry(id, role, text string, toolCalls, toolIn, toolRes int) FoldEntry {
	return FoldEntry{ID: id, Role: role, Text: text, ToolCalls: toolCalls, ToolInput: toolIn, ToolResult: toolRes}
}

// ① foldEntries concatenates the MULTIPLE text parts of ONE assistant message
// into a single entry.Text (not just the last part), and FoldQA renders it.
func TestFoldEntriesConcatenatesTextParts(t *testing.T) {
	msgs := []store.Message{{ID: "a1", Role: "assistant"}}
	parts := []store.Part{
		{MessageID: "a1", Type: "text", Seq: 0, Data: `{"text":"step one "}`},
		{MessageID: "a1", Type: "text", Seq: 1, Data: `{"text":"step two"}`},
		{MessageID: "a1", Type: "tool", Seq: 2, Data: `{"id":"c1","name":"echo","input":{}}`},
	}
	entries, _ := foldEntries(msgs, parts)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].Text != "step one step two" {
		t.Fatalf("text parts not concatenated: %q", entries[0].Text)
	}
	if entries[0].ToolCalls != 1 {
		t.Fatalf("tool calls = %d, want 1", entries[0].ToolCalls)
	}
	out := FoldQA(entries, nil)
	if !strings.Contains(out, "step one step two") {
		t.Fatalf("folded QA missing the concatenated text: %q", out)
	}
}

// ② Regression: a turn whose TEXT is tiny but whose tool RESULTS are huge must
// still fold. (The real bug: 868k-context sessions measured ~40k and were
// declared "too short" because only text was counted.)
func TestSplitScanToolHeavyTurnFolds(t *testing.T) {
	entries := []FoldEntry{
		entry("u1", "user", "run it", 0, 0, 0),
		entry("a1", "assistant", "done", 5, 500, 50_000), // tiny text, huge results
		entry("u2", "user", "again", 0, 0, 0),
		entry("a2", "assistant", "done", 3, 300, 40_000),
	}
	r := SplitScan(entries, 100, 100) // small tail budget
	if len(r.Folded) == 0 {
		t.Fatal("a tool-heavy turn must fold despite little text")
	}
	found := false
	for _, e := range r.Folded {
		if e.ID == "a1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tool-heavy turn a1 not folded: %+v", r.Folded)
	}
}

// ③ FoldQA keeps a TRUNCATED snippet of each tool result (the trace), capped
// at 200 chars + an ellipsis, not the full content.
func TestFoldQAToolResultSnippetTruncated(t *testing.T) {
	long := strings.Repeat("Z", 500)
	out := FoldQA(
		[]FoldEntry{entry("a1", "assistant", "working", 1, 10, 200)},
		map[string][]string{"a1": {long}},
	)
	if !strings.Contains(out, "tool: ") {
		t.Fatalf("tool-result trace line missing: %q", out)
	}
	if !strings.Contains(out, "ZZZ") {
		t.Fatalf("snippet body missing: %q", out)
	}
	if !strings.Contains(out, "…") {
		t.Fatalf("snippet not truncated with an ellipsis: %q", out)
	}
	// 200 Zs + the "tool: " prefix + newlines — well under the full 500.
	if len(out) >= 400 {
		t.Fatalf("snippet not truncated (len %d): %q", len(out), out)
	}
}

// ④ With no snippets, FoldQA must NOT emit a tool-result trace line.
func TestFoldQAOmitsEmptySnippetLine(t *testing.T) {
	out := FoldQA([]FoldEntry{entry("a1", "assistant", "plain", 0, 0, 0)}, nil)
	if strings.Contains(out, "tool: ") {
		t.Fatalf("empty snippet produced a trace line: %q", out)
	}
	// An empty-string snippet is skipped too.
	out2 := FoldQA(
		[]FoldEntry{entry("a1", "assistant", "plain", 1, 0, 0)},
		map[string][]string{"a1": {""}},
	)
	if strings.Contains(out2, "tool: ") {
		t.Fatalf("empty-string snippet produced a trace line: %q", out2)
	}
}

// ⑤ compactThreshold picks the SMALLER of the window fraction and the absolute
// force ceiling; the reason reflects which one won.
func TestCompactThresholdPicksSmaller(t *testing.T) {
	cases := []struct {
		name       string
		limit      int
		frac       float64
		force      int
		wantT      int
		wantReason string
	}{
		// Huge window: 0.9 * 2M = 1.8M > force 900k -> force wins.
		{"huge window uses force", 2_000_000, 0.9, 900_000, 900_000, "force"},
		// Small window: 0.9 * 100k = 90k < force 900k -> threshold wins.
		{"small window uses fraction", 100_000, 0.9, 900_000, 90_000, "threshold"},
		// No window, force set -> force alone.
		{"no window uses force", 0, 0.9, 900_000, 900_000, "force"},
		// No window, no force -> disabled.
		{"disabled", 0, 0.9, 0, 0, ""},
	}
	for _, c := range cases {
		got, reason := compactThreshold(c.limit, c.frac, c.force)
		if got != c.wantT || reason != c.wantReason {
			t.Errorf("%s: got %d/%q, want %d/%q", c.name, got, reason, c.wantT, c.wantReason)
		}
	}
}

// ⑥ compactAtFraction defaults to 0.9 and clamps to (0, 1].
func TestCompactAtFractionClamp(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0, 0.9}, {-1, 0.9}, {1.5, 1}, {1, 1}, {0.5, 0.5},
	}
	for _, c := range cases {
		if got := compactAtFraction(c.in); got != c.want {
			t.Errorf("compactAtFraction(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// ⑦ EntryCost counts text + tool-call args + tool results (+ per-call framing),
// so each class contributes to the fold decision.
func TestEntryCostCountsAllClasses(t *testing.T) {
	text := EstimateTokens(strings.Repeat("a", 400)) // ~100 tokens (4 chars/token)
	base := EntryCost(entry("x", "assistant", strings.Repeat("a", 400), 0, 0, 0))
	if base < text {
		t.Fatalf("text-only cost %d < its token estimate %d", base, text)
	}
	withTools := EntryCost(entry("x", "assistant", strings.Repeat("a", 400), 2, 1000, 2000))
	if withTools <= base {
		t.Fatalf("tool input/result not counted: base=%d with=%d", base, withTools)
	}
	// A turn with ONLY tool weight still costs more than nothing.
	if EntryCost(entry("y", "assistant", "", 3, 500, 5000)) <= 0 {
		t.Fatal("a tool-heavy turn with no text costs nothing")
	}
	// EstimateMessages counts content + tool-call names/args.
	msgs := []llm.Message{{Role: llm.RoleAssistant, Content: "hello"}}
	if EstimateMessages(msgs) <= 0 {
		t.Fatal("message estimate must be positive")
	}
}

// ⑧ An empty chain folds nothing (compactWithBudgets returns false, no
// checkpoint written).
func TestCompactEmptyChainIsNoop(t *testing.T) {
	db := testStore(t)
	_ = db.CreateSession("t", store.SessionInput{Name: "s", Model: "gw/m"})
	rt := NewRuntime(RuntimeDeps{DB: db, Lease: NewMemoryLease()})
	written, err := rt.compactWithBudgets(t.Context(), "t", "s", "manual", 1, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if written {
		t.Fatal("an empty chain must not produce a checkpoint")
	}
}
