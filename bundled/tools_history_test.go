package bundled

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * A STUB CONVERSATION: one user prompt, one assistant message that both spoke and called a tool,
 * and the tool's (large) result.
 *
 * It is built as REAL STORED PARTS — the same JSON the runtime writes — because the whole point of
 * these tests is what the tools make of the stored shape. A stub that handed back pre-decoded
 * entries would test the renderer and none of the reading.
 */
func historyStubDeps() *stubDeps {
	parts := []MessagePartRow{
		{MessageID: "m1", Type: "text", Seq: 0, Data: `{"text":"please deploy the app"}`},
		{MessageID: "m2", Type: "text", Seq: 0, Data: `{"text":"deploying now"}`},
		{MessageID: "m2", Type: "tool", Seq: 1,
			Data: `{"id":"call-1","name":"sandbox-job-start","input":{"cmd":"make deploy"}}`},
		{MessageID: "m2", Type: "tool_result", Seq: 2,
			// BUILT AS JSON, not by concatenating a raw string: a newline inside a JSON string must be
			// escaped, and pasting one in produces a part the tools cannot decode — which would make
			// these tests pass against a stored shape that never occurs.
			Data: mustJSON(map[string]any{
				"tool_use_id": "call-1",
				"content":     strings.Repeat("build-line\n", 400) + "DEPLOY-OK",
				"state":       "ok",
			})},
	}
	// OLDEST FIRST, matching what MessageChain returns: depth counts back from the tip, so the
	// oldest message has the highest depth.
	return &stubDeps{
		chain: []ChainEntry{
			{ID: "m1", Role: "user", CreatedAt: "2026-01-02 09:59:00", Seq: 1},
			{ID: "m2", Role: "assistant", CreatedAt: "2026-01-02 10:00:00", Seq: 2},
		},
		parts: parts,
	}
}

// mustJSON encodes a stored part body.
func mustJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// callHistory runs a history tool and returns its outcome.
func callHistory(t *testing.T, tool string, args map[string]any) agent.ToolOutcome {
	t.Helper()
	e, err := New(historyStubDeps())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(args)
	out, err := e.Execute(context.Background(), "t1", "s1", "c1", tool, string(raw))
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	return out
}

// historyEntries pulls the structured entries out of a history tool's outcome.
//
// THE FIELD IS `any` ON THE WIRE, so every assertion has to go through one helper rather than
// repeating the assertion at each call site — and a shape change then breaks in one place.
func historyEntries(t *testing.T, out agent.ToolOutcome) []map[string]any {
	t.Helper()
	meta, ok := out.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %T, want map[string]any", out.Metadata)
	}
	entries, ok := meta["entries"].([]map[string]any)
	if !ok {
		t.Fatalf("entries = %T", meta["entries"])
	}
	return entries
}

// historyMeta reads one metadata field.
func historyMeta(t *testing.T, out agent.ToolOutcome, key string) any {
	t.Helper()
	meta, ok := out.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %T", out.Metadata)
	}
	return meta[key]
}

/**
 * TEXT ONLY BY DEFAULT: A TOOL LOG MUST NOT DROWN THE CONVERSATION.
 *
 * `include` defaults to ["text"]. Before this, the search walked tool names but never their output,
 * so a model could not find what a command printed — and the only way to see it was to widen the
 * query until everything matched.
 */
func TestHistorySearchDefaultsToTextOnly(t *testing.T) {
	// DEPLOY-OK appears ONLY in the tool result, never in prose.
	out := callHistory(t, "history-search", map[string]any{"query": "DEPLOY-OK"})
	if n := len(historyEntries(t, out)); n != 0 {
		t.Fatalf("a result-only match was returned without include=[result]: %d entries", n)
	}

	// A prose match still works.
	out = callHistory(t, "history-search", map[string]any{"query": "deploy the app"})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m1" {
		t.Fatalf("prose search = %+v", entries)
	}
}

// Asking for results explicitly finds the tool output.
func TestHistorySearchCanIncludeResults(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{
		"query":   "DEPLOY-OK",
		"include": []any{"text", "result"},
	})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m2" {
		t.Fatalf("result search = %+v", entries)
	}
	got, _ := entries[0]["matched"].([]string)
	if len(got) != 1 || got[0] != includeResult {
		t.Fatalf("matched = %v, want [result]", got)
	}
}

// Tool NAMES and arguments are a separate kind from their output.
func TestHistorySearchToolKindMatchesNameAndArgs(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{
		"query":   "make deploy",
		"include": []any{"tool"},
	})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m2" {
		t.Fatalf("tool search = %+v", entries)
	}
}

/**
 * AN UNKNOWN include NAME IS AN ERROR.
 *
 * A caller that writes "results" instead of "result" would otherwise get text-only results and
 * believe the search covered tool output — the same silent-wrong-answer shape as the default.
 */
func TestHistorySearchRejectsUnknownInclude(t *testing.T) {
	e, _ := New(historyStubDeps())
	_, err := e.Execute(context.Background(), "t1", "s1", "c1", "history-search",
		`{"query":"x","include":["results"]}`)
	if err == nil {
		t.Fatal("an unknown include kind was accepted")
	}
}

/**
 * EVERY ENTRY IS CUT TO preview_chars AND SAYS SO.
 *
 * The tool result here is ~4.4k characters. Without a per-entry limit one page would be dominated by
 * it, and `max_total_chars` would then cut the page off after a single entry.
 */
func TestHistorySearchTruncatesPerEntryAndAnnouncesIt(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{
		"query":   "DEPLOY-OK",
		"include": []any{"result"},
	})
	entries := historyEntries(t, out)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	body, _ := entries[0]["content"].(string)
	if len(body) > defaultPreviewChars {
		t.Fatalf("entry was not cut: %d chars", len(body))
	}
	if entries[0]["truncated"] != true {
		t.Fatal("a cut entry did not say it was truncated")
	}
	if n, _ := entries[0]["full_chars"].(int); n <= defaultPreviewChars {
		t.Fatalf("full_chars = %v, want more than the preview", entries[0]["full_chars"])
	}
	// The rendered text names the tool that returns the rest.
	if !strings.Contains(out.Content, "history-read") {
		t.Fatalf("the truncation marker does not name history-read:\n%s", out.Content)
	}
}

// preview_chars=0 means no limit, which is how a caller opts out of the cut.
func TestHistorySearchPreviewZeroIsUnlimited(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{
		"query": "DEPLOY-OK", "include": []any{"result"}, "preview_chars": 0,
	})
	entries := historyEntries(t, out)
	body, _ := entries[0]["content"].(string)
	if len(body) < defaultPreviewChars {
		t.Fatalf("preview_chars=0 still cut the entry: %d chars", len(body))
	}
	if entries[0]["truncated"] == true {
		t.Fatal("an untruncated entry was marked truncated")
	}
}

/**
 * history-read RETURNS THE WHOLE MESSAGE, UNTRUNCATED.
 *
 * This is the other half of truncation: a model that sees `truncated` must be able to get the rest
 * without re-running a search with a bigger preview (which re-scans the chain).
 */
func TestHistoryReadReturnsFullContent(t *testing.T) {
	out := callHistory(t, "history-read", map[string]any{"id": "m2", "part": "result"})
	if !strings.Contains(out.Content, "DEPLOY-OK") {
		t.Fatalf("history-read did not return the full result:\n%s", out.Content)
	}
	if len(out.Content) <= defaultPreviewChars {
		t.Fatalf("history-read cut the content at %d chars", len(out.Content))
	}

	// part=text returns only the prose.
	out = callHistory(t, "history-read", map[string]any{"id": "m2", "part": "text"})
	if strings.TrimSpace(out.Content) != "deploying now" {
		t.Fatalf("part=text = %q", out.Content)
	}

	// Omitting part returns everything: prose AND the tool call AND its result.
	out = callHistory(t, "history-read", map[string]any{"id": "m2"})
	for _, want := range []string{"deploying now", "sandbox-job-start", "DEPLOY-OK"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("history-read without part omitted %q", want)
		}
	}
}

// An id from another conversation is a miss, not a silent empty answer.
func TestHistoryReadUnknownIDIsAnError(t *testing.T) {
	e, _ := New(historyStubDeps())
	if _, err := e.Execute(context.Background(), "t1", "s1", "c1", "history-read", `{"id":"nope"}`); err == nil {
		t.Fatal("an unknown message id was accepted")
	}
}

/**
 * CROSS-SESSION READS GO THROUGH THE SAME PATH.
 *
 * `session` names another conversation in the same tenant; the chain is walked from ITS tip. An
 * unknown name must be an ERROR — "no results" would read as "that conversation is empty".
 */
func TestHistorySearchReadsAnotherSession(t *testing.T) {
	d := historyStubDeps()
	d.existing = map[string]bool{"s1": true, "other": true}
	// The OTHER conversation has its own tip and its own message.
	d.tips = map[string]string{"s1": "m2", "other": "x1"}
	d.chainByTip = map[string][]ChainEntry{
		"m2": d.chain,
		"x1": {{ID: "x1", Role: "user", CreatedAt: "2026-02-01 08:00:00", Seq: 1}},
	}
	d.parts = append(d.parts, MessagePartRow{
		MessageID: "x1", Type: "text", Seq: 0, Data: `{"text":"the other conversation is about caching"}`,
	})

	e, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"query": "caching", "session": "other"})
	out, rerr := e.Execute(context.Background(), "t1", "s1", "c1", "history-search", string(raw))
	if rerr != nil {
		t.Fatalf("cross-session search: %v", rerr)
	}
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "x1" {
		t.Fatalf("cross-session entries = %+v", entries)
	}
	if historyMeta(t, out, "session") != "other" {
		t.Fatalf("session echoed as %v", historyMeta(t, out, "session"))
	}
}

// A name that does not exist is refused, so a typo cannot look like an empty conversation.
func TestHistorySearchUnknownSessionIsAnError(t *testing.T) {
	d := historyStubDeps()
	d.existing = map[string]bool{"s1": true}
	e, _ := New(d)
	_, err := e.Execute(context.Background(), "t1", "s1", "c1", "history-search",
		`{"query":"x","session":"typo"}`)
	if err == nil {
		t.Fatal("an unknown session name was accepted")
	}
}

/**
 * history-range-read WINDOWS BY seq, NOT depth.
 *
 * A DEPTH WINDOW IS NOT AN ADDRESS. `depth` is recomputed as `len(chain) - 1 - i`, so appending one
 * message shifts every depth in the conversation. A model that read "depth 1 is where the deploy
 * failed" and came back next turn would find different content there — no error, nothing to notice.
 *
 * `seq` is fixed when the message is written, so `from_seq: 2, to_seq: 3` means the second message
 * today and the second message tomorrow.
 */
func TestHistoryRangeReadWindowsBySeq(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{"from_seq": 2, "to_seq": 3})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m2" {
		t.Fatalf("seq window = %+v", entries)
	}

	// The whole chain, when no bounds are given.
	out = callHistory(t, "history-range-read", map[string]any{})
	if n := len(historyEntries(t, out)); n != 2 {
		t.Fatalf("unbounded read returned %d entries, want both", n)
	}
}

// to_seq is EXCLUSIVE, matching every other half-open range in the codebase.
func TestHistoryRangeReadToSeqIsExclusive(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{"from_seq": 1, "to_seq": 2})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m1" {
		t.Fatalf("to_seq is not exclusive: %+v", entries)
	}
}

// `last` keeps the newest N, which a seq window cannot express without knowing the count.
func TestHistoryRangeReadLastKeepsNewest(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{"last": 1})
	entries := historyEntries(t, out)
	if len(entries) != 1 || entries[0]["id"] != "m2" {
		t.Fatalf("last=1 = %+v, want the newest message", entries)
	}
}

// The bounds are reported, so a caller can page without first counting the conversation.
func TestHistoryRangeReadReportsSeqBounds(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{})
	if got := historyMeta(t, out, "seq_min"); got != int64(1) {
		t.Fatalf("seq_min = %v, want 1", got)
	}
	if got := historyMeta(t, out, "seq_max"); got != int64(2) {
		t.Fatalf("seq_max = %v, want 2", got)
	}
}

// A positional read is OLDEST FIRST: it re-reads a stretch in the order it happened.
func TestHistoryRangeReadIsOldestFirst(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{})
	entries := historyEntries(t, out)
	if len(entries) != 2 {
		t.Fatalf("entries = %+v", entries)
	}
	if entries[0]["id"] != "m1" || entries[1]["id"] != "m2" {
		t.Fatalf("order = %v, %v; want oldest first", entries[0]["id"], entries[1]["id"])
	}
}

// Omitting the bounds (or passing 0) means "no bound", not "stop immediately".
func TestHistoryRangeReadZeroBoundMeansUnbounded(t *testing.T) {
	out := callHistory(t, "history-range-read", map[string]any{"from_seq": 0, "to_seq": 0})
	if n := len(historyEntries(t, out)); n != 2 {
		t.Fatalf("zero bounds returned %d entries, want both", n)
	}
}

/**
 * THE RENDERED TEXT MUST SHOW THE ID.
 *
 * A model reads the text, not `metadata`. The id used to live ONLY in the metadata, so a search hit
 * gave the model a `[depth]` number and nothing to hand `history-read` — and a model that tried the
 * depth anyway got "no such message", which reads as a broken tool rather than a missing field.
 *
 * Found in the cluster: the model reported that it could not find an id and had guessed wrong twice.
 */
func TestHistorySearchShowsTheIDInTheRenderedText(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{"query": "deploy the app"})
	if !strings.Contains(out.Content, "id=m1") {
		t.Fatalf("the rendered line does not name the id:\n%s", out.Content)
	}
	// The depth is still there, and it is LABELLED, so the two numbers cannot be confused.
	if !strings.Contains(out.Content, "seq 1") {
		t.Fatalf("the rendered line lost the seq:\n%s", out.Content)
	}
}

// The id in the rendered text is the one history-read accepts — the two must not drift.
func TestHistoryRenderedIDIsReadable(t *testing.T) {
	out := callHistory(t, "history-search", map[string]any{"query": "deploy the app"})
	entries := historyEntries(t, out)
	if len(entries) != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	id, _ := entries[0]["id"].(string)
	if !strings.Contains(out.Content, "id="+id) {
		t.Fatalf("rendered text names a different id than metadata (%q):\n%s", id, out.Content)
	}
	// And history-read accepts it.
	if got := callHistory(t, "history-read", map[string]any{"id": id}); strings.TrimSpace(got.Content) == "" {
		t.Fatal("history-read returned nothing for the id the search printed")
	}
}

/**
 * DEPTH IS GONE FROM THE OUTPUT, NOT JUST FROM THE PARAMETERS.
 *
 * Leaving it in `entries[].depth` would keep offering a number that looks like an address: it is
 * recomputed from the chain's length, so it shifts for every message each time one is appended. The
 * tool would be documenting a handle that quietly retargets.
 *
 * This asserts on the RENDERED TEXT and the METADATA, because the two are read by different
 * consumers — a model reads the text, a client reads `data` — and either one still carrying depth
 * would be enough to mislead.
 */
func TestHistoryOutputHasNoDepth(t *testing.T) {
	for _, tool := range []string{"history-search", "history-range-read"} {
		out := callHistory(t, tool, map[string]any{"query": "deploy"})
		if strings.Contains(out.Content, "depth") {
			t.Errorf("%s: the rendered text still mentions depth:\n%s", tool, out.Content)
		}
		for _, row := range historyEntries(t, out) {
			if _, ok := row["depth"]; ok {
				t.Errorf("%s: an entry still carries a depth field: %+v", tool, row)
			}
			if _, ok := row["seq"]; !ok {
				t.Errorf("%s: an entry carries no seq: %+v", tool, row)
			}
		}
	}

	// history-read reports the position too, and must report the stable one.
	out := callHistory(t, "history-read", map[string]any{"id": "m2"})
	meta, _ := out.Metadata.(map[string]any)
	if _, ok := meta["depth"]; ok {
		t.Error("history-read still reports a depth")
	}
	if meta["seq"] != int64(2) {
		t.Errorf("history-read seq = %v, want 2", meta["seq"])
	}
}
