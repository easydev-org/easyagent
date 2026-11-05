package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

// The client reads fields off `input` (`input.todos`, `input.repo`, …). Every encoding layer the
// provider adds is invisible to `json.Unmarshal` — it just yields a string — so a card renders
// empty with no error anywhere. These cases are the four shapes seen in practice.
/**
 * THE UNWRAPPING MOVED TO THE LLM LAYER, so this function parses and nothing more.
 *
 * The loop that used to live here guessed at a nesting depth, and its bound was arbitrary. The stream
 * protocol is now normalised where it is read (`llm.inputToString`), which means arguments arrive as an
 * object's JSON text — one decode is the whole job, and a producer that sends something else is reported
 * there rather than tripped over here.
 */
func TestDecodeToolInputParsesAnObject(t *testing.T) {
	got := decodeToolInput(`{"todos":[{"content":"a"}]}`)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("decodeToolInput = %T, want map", got)
	}
	if _, ok := m["todos"]; !ok {
		t.Fatalf("decoded map has no `todos` key: %#v", m)
	}
}

// An empty argument list is an EMPTY OBJECT, not an empty string: every tool handler reads fields off it,
// and a string there would make each of them check the type first.
func TestDecodeToolInputEmptyIsAnObject(t *testing.T) {
	got := decodeToolInput("")
	if m, ok := got.(map[string]any); !ok || len(m) != 0 {
		t.Fatalf("decodeToolInput(\"\") = %#v, want an empty map", got)
	}
	if got := decodeToolInput("   "); got == nil {
		t.Fatal("whitespace-only input produced nil")
	}
}

/**
 * A NON-OBJECT IS PASSED THROUGH, because it can be legitimate.
 *
 * A tool whose argument really is a bare string exists, and a model that emitted malformed JSON should have
 * that text shown rather than dropped — the reader can see what it tried. What must NOT happen is the old
 * behaviour of unwrapping a string repeatedly on the chance that its content parses: `"123"` is both a
 * string argument and valid JSON, and the loop could turn one into the other by accident.
 */
func TestDecodeToolInputLeavesOtherShapesAlone(t *testing.T) {
	if got := decodeToolInput(`"hello"`); got != "hello" {
		t.Errorf(`decodeToolInput("\"hello\"") = %#v, want "hello"`, got)
	}
	if got := decodeToolInput(`not json`); got != "not json" {
		t.Errorf("non-JSON input = %#v, want it unchanged", got)
	}
	// A string whose content IS valid JSON stays a string here — the LLM layer decides what a nested
	// encoding means, and it does so where the protocol is known.
	if got := decodeToolInput(`"{\"a\":1}"`); got != `{"a":1}` {
		t.Errorf("a JSON-looking string was unwrapped to %#v", got)
	}
}

// The emitted value must survive a JSON round-trip as an object — that is what the wire carries.
func TestDecodeToolInputRoundTripsAsObject(t *testing.T) {
	args := `{"todos":[{"content":"a","status":"pending"}]}`
	wire, err := json.Marshal(map[string]any{"input": decodeToolInput(args)})
	if err != nil {
		t.Fatal(err)
	}
	var back struct {
		Input json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(wire, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Input) == 0 || back.Input[0] != '{' {
		t.Fatalf("input on the wire is not an object: %s", back.Input)
	}
}

// truncateHead must not split a multi-byte rune in half — a broken sequence in a log line makes the
// surrounding output unreadable, and this is the path that reports the failure being investigated.
func TestTruncateHeadCutsOnRuneBoundary(t *testing.T) {
	// Every rune here is 3 bytes, so byte 160 is guaranteed to land mid-rune.
	s := strings.Repeat("工", 200)
	got := truncateHead(s)
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("expected an ellipsis, got %q", got)
	}
	body := strings.TrimSuffix(got, "…")
	// utf8.ValidString is the direct check: a cut mid-rune leaves an invalid sequence, which is
	// exactly what shows up as a replacement character in a terminal or a log viewer.
	if !utf8.ValidString(body) {
		t.Fatalf("truncated on a non-rune boundary: %q", body)
	}
	if len(body) > 160 {
		t.Fatalf("kept %d bytes, over the 160 limit", len(body))
	}
}

// Short values pass through untouched — the ellipsis is for long payloads only.
func TestTruncateHeadLeavesShortValues(t *testing.T) {
	if got := truncateHead("hello"); got != "hello" {
		t.Errorf("truncateHead(hello) = %q", got)
	}
}
