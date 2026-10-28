package agent

import (
	"encoding/json"
	"testing"
)

// The client reads fields off `input` (`input.todos`, `input.repo`, …). Every encoding layer the
// provider adds is invisible to `json.Unmarshal` — it just yields a string — so a card renders
// empty with no error anywhere. These cases are the four shapes seen in practice.
func TestDecodeToolInputReachesAnObject(t *testing.T) {
	cases := []struct {
		name string
		args string
	}{
		{"plain object", `{"todos":[{"content":"a"}]}`},
		{"json string", `"{\"todos\":[{\"content\":\"a\"}]}"`},
		{"string in a string", `"\"{\\\"todos\\\":[{\\\"content\\\":\\\"a\\\"}]}\""`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := decodeToolInput(c.args)
			m, ok := got.(map[string]any)
			if !ok {
				t.Fatalf("decodeToolInput(%s) = %T, want map — a string here means every card "+
					"reading a field of it shows nothing", c.args, got)
			}
			if _, ok := m["todos"]; !ok {
				t.Fatalf("decoded map has no `todos` key: %#v", m)
			}
		})
	}
}

// A genuine bare string (a tool that takes a string argument) must survive as a string, and text
// that is not JSON at all must be passed through rather than dropped.
func TestDecodeToolInputLeavesOtherShapesAlone(t *testing.T) {
	if got := decodeToolInput(`"hello"`); got != "hello" {
		t.Errorf(`decodeToolInput("\"hello\"") = %#v, want "hello"`, got)
	}
	if got := decodeToolInput(`not json`); got != "not json" {
		t.Errorf("non-JSON input = %#v, want it unchanged", got)
	}
	if got := decodeToolInput(``); got != "" {
		t.Errorf("empty input = %#v, want empty string", got)
	}
}

// The emitted value must survive a JSON round-trip as an object — that is what the wire carries.
func TestDecodeToolInputRoundTripsAsObject(t *testing.T) {
	args := `"\"{\\\"todos\\\":[{\\\"content\\\":\\\"a\\\",\\\"status\\\":\\\"pending\\\"}]}\""`
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
