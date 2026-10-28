package server

import (
	"encoding/json"
	"testing"
)

// The event struct carries tool arguments as `input`. `structpb.NewStruct` accepts only its own
// set of Go types, and anything else goes through `jsonScalar`, which turns it into a STRING — so
// a client reading `input.todos` finds nothing and renders an empty card.
//
// This test pins the shapes that actually arrive from a provider.
func TestJSONSafeKeepsToolInputStructured(t *testing.T) {
	// What decodeToolInput produces: json.Unmarshal into `any`.
	var input any
	if err := json.Unmarshal([]byte(`{"todos":[{"content":"a","status":"pending"}]}`), &input); err != nil {
		t.Fatal(err)
	}
	if _, ok := input.(map[string]any); !ok {
		t.Fatalf("precondition: decoded input is %T, not map[string]any", input)
	}

	got := jsonSafe(map[string]any{"input": input})
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("jsonSafe returned %T", got)
	}
	inner, ok := m["input"].(map[string]any)
	if !ok {
		t.Fatalf("input became %T — a client reading `input.todos` would see nothing", m["input"])
	}
	if _, ok := inner["todos"]; !ok {
		t.Fatalf("input lost its `todos` key: %#v", inner)
	}
}

// A NESTED slice of maps is the shape that a shallow coercion misses.
func TestJSONSafeDescendsIntoToolInput(t *testing.T) {
	var input any
	if err := json.Unmarshal([]byte(`{"todos":[{"content":"a"}]}`), &input); err != nil {
		t.Fatal(err)
	}
	m := jsonSafe(map[string]any{"input": input}).(map[string]any)
	inner := m["input"].(map[string]any)
	todos, ok := inner["todos"].([]any)
	if !ok {
		t.Fatalf("todos became %T", inner["todos"])
	}
	if _, ok := todos[0].(map[string]any); !ok {
		t.Fatalf("todos[0] became %T", todos[0])
	}
}
