package llm

import (
	"encoding/json"
	"strings"
	"testing"
)

/**
 * THE STREAM ALLOWS EXACTLY TWO SHAPES FOR A TOOL CALL'S `input`, AND THIS IS THE ONLY PLACE THAT KNOWS
 * IT.
 *
 *	an OBJECT       the arguments, inline
 *	a JSON STRING   whose content is the arguments object
 *
 * Anything else is a protocol violation and is REPORTED. The previous behaviour unwrapped repeatedly until
 * the value stopped changing, which meant a third shape was handled by guessing a nesting depth — and the
 * guess could rewrite a legitimate string argument whose content happened to be valid JSON.
 */
func TestInputToStringAcceptsTheTwoShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"inline object", `{"todos":[{"content":"a"}]}`, `{"todos":[{"content":"a"}]}`},
		{"json string", `"{\"todos\":[{\"content\":\"a\"}]}"`, `{"todos":[{"content":"a"}]}`},
		{"empty string content", `""`, ""},
		{"object with whitespace", `{"a": 1}`, `{"a": 1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := inputToString(json.RawMessage(c.raw))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("inputToString(%s) = %q, want %q", c.raw, got, c.want)
			}
		})
	}
}

func TestInputToStringEmptyIsEmpty(t *testing.T) {
	for _, raw := range []string{``, `null`} {
		if got, err := inputToString(json.RawMessage(raw)); err != nil || got != "" {
			t.Errorf("inputToString(%q) = %q, %v; want empty", raw, got, err)
		}
	}
}

/**
 * A THIRD SHAPE IS AN ERROR, NOT A GUESS.
 *
 * Each of these used to be silently tolerated or silently mishandled. Reporting them is what makes a
 * provider change visible at the moment it happens, instead of as a tool that receives nothing and a model
 * that retries the same way.
 */
func TestInputToStringRejectsOtherShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"a bare number", `42`},
		{"a bare boolean", `true`},
		{"an array", `[1,2]`},
		// A string whose content is NOT an object: the protocol says the content is the arguments object.
		{"string holding a number", `"42"`},
		{"string holding an array", `"[1,2]"`},
		{"string holding prose", `"hello"`},
		// A string inside a string — the shape the old unwrapping loop existed to flatten.
		{"string in a string", `"\"{\\\"a\\\":1}\""`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := inputToString(json.RawMessage(c.raw)); err == nil {
				t.Errorf("inputToString(%s) accepted a shape the protocol does not define", c.raw)
			}
		})
	}
}

// The error must name what arrived, or a provider change is a mystery.
func TestInputToStringErrorIsDescriptive(t *testing.T) {
	_, err := inputToString(json.RawMessage(`42`))
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "neither an object nor a string") {
		t.Errorf("the error should say what the two shapes are: %v", err)
	}
}
