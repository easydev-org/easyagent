package server

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/easydev-org/easyagent/bundled"
)

// looksLikeProtoValueJSON reports whether a knob's stored text is a serialised
// protobuf Value rather than the value itself.
//
// This is what a caller gets wrong when it sends
// `{"value": {"kind": {"case": "stringValue", "value": "http://x"}}}` — the shape
// a Connect-ES CLIENT uses internally — to an endpoint whose body is plain JSON.
// The server decodes it as a string and stores the JSON text, so the knob ends up
// holding `{"stringValue":"http://x"}`. Nothing errors: the console shows it, the
// extension receives it, and only the extension's own parse fails, days later, with
// a message about a malformed URL.
//
// Rejecting it here turns a silent corruption into an error at the point of the
// mistake.
func looksLikeProtoValueJSON(v string) bool {
	s := strings.TrimSpace(v)
	if !strings.HasPrefix(s, "{") {
		return false
	}
	var m map[string]any
	if json.Unmarshal([]byte(s), &m) != nil {
		return false
	}
	// The wrapper always has exactly these keys; a genuine object knob would not.
	if len(m) == 1 {
		if _, ok := m["stringValue"]; ok {
			return true
		}
		if _, ok := m["numberValue"]; ok {
			return true
		}
		if _, ok := m["boolValue"]; ok {
			return true
		}
	}
	if kind, ok := m["kind"].(map[string]any); ok && len(m) == 1 {
		if _, ok := kind["case"]; ok {
			return true
		}
	}
	return false
}

// validateKnobValue rejects values that cannot possibly be what the knob means.
//
// Deliberately narrow: it only catches shapes that are certainly wrong, so it can
// never reject a legitimate value. A knob whose name says "url" must not hold a
// sentence; a knob that is a bare URL must parse as one.
//
// format is the knob's DECLARED format, when the extension declares one. It is
// the authority; the name-based rule below is only a fallback for extensions that
// have not been taught to declare it yet (a "-url" suffix is a convention of this
// stack, not a contract).
func validateKnobValue(name, format, value string) error {
	if value == "" {
		return nil // clearing a knob is always allowed
	}
	if looksLikeProtoValueJSON(value) {
		return fmt.Errorf(
			"%q looks like a serialised protobuf Value, not the value itself; "+
				"send the plain string (e.g. \"http://host:80\")", name)
	}
	if format == "uri" || format == "host" {
		u, err := url.Parse(value)
		if err != nil {
			return fmt.Errorf("%q must be a URL: %v", name, err)
		}
		if format == "uri" && (u.Scheme == "" || u.Host == "") {
			return fmt.Errorf("%q must be an absolute URL (scheme://host[:port]), got %q", name, value)
		}
		if format == "host" && u.Host == "" && u.Path == "" {
			return fmt.Errorf("%q must be a host name, got %q", name, value)
		}
		return nil
	}
	// Fallback: no declared format, so lean on the naming convention this stack
	// uses (easyops-url, selenium-url, gateway-url).
	if format == "" {
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, "-url") || lower == "url" {
			u, err := url.Parse(value)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("%q must be an absolute URL (scheme://host[:port]), got %q", name, value)
			}
		}
	}
	return nil
}

// validateStructuredKnob checks a list/object knob against its DECLARED shape.
//
// The console renders a form and produces correct JSON, but nothing stops another
// client from storing `{"a":1}` in a list knob or `[]` where at least one entry is
// required. The extension then fails at parse time — in its own words, far from
// the write that caused it — so the shape is checked here where the mistake is.
func validateStructuredKnob(cf bundled.ConfigField, value string) error {
	if value == "" {
		return nil
	}
	if cf.Type != "list" && cf.Type != "object" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return fmt.Errorf("%q must be valid JSON when it is of type %s: %v", cf.Name, cf.Type, err)
	}

	items, ok := decoded.([]any)
	if cf.Type == "list" {
		if !ok {
			return fmt.Errorf("%q is a list and must be a JSON array", cf.Name)
		}
		if cf.MinItems > 0 && len(items) < cf.MinItems {
			return fmt.Errorf("%q needs at least %d item(s), got %d", cf.Name, cf.MinItems, len(items))
		}
		if cf.MaxItems > 0 && len(items) > cf.MaxItems {
			return fmt.Errorf("%q allows at most %d item(s), got %d", cf.Name, cf.MaxItems, len(items))
		}
	} else {
		if _, ok := decoded.(map[string]any); !ok {
			return fmt.Errorf("%q is an object and must be a JSON object", cf.Name)
		}
		return nil
	}

	// Per-item: required keys must be present. Values are not type-checked — the
	// extension's own parser is the authority on meaning, and over-validating here
	// would reject values that are unusual but legal.
	if cf.ItemType != "object" || len(cf.Fields) == 0 {
		return nil
	}
	for i, it := range items {
		obj, ok := it.(map[string]any)
		if !ok {
			return fmt.Errorf("%q item %d must be an object", cf.Name, i+1)
		}
		for _, f := range cf.Fields {
			if !f.Required {
				continue
			}
			if v, ok := obj[f.Name]; !ok || v == nil || v == "" {
				return fmt.Errorf("%q item %d is missing required field %q", cf.Name, i+1, f.Name)
			}
		}
	}
	return nil
}
