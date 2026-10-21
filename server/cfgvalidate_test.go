package server

import (
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/bundled"
)

// TestValidateKnobValueRejectsProtoWrapper is the regression test for a silent
// corruption that cost real debugging time.
//
// A caller sending the Connect-ES client's internal shape
// `{"kind":{"case":"stringValue","value":"http://x"}}` (or the decoded
// `{"stringValue":"http://x"}`) had it stored verbatim, because the endpoint
// decodes the value as a plain string. Nothing failed: the console displayed it,
// the extension received it, and only the extension's own URL parse failed — with
// a message about a malformed URL, far from the write that caused it.
func TestValidateKnobValueRejectsProtoWrapper(t *testing.T) {
	bad := []string{
		`{"stringValue":"http://x"}`,
		`{"kind":{"case":"stringValue","value":"http://x"}}`,
		`{"numberValue":3}`,
		`{"boolValue":true}`,
	}
	for _, v := range bad {
		if err := validateKnobValue("easyops-url", v); err == nil {
			t.Errorf("accepted a serialised protobuf Value: %s", v)
		}
	}
}

// TestValidateKnobValueAcceptsRealValues guards the other side: the check must
// never reject a legitimate value, or it becomes the next outage.
func TestValidateKnobValueAcceptsRealValues(t *testing.T) {
	ok := map[string]string{
		"easyops-url":          "http://easyops.dev.svc.cluster.local:80",
		"selenium-url":         "http://selenium.dev.svc.cluster.local:80",
		"gateway-url":          "https://api.example.com/v4/ai",
		"namespace":            "dev",
		"servers":              `[{"host":"forgejo.dev.svc.cluster.local","kind":"gitea"}]`,
		"brave_api_key":        "BSAabc123",
		"easyops-url(c)leared": "",
	}
	for name, v := range ok {
		if err := validateKnobValue(name, v); err != nil {
			t.Errorf("rejected a legitimate value for %s: %v", name, err)
		}
	}
}

// TestValidateKnobValueRejectsBadURL keeps a URL knob from holding a sentence.
func TestValidateKnobValueRejectsBadURL(t *testing.T) {
	for _, v := range []string{"not a url", "easyops.dev.svc.cluster.local", "://missing-scheme"} {
		if err := validateKnobValue("easyops-url", v); err == nil {
			t.Errorf("accepted a non-URL for a -url knob: %q", v)
		}
	}
}

// TestValidateStructuredKnob pins the list/object contract the extension relies on.
func TestValidateStructuredKnob(t *testing.T) {
	required := true
	cf := bundled.ConfigField{
		Name:     "sandboxes",
		Type:     "list",
		ItemType: "object",
		MinItems: 1,
		Fields: []bundled.ConfigFieldSpec{
			{Name: "name", Type: "string", Required: required},
			{Name: "url", Type: "string", Required: required},
		},
	}

	cases := []struct {
		value string
		ok    bool
		why   string
	}{
		{`[{"name":"a","url":"http://x"}]`, true, "minimal valid item"},
		{`[{"name":"a","url":"http://x","os":"linux"}]`, true, "extra keys are fine"},
		{`[]`, false, "min_items is 1"},
		{`{}`, false, "a list must be an array"},
		{`[{"name":"a"}]`, false, "url is required"},
		{`["just-a-string"]`, false, "item_type is object"},
		{`not json`, false, "must parse"},
		{``, true, "clearing is always allowed"},
	}
	for _, c := range cases {
		err := validateStructuredKnob(cf, c.value)
		if c.ok && err != nil {
			t.Errorf("%s: unexpected error for %s: %v", c.why, c.value, err)
		}
		if !c.ok {
			if err == nil {
				t.Errorf("%s: accepted %s", c.why, c.value)
			} else if !strings.Contains(err.Error(), cf.Name) {
				t.Errorf("%s: error should name the knob: %v", c.why, err)
			}
		}
	}
}
