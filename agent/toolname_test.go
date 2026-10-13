package agent

import "testing"

// The LLM-visible tool name is the tool's OWN (sanitized) name — NOT
// `extId`-prefixed — mirroring the TS `toolQualifiedName`. Out-of-charset
// characters are replaced by '-' (the gateway rejects '.').
func TestLLMToolName(t *testing.T) {
	cases := []struct {
		def  ToolDef
		want string
	}{
		{ToolDef{Name: "todo-write", ExtID: "bundled"}, "todo-write"},
		{ToolDef{Name: "mail-send", ExtID: "bundled"}, "mail-send"},
		{ToolDef{Name: "model.image", ExtID: "bundled"}, "model-image"},
		{ToolDef{Name: "browser-navigate", ExtID: "playwright"}, "browser-navigate"},
		{ToolDef{Name: "weird/name.with.dots", ExtID: "x"}, "weird-name-with-dots"},
	}
	for _, c := range cases {
		if got := LLMToolName(c.def); got != c.want {
			t.Errorf("LLMToolName(%q) = %q, want %q", c.def.Name, got, c.want)
		}
	}
	// The host-facing qualified id (denylist) keeps the extId prefix.
	if QualifiedName(ToolDef{Name: "mail-send", ExtID: "bundled"}) != "bundled-mail-send" {
		t.Error("QualifiedName must keep the extId prefix")
	}
}

// WhitelistMatch accepts the bare name, the `<extId>.<name>` and the
// `<extId>-<name>` qualified forms.
func TestWhitelistMatchForms(t *testing.T) {
	def := ToolDef{Name: "search", ExtID: "extA"}
	for _, entry := range []string{"search", "extA.search", "extA-search"} {
		if !WhitelistMatch(def, map[string]bool{entry: true}) {
			t.Errorf("whitelist entry %q should match", entry)
		}
	}
	if WhitelistMatch(def, map[string]bool{"other": true}) {
		t.Error("an unrelated entry must not match")
	}
	// A same-named tool from another extension is NOT selected by extA.search.
	other := ToolDef{Name: "search", ExtID: "extB"}
	if WhitelistMatch(other, map[string]bool{"extA.search": true}) {
		t.Error("extA.search must not match extB's search")
	}
}
