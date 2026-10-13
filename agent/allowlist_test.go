package agent

import "testing"

// P4: a delegation subtree may restrict which extension servers its tenants can
// use. filterByExtensionAllowlist drops tools from non-allowed extensions while
// always keeping built-ins (no ExtID).

func TestExtensionAllowlistFilters(t *testing.T) {
	tools := []ToolDef{
		{Name: "echo", ExtID: "workspace"},
		{Name: "shot", ExtID: "playwright"},
		{Name: "read", ExtID: ""}, // built-in: always allowed
	}
	// Unrestricted (nil resolver).
	if got := filterByExtensionAllowlist(tools, RuntimeDeps{}, "t"); len(got) != 3 {
		t.Fatalf("nil resolver should not filter: %d", len(got))
	}
	// Resolver returns nil for this tenant -> unrestricted.
	deps := RuntimeDeps{ExtensionAllowlist: func(string) []string { return nil }}
	if got := filterByExtensionAllowlist(tools, deps, "t"); len(got) != 3 {
		t.Fatalf("nil allowlist should not filter: %d", len(got))
	}
	// Only "workspace" allowed -> playwright's tool drops; built-in stays.
	deps = RuntimeDeps{ExtensionAllowlist: func(string) []string { return []string{"workspace"} }}
	got := filterByExtensionAllowlist(tools, deps, "t")
	if len(got) != 2 {
		t.Fatalf("allowlist [workspace] kept %d tools, want 2", len(got))
	}
	for _, tt := range got {
		if tt.ExtID == "playwright" {
			t.Fatal("playwright tool leaked past the allowlist")
		}
	}
	// An EMPTY (present-but-empty) allowlist allows only built-ins.
	deps = RuntimeDeps{ExtensionAllowlist: func(string) []string { return []string{} }}
	got = filterByExtensionAllowlist(tools, deps, "t")
	if len(got) != 1 || got[0].ExtID != "" {
		t.Fatalf("empty allowlist should keep only built-ins, got %+v", got)
	}
}
