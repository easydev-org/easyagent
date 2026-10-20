package agent

import "testing"

// TestCfgKeyNamespacesByExtension: the stored address is
// (tenant, extId, namespace, name). The old key was (tenant, name), one GLOBAL
// flat namespace, so two extensions declaring a knob with the same name silently
// shared a value: setting git.sandboxes made sandbox.sandboxes read as
// configured. The extension MUST be in the key for that to be impossible.
func TestCfgKeyNamespacesByExtension(t *testing.T) {
	cases := []struct {
		name  string
		args  []string // tenant, extID, namespace, knob
		want  string
		panic string
	}{
		{"tenant-wide", []string{"acme", "", "", "locale"}, "cfg.acme.locale", ""},
		{"extension-level", []string{"acme", "git", "", "sandboxes"}, "cfg.acme.git.sandboxes", ""},
		{"namespaced", []string{"acme", "ops", "build", "addr"}, "cfg.acme.ops.build.addr", ""},
	}
	for _, c := range cases {
		got := cfgKey(c.args[0], c.args[1], c.args[2], c.args[3])
		if got != c.want {
			t.Errorf("%s: cfgKey = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestTwoExtensionsDoNotShareAValue: the regression that motivated all of this.
func TestTwoExtensionsDoNotShareAValue(t *testing.T) {
	c := NewConfig(NewMemoryKV())
	c.SetExt("acme", "git", "", "sandboxes", `[{"name":"w1"}]`)

	if v, ok := c.GetExt("acme", "git", "", "sandboxes"); !ok || v == "" {
		t.Fatal("git's own value must be readable")
	}
	// The OTHER extension must see nothing: this is the bug that was live.
	if _, ok := c.GetExt("acme", "sandbox", "", "sandboxes"); ok {
		t.Error("sandbox must NOT see git's sandboxes value")
	}
	// And the tenant-wide tier is a third, independent address.
	if _, ok := c.Get("acme", "sandboxes"); ok {
		t.Error("the tenant-wide tier must not see an extension's value")
	}
}

// TestNamespacesAreIsolatedWithinOneExtension: one extension may cover several
// jobs, and a knob scoped to a namespace must not leak into a sibling namespace.
func TestNamespacesAreIsolatedWithinOneExtension(t *testing.T) {
	c := NewConfig(NewMemoryKV())
	c.SetExt("acme", "ops", "build", "addr", "buildkit:1234")
	c.SetExt("acme", "ops", "pvc", "addr", "storage-class-x")

	if v, _ := c.GetExt("acme", "ops", "build", "addr"); v != "buildkit:1234" {
		t.Errorf("build.addr = %q", v)
	}
	if v, _ := c.GetExt("acme", "ops", "pvc", "addr"); v != "storage-class-x" {
		t.Errorf("pvc.addr = %q", v)
	}
	// Extension-level is a FOURTH address, shared by every namespace of the ext.
	c.SetExt("acme", "ops", "", "url", "http://ops")
	if v, _ := c.GetExt("acme", "ops", "", "url"); v != "http://ops" {
		t.Errorf("ext-level url = %q", v)
	}
}

// TestConfigExtUsesDeclaredNamespace: a read has to address the same key the
// write used, so the declared namespace must drive ConfigExt.
func TestConfigExtUsesDeclaredNamespace(t *testing.T) {
	c := NewConfig(NewMemoryKV())
	c.SetKnobNamespaces(map[string]string{"ops.addr": "build"})
	c.SetExt("acme", "ops", "build", "addr", "buildkit:1234")

	if v, ok := c.ConfigExt("acme", "ops", "addr"); !ok || v != "buildkit:1234" {
		t.Errorf("ConfigExt = %q ok=%v, want the namespaced value", v, ok)
	}
}
