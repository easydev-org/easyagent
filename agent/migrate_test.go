package agent

import "testing"

// TestMigrateCopiesASharedKnobToEveryDeclarer: the flat key was ONE cell, so a
// name declared by two extensions belongs to both. Giving it to an arbitrary
// winner would silently empty the other.
func TestMigrateCopiesASharedKnobToEveryDeclarer(t *testing.T) {
	kv := NewMemoryKV()
	kv.Put(cfgKey("acme", "", "", "sandboxes"), `[{"name":"w1"}]`)
	c := NewConfig(kv)

	moved := c.MigrateConfigNamespaces(map[string][]string{
		"sandboxes": {"git", "sandbox"},
	})
	if moved != 1 {
		t.Errorf("moved = %d, want 1", moved)
	}
	for _, ext := range []string{"git", "sandbox"} {
		if v, ok := c.GetExt("acme", ext, "", "sandboxes"); !ok || v == "" {
			t.Errorf("%s did not receive the migrated value", ext)
		}
	}
	// The legacy cell is gone, so a second run is a no-op.
	if _, ok := c.Get("acme", "sandboxes"); ok {
		t.Error("the legacy key must be removed after migration")
	}
	if again := c.MigrateConfigNamespaces(map[string][]string{"sandboxes": {"git"}}); again != 0 {
		t.Errorf("second run moved %d, want 0 (idempotent)", again)
	}
}

// TestMigrateLeavesTenantWideKnobsAlone: locale/default_model are not an
// extension's, so they must stay in the tenant-wide tier.
func TestMigrateLeavesTenantWideKnobsAlone(t *testing.T) {
	kv := NewMemoryKV()
	kv.Put(cfgKey("acme", "", "", "locale"), "zh")
	c := NewConfig(kv)

	if moved := c.MigrateConfigNamespaces(map[string][]string{"sandboxes": {"git"}}); moved != 0 {
		t.Errorf("moved = %d, want 0", moved)
	}
	if v, ok := c.Get("acme", "locale"); !ok || v != "zh" {
		t.Error("a tenant-wide knob must not be touched")
	}
}

// TestMigrateDoesNotClobberANewerValue: if the new address is already populated
// (the user edited it after the upgrade), the migration must not overwrite it.
func TestMigrateDoesNotClobberANewerValue(t *testing.T) {
	kv := NewMemoryKV()
	kv.Put(cfgKey("acme", "", "", "sandboxes"), "OLD")
	c := NewConfig(kv)
	c.SetExt("acme", "git", "", "sandboxes", "NEW")

	c.MigrateConfigNamespaces(map[string][]string{"sandboxes": {"git"}})
	if v, _ := c.GetExt("acme", "git", "", "sandboxes"); v != "NEW" {
		t.Errorf("git.sandboxes = %q, want the newer NEW", v)
	}
}

// TestSplitLegacy: a namespaced key must not be mistaken for a legacy one.
func TestSplitLegacy(t *testing.T) {
	cases := []struct {
		rest, tenant, name string
		ok                 bool
	}{
		{"acme.sandboxes", "acme", "sandboxes", true},
		{"acme.git.sandboxes", "", "", false},
		{"acme.ops.build.addr", "", "", false},
		{"acme", "", "", false},
		{".x", "", "", false},
	}
	for _, c := range cases {
		tenant, name, ok := splitLegacy(c.rest)
		if ok != c.ok || tenant != c.tenant || name != c.name {
			t.Errorf("splitLegacy(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.rest, tenant, name, ok, c.tenant, c.name, c.ok)
		}
	}
}
