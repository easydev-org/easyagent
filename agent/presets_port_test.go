package agent

import "testing"

// Ported from the TS agent's presets.test.ts / preset-missing.test.ts: a user
// preset shadows a system preset with the same id, and deleting the user row
// restores the system one.

func TestPresetUserShadowsSystem(t *testing.T) {
	p := NewPresets(NewMemoryKV())
	p.SeedSystem("t", PresetRow{ID: "default", SystemPrompt: "sys", IsSystem: true})

	got, ok := p.Get("t", "default")
	if !ok || got.SystemPrompt != "sys" || !got.IsSystem {
		t.Fatalf("system preset = %+v ok=%v", got, ok)
	}

	// Upsert writes the USER keyspace, shadowing the system row.
	p.Upsert("t", PresetRow{ID: "default", SystemPrompt: "user"})
	got, ok = p.Get("t", "default")
	if !ok || got.SystemPrompt != "user" || !got.IsUserOverride {
		t.Fatalf("after upsert = %+v ok=%v; want the user override", got, ok)
	}

	// Delete removes only the user row -> system version comes back.
	p.Delete("t", "default")
	got, ok = p.Get("t", "default")
	if !ok || got.SystemPrompt != "sys" || !got.IsSystem {
		t.Fatalf("after delete = %+v ok=%v; want the system version restored", got, ok)
	}
}

func TestPresetListMergesKeyspaces(t *testing.T) {
	p := NewPresets(NewMemoryKV())
	p.SeedSystem("t", PresetRow{ID: "a", SystemPrompt: "sys-a"})
	p.SeedSystem("t", PresetRow{ID: "b", SystemPrompt: "sys-b"})
	p.Upsert("t", PresetRow{ID: "b", SystemPrompt: "user-b"})
	p.Upsert("t", PresetRow{ID: "c", SystemPrompt: "user-c"})

	list := p.List("t")
	if len(list) != 3 {
		t.Fatalf("list = %d presets, want 3 (a,b,c): %+v", len(list), list)
	}
	if list[0].ID != "a" || list[1].ID != "b" || list[2].ID != "c" {
		t.Fatalf("list order = %s,%s,%s", list[0].ID, list[1].ID, list[2].ID)
	}
	if list[1].SystemPrompt != "user-b" {
		t.Fatalf("b = %q, want the user override", list[1].SystemPrompt)
	}
}

func TestPresetMissing(t *testing.T) {
	p := NewPresets(NewMemoryKV())
	if _, ok := p.Get("t", "nope"); ok {
		t.Fatal("missing preset reported present")
	}
	if len(p.List("t")) != 0 {
		t.Fatal("empty tenant listed presets")
	}
}

// Tenants are isolated: one tenant's presets never leak into another's.
func TestPresetTenantIsolation(t *testing.T) {
	p := NewPresets(NewMemoryKV())
	p.Upsert("t1", PresetRow{ID: "x", SystemPrompt: "t1-x"})
	if _, ok := p.Get("t2", "x"); ok {
		t.Fatal("t2 saw t1's preset")
	}
	p.SeedSystem("t2", PresetRow{ID: "x", SystemPrompt: "t2-x"})
	a, _ := p.Get("t1", "x")
	b, _ := p.Get("t2", "x")
	if a.SystemPrompt != "t1-x" || b.SystemPrompt != "t2-x" {
		t.Fatalf("isolation broken: t1=%q t2=%q", a.SystemPrompt, b.SystemPrompt)
	}
}
