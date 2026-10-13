package agent

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPlanPresetWhitelistAndEnv(t *testing.T) {
	kv := NewMemoryKV()
	presets := NewPresets(kv)
	presets.SeedSystem("t1", PresetRow{
		ID: "default", SystemPrompt: "Be terse.", MaxTurns: 4,
		Tools: []string{"echo", "bundled.todo-write"}, IsSystem: true,
	})
	registry := StaticTools{
		{Name: "echo", ExtID: "bundled"},
		{Name: "todo-write", ExtID: "bundled"},
		{Name: "danger", ExtID: "ext"},
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	plan, err := Plan(context.Background(), RuntimeDeps{
		Presets: PresetStore{P: presets}, Config: ConfigStore{C: NewConfig(kv)},
		Registry: registry, MaxTurns: 8,
	}, PlanInput{Tenant: "t1", SessionPreset: "default", SessionLocale: "zh", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if plan.MaxTurns != 4 {
		t.Errorf("maxTurns = %d, want preset's 4", plan.MaxTurns)
	}
	if !strings.Contains(plan.System, "Be terse.") || !strings.Contains(plan.System, "<env>") {
		t.Errorf("system = %q", plan.System)
	}
	if !strings.Contains(plan.System, "2026-10-09") || !strings.Contains(plan.System, "中文") {
		t.Errorf("env block (date/zh directive) missing: %q", plan.System)
	}
	// whitelist keeps echo + bundled.todo-write (qualified form), drops danger.
	if len(plan.Tools) != 2 {
		t.Fatalf("tools = %+v", plan.Tools)
	}
}

func TestPlanFailsClosedOnMissingPreset(t *testing.T) {
	_, err := Plan(context.Background(), RuntimeDeps{
		Presets: PresetStore{P: NewPresets(NewMemoryKV())},
	}, PlanInput{Tenant: "t1", SessionPreset: "ghost"})
	if err == nil {
		t.Fatal("a session referencing a missing preset must FAIL CLOSED")
	}
}

func TestPlanNoPresetIsUnrestricted(t *testing.T) {
	plan, err := Plan(context.Background(), RuntimeDeps{
		Registry: StaticTools{{Name: "a"}, {Name: "b"}}, MaxTurns: 3,
	}, PlanInput{Tenant: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tools) != 2 {
		t.Errorf("a session with no preset should see all tools: %+v", plan.Tools)
	}
}

func TestFilterToolsDenylist(t *testing.T) {
	all := StaticTools{{Name: "echo", ExtID: "bundled"}, {Name: "danger", ExtID: "ext"}}
	got := FilterTools(all, nil, []string{"ext-danger"})
	if len(got) != 1 || got[0].Name != "echo" {
		t.Errorf("denylist filter = %+v", got)
	}
}
