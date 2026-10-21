package agent

import "testing"

// TestBlockedToolIsNotOfferedButStillVisible pins the line between the two views.
//
// The console (ListTools) must show a tool whose required config is unset — it is
// the only place the user can learn WHICH knob to fill. The turn must not offer it
// to the model, because the model would call it and get an error it cannot fix.
//
// Both halves are asserted together because the interesting property is the
// DIFFERENCE; testing either alone would pass even if the two collapsed into one.
func TestBlockedToolIsNotOfferedButStillVisible(t *testing.T) {
	tools := []ToolDef{
		{Name: "no-config", ExtID: "x"},
		{Name: "needs", ExtID: "x", RequiredConfig: []string{"knob"}},
	}
	empty := ConfigStore{C: NewConfig(NewMemoryKV())}

	// The model's view: the blocked tool is gone.
	offered := FilterBlockedByConfig(tools, empty, "t1")
	if len(offered) != 1 || offered[0].Name != "no-config" {
		t.Fatalf("turn view = %v, want only the tool with no required config", names(offered))
	}

	// The console's view: everything is there.
	if len(tools) != 2 {
		t.Fatalf("console view lost a tool")
	}

	// And once the knob is set, the tool is offered again.
	empty.C.SetExt("t1", "x", "", "knob", "value")
	offered = FilterBlockedByConfig(tools, empty, "t1")
	if len(offered) != 2 {
		t.Fatalf("after setting the knob, turn view = %v, want both", names(offered))
	}
}

func names(ts []ToolDef) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.Name)
	}
	return out
}
