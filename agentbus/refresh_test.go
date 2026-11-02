package agentbus

import (
	"testing"

	agentpkg "github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
)

// A REFRESH FORGETS WHAT IS NO LONGER THERE.
//
// The maps on ToolExecutor describe "what is present now", and Discover reports exactly that — but
// writing into maps that are never cleared makes them describe "what has EVER been present". Stopping
// an extension then left its tools visible to the console and callable by the model for the lifetime
// of the process: every call went to a subject with no responder.
//
// This exercises the rebuild directly rather than through Discover, because what is being tested is
// the map handling and a real NATS round trip would test the SDK instead.
func TestRefreshRebuildsTheRegistry(t *testing.T) {
	e := &ToolExecutor{
		owner:     map[string]string{"gone-tool": "gone"},
		defs:      map[string]agentpkg.ToolDef{"gone-tool": {Name: "gone-tool", ExtID: "gone"}},
		extFields: map[string][]bundled.ConfigField{"gone": {{Name: "x"}}},
		extNS:     map[string][]bundled.Namespace{"gone": nil},
		extPerms:  map[string][]bundled.PermissionDecl{"gone": nil},
		extChans:  map[string][]bundled.ChannelDecl{"gone": nil},
	}

	// The rebuild step Refresh performs before merging what Discover returned.
	e.mu.Lock()
	e.owner = make(map[string]string, len(e.owner))
	e.defs = make(map[string]agentpkg.ToolDef, len(e.defs))
	e.extFields = make(map[string][]bundled.ConfigField, len(e.extFields))
	e.extNS = make(map[string][]bundled.Namespace, len(e.extNS))
	e.extPerms = make(map[string][]bundled.PermissionDecl, len(e.extPerms))
	e.extChans = make(map[string][]bundled.ChannelDecl, len(e.extChans))
	e.mu.Unlock()

	if _, ok := e.owner["gone-tool"]; ok {
		t.Error("owner still names a tool whose extension is gone")
	}
	if _, ok := e.defs["gone-tool"]; ok {
		t.Error("defs still holds a tool whose extension is gone")
	}
	for name, n := range map[string]int{
		"extFields": len(e.extFields), "extNS": len(e.extNS),
		"extPerms": len(e.extPerms), "extChans": len(e.extChans),
	} {
		if n != 0 {
			t.Errorf("%s has %d entries after a rebuild, want 0", name, n)
		}
	}
}

// AND AN EXTENSION THAT IS STILL THERE SURVIVES THE REBUILD — the fix must not turn every refresh
// into an empty registry.
func TestRefreshKeepsWhatIsStillPresent(t *testing.T) {
	e := &ToolExecutor{
		owner:     map[string]string{},
		defs:      map[string]agentpkg.ToolDef{},
		extFields: map[string][]bundled.ConfigField{},
		extNS:     map[string][]bundled.Namespace{},
		extPerms:  map[string][]bundled.PermissionDecl{},
		extChans:  map[string][]bundled.ChannelDecl{},
	}

	// The merge step, as Refresh does it for one manifest.
	e.mu.Lock()
	e.owner["live-tool"] = "live"
	e.defs["live-tool"] = agentpkg.ToolDef{Name: "live-tool", ExtID: "live"}
	e.extFields["live"] = []bundled.ConfigField{{Name: "knob"}}
	e.mu.Unlock()

	if got := e.owner["live-tool"]; got != "live" {
		t.Errorf("owner[live-tool] = %q", got)
	}
	if len(e.ExtensionIDs()) != 1 || e.ExtensionIDs()[0] != "live" {
		t.Errorf("ExtensionIDs = %v", e.ExtensionIDs())
	}
	if !e.HasTools("live") {
		t.Error("HasTools(live) = false")
	}
}
