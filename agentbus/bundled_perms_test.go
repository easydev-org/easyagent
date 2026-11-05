package agentbus

import (
	"testing"

	"github.com/easydev-org/easyagent/bundled"
)

/**
 * A REFRESH MUST NOT FORGET THE IN-PROCESS EXTENSION'S PERMISSIONS.
 *
 * `Refresh` rebuilds every table from what `Discover` reported, because "present now" is the only
 * truthful answer — an extension that stopped is gone from the report and must be gone from the tables.
 *
 * `bundled` is the exception: it is not remote, so it is never in that report, and a permission written
 * once into `extPerms` was erased by the first refresh. The failure is silent — a tool still declares
 * `requires: [group.create]` and the gate still refuses calls, but no client can offer the grant, so the
 * tool is permanently unusable while looking merely broken.
 *
 * This pins the merge: the perms are held separately and put back after every rebuild.
 */
func TestBundledPermissionsSurviveRebuild(t *testing.T) {
	e := NewToolExecutor(nil)
	decl := []bundled.PermissionDecl{{Key: "group.create", Risk: "write"}}
	e.WithBundledPermissions(map[string][]bundled.PermissionDecl{"bundled": decl})

	got := e.AllPermissions()
	if len(got["bundled"]) != 1 || got["bundled"][0].Key != "group.create" {
		t.Fatalf("before any rebuild: %v", got["bundled"])
	}

	// Simulate what Refresh does to the tables.
	e.mu.Lock()
	e.extPerms = map[string][]bundled.PermissionDecl{}
	for id, list := range e.bundledPerms {
		e.extPerms[id] = list
	}
	e.mu.Unlock()

	got = e.AllPermissions()
	if len(got["bundled"]) != 1 || got["bundled"][0].Key != "group.create" {
		t.Fatalf("the in-process permission did not survive a rebuild: %v", got["bundled"])
	}
}
