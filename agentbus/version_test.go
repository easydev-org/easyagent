package agentbus

import "testing"

// The version an extension announces must reach the client.
//
// IT WAS EMPTY FOR EVERY DISCOVERED EXTENSION, and nothing failed: `ExtensionInfo.version` is a plain
// string on the proto, so a client rendered nothing and "no version" reads as "unknown", not as a bug.
// Only the in-process bundled extension had one, because its version was hard-coded in the server.
//
// A SHAPE TEST, not a discovery test. It pins the accessor's three cases, which is what the RPC needs;
// the recording itself happens in Refresh, where a bus would be required to exercise it and where the
// existing tests already cover the rebuilt-maps property this extends.
func TestExtensionVersionAccessor(t *testing.T) {
	e := NewToolExecutor(nil)
	// A map with both a versioned and an unversioned entry: empty must stay empty rather than inherit a
	// neighbour's value.
	e.extVersions = map[string]string{"gitdev": "1.0", "noversion": ""}

	if got := e.ExtensionVersion("gitdev"); got != "1.0" {
		t.Errorf("gitdev version = %q, want 1.0", got)
	}
	if got := e.ExtensionVersion("noversion"); got != "" {
		t.Errorf("noversion = %q, want empty", got)
	}
	// AN UNKNOWN ID IS EMPTY, not a panic: a client may ask about an extension that went away between
	// the list and the detail call.
	if got := e.ExtensionVersion("gone"); got != "" {
		t.Errorf("unknown extension = %q, want empty", got)
	}
}

// A fresh executor must not panic on the accessor before any Refresh has run — which is what a read
// during boot would do if the map were nil.
func TestExtensionVersionBeforeRefresh(t *testing.T) {
	e := NewToolExecutor(nil)
	if got := e.ExtensionVersion("anything"); got != "" {
		t.Errorf("before Refresh = %q, want empty", got)
	}
}
