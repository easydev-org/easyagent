package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/store"
)

// TestPersistSinkStoresToolResults is the regression test for tool results that
// never reached the database.
//
// A step's events arrive in a fixed order: deltas, tool-call, then tool-result,
// and finally message-added. Parts used to be written only on message-added — by
// which point the result map was still empty for the CURRENT step, and the
// previous step's message was already marked final. The database ended up with 23
// tool calls and zero tool_results, so the console showed a tool card with an
// empty Content block and rebuilt history had the same hole.
//
// The assertion is on the STORED rows, not on the sink: the bug was in what got
// written, so a test of in-memory state would have passed all along.
func TestPersistSinkStoresToolResults(t *testing.T) {
	db := testStore(t)
	// The sink advances the session's tip, so the session has to exist first.
	if err := db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.InsertMessage("t1", "user-1", "user", nil, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendMessageTip("t1", "s1", "user-1"); err != nil {
		t.Fatal(err)
	}

	rt := &Runtime{deps: RuntimeDeps{DB: db}}
	sink := &persistSink{rt: rt, tenant: "t1", session: "s1", prevID: "user-1"}
	ctx := context.Background()

	emit := func(name string, params map[string]any) {
		t.Helper()
		if err := sink.Emit(ctx, Event{Name: name, Params: params}); err != nil {
			t.Fatalf("emit %s: %v", name, err)
		}
	}

	// The order the turn emits them in.
	emit("text-delta", map[string]any{"id": "s", "text": "running it", "message_id": "msg-1"})
	emit("tool-call", map[string]any{
		"toolCallId": "call_1", "toolName": "sandbox-job-start",
		"input": map[string]any{"command": "echo hi"}, "message_id": "msg-1",
	})
	emit("tool-result", map[string]any{
		"toolCallId": "call_1", "toolName": "sandbox-job-start",
		"formatted": "line1\nline2", "message_id": "msg-1",
	})
	emit("message-added", map[string]any{
		"message_id": "msg-1", "streaming": false, "role": "assistant",
	})

	parts, err := db.PartsByMessages("t1", []string{"msg-1"})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, p := range parts {
		kinds = append(kinds, p.Type)
	}

	var haveResult bool
	for _, p := range parts {
		if p.Type == "tool_result" {
			haveResult = true
			if !strings.Contains(p.Data, "line1") {
				t.Errorf("tool_result lost its content: %s", p.Data)
			}
		}
	}
	if !haveResult {
		t.Fatalf("stored parts = %v, want a tool_result among them", kinds)
	}
	// And the call itself must still be there (replacing, not clobbering).
	var haveTool bool
	for _, k := range kinds {
		if k == "tool" {
			haveTool = true
		}
	}
	if !haveTool {
		t.Fatalf("stored parts = %v, want the tool call preserved", kinds)
	}
}
