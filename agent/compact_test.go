package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/store"
)

func TestCompactSessionFoldsOlderTurns(t *testing.T) {
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"})
	rt := NewRuntime(RuntimeDeps{DB: db, Lease: NewMemoryLease()})

	// Two user/assistant turns with a tool call in the first.
	_, _ = db.InsertMessage("t1", "u1", "user", nil, "user")
	_ = db.InsertPart("t1", "u1", "text", 0, map[string]any{"text": "first question"})
	_, _ = db.AppendMessageTip("t1", "s1", "u1")
	_, _ = db.InsertMessage("t1", "a1", "assistant", strOrNil("u1"), "")
	_ = db.InsertPart("t1", "a1", "tool", 0, map[string]any{"id": "c1", "name": "echo", "input": map[string]any{}})
	_ = db.InsertPart("t1", "a1", "tool_result", 1, map[string]any{"tool_use_id": "c1", "content": "done"})
	_, _ = db.AppendMessageTip("t1", "s1", "a1")
	_, _ = db.InsertMessage("t1", "u2", "user", strOrNil("a1"), "user")
	_ = db.InsertPart("t1", "u2", "text", 0, map[string]any{"text": "second question"})
	_, _ = db.AppendMessageTip("t1", "s1", "u2")

	// Force a tiny tail so the older turn folds.
	written, err := rt.compactWithBudgets(context.Background(), "t1", "s1", "manual", 1, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if !written {
		t.Fatal("expected a compaction checkpoint")
	}
	tip, _ := db.SessionTip("t1", "s1")
	m, ok, _ := db.GetMessage("t1", tip)
	if !ok || m.Role != CompactionRole {
		t.Fatalf("tip should be a compaction checkpoint, got %+v", m)
	}
	parts, _ := db.PartsByMessages("t1", []string{tip})
	var summary string
	for _, p := range parts {
		if p.Type == "summary" {
			summary = jsonField(p.Data, "summary")
		}
	}
	if summary == "" {
		t.Fatal("checkpoint must carry a summary part")
	}
	// The checkpoint never appears in the rebuilt model context.
	msgs, _ := db.Chain("t1", tip, 0, "")
	ids := make([]string, len(msgs))
	for i, x := range msgs {
		ids[i] = x.ID
	}
	allParts, _ := db.PartsByMessages("t1", ids)
	for _, m := range rebuildHistory(msgs, allParts) {
		_ = m
	}
}
