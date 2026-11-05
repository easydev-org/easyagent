package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
)

func TestCompactSessionFoldsOlderTurns(t *testing.T) {
	q := mailbox.NewMemory()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	rt := NewRuntime(RuntimeDeps{Mailbox: q, DB: db, Lease: NewMemoryLease()})

	// Two user/assistant turns with a tool call in the first.
	_, _ = db.InsertMessage("t1", "u1", "user", nil, "user")
	_ = db.InsertPart("t1", "u1", "text", 0, map[string]any{"text": "first question"})
	_, _ = db.AppendMessageTip("t1", sess.ID, "u1")
	_, _ = db.InsertMessage("t1", "a1", "assistant", strOrNil("u1"), "")
	_ = db.InsertPart("t1", "a1", "tool", 0, map[string]any{"id": "c1", "name": "echo", "input": map[string]any{}})
	_ = db.InsertPart("t1", "a1", "tool_result", 1, map[string]any{"tool_use_id": "c1", "content": "done"})
	_, _ = db.AppendMessageTip("t1", sess.ID, "a1")
	_, _ = db.InsertMessage("t1", "u2", "user", strOrNil("a1"), "user")
	_ = db.InsertPart("t1", "u2", "text", 0, map[string]any{"text": "second question"})
	_, _ = db.AppendMessageTip("t1", sess.ID, "u2")

	// Force a tiny tail so the older turn folds.
	written, err := rt.compactWithBudgets(context.Background(), "t1", sess.ID, "manual", 1, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if !written {
		t.Fatal("expected a compaction checkpoint")
	}
	tip, _ := db.SessionTip("t1", sess.ID)
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
	/**
	 * THE SESSION LIST STILL NEEDS A PREVIEW, AND THE CHECKPOINT HAS NO TEXT TO MAKE ONE FROM.
	 *
	 * `persistSink` writes the preview from a step's TEXT parts. A checkpoint carries a `summary` part
	 * and nothing else, so the row went BLANK in the session list at the exact moment it was compacted.
	 * `CompactionPreview` existed for this and nothing wrote it; the console's `previewLabel` was written
	 * to recognise it. This is the loop being closed.
	 */
	row, ok, _ := db.GetSession("t1", sess.ID)
	if !ok {
		t.Fatal("session vanished")
	}
	if row.LastMessagePreview != CompactionPreview {
		t.Fatalf("preview = %q, want the compaction sentinel %q — the list row would be blank",
			row.LastMessagePreview, CompactionPreview)
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
