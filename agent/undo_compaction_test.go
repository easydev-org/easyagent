package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/store"
)

// seedCompacted builds u1/a1, compacts (so the tip becomes a compaction
// checkpoint with tail_from = u2), then appends u2/a2 after it.
//
//	u1 -> a1 -> [compaction(tail_from=u2)] -> u2 -> a2
func seedCompacted(t *testing.T) (*store.DB, *Runtime) {
	t.Helper()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m", Preset: "default"}); err != nil {
		t.Fatal(err)
	}
	rt := NewRuntime(RuntimeDeps{DB: db, Lease: NewMemoryLease(), Presets: stubPresets{}})

	// u1 / a1 (the part that will be folded).
	_, _ = db.InsertMessage("t1", "u1", "user", nil, "user")
	_ = db.InsertPart("t1", "u1", "text", 0, map[string]any{"text": "first question"})
	_, _ = db.AppendMessageTip("t1", "s1", "u1")
	_, _ = db.InsertMessage("t1", "a1", "assistant", strOrNil("u1"), "")
	_ = db.InsertPart("t1", "a1", "text", 0, map[string]any{"text": "first answer"})
	_, _ = db.AppendMessageTip("t1", "s1", "a1")

	// Compact with a tiny tail so u1/a1 fold and the checkpoint's tail_from
	// points at the next message (u2).
	_, _ = db.InsertMessage("t1", "u2", "user", strOrNil("a1"), "user")
	_ = db.InsertPart("t1", "u2", "text", 0, map[string]any{"text": "second question"})
	_, _ = db.AppendMessageTip("t1", "s1", "u2")
	if _, err := rt.compactWithBudgets(context.Background(), "t1", "s1", "manual", 1, 100000); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// After compaction the tip is the checkpoint; append a fresh turn on top.
	_, _ = db.InsertMessage("t1", "a2", "assistant", strOrNil(currentTip(t, db)), "")
	_ = db.InsertPart("t1", "a2", "text", 0, map[string]any{"text": "second answer"})
	_, _ = db.AppendMessageTip("t1", "s1", "a2")
	return db, rt
}

func currentTip(t *testing.T, db *store.DB) string {
	t.Helper()
	tip, err := db.SessionTip("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	return tip
}

// rebuild is what the model would see.
func rebuild(t *testing.T, db *store.DB) []string {
	t.Helper()
	tip := currentTip(t, db)
	msgs, err := db.Chain("t1", tip, 0, "")
	if err != nil {
		t.Fatalf("chain: %v", err)
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	parts, _ := db.PartsByMessages("t1", ids)
	out := rebuildHistory(msgs, parts)
	texts := make([]string, len(out))
	for i, m := range out {
		texts[i] = m.Content
	}
	return texts
}

// TestUndoRecentMessage_KeepsCheckpoint: undo ONLY the message after the
// checkpoint (tip -> checkpoint). The checkpoint is still the tip, so the
// folded prefix must stay folded.
func TestUndoRecentMessage_KeepsCheckpoint(t *testing.T) {
	db, _ := seedCompacted(t)

	// Undo a2 by moving the tip back to its predecessor, the checkpoint.
	tip := currentTip(t, db)
	m, ok, _ := db.GetMessage("t1", tip)
	if !ok || m.Role != "assistant" {
		t.Fatalf("expected a2 as tip, got %+v", m)
	}
	if _, err := db.SetTip("t1", "s1", m.PrevID); err != nil {
		t.Fatal(err)
	}
	texts := rebuild(t, db)
	joined := joinAll(texts)
	if !contains(joined, "conversation-checkpoint") {
		t.Errorf("checkpoint lost after undoing only the post-checkpoint reply:\n%s", joined)
	}
	// The folded turn must NOT come back as its own message: it may only appear
	// inside the checkpoint summary. Rebuild yields one llm.Message per chain
	// row, so count the rows whose content is exactly the original text.
	for _, x := range texts {
		if x == "first question" || x == "first answer" {
			t.Errorf("folded message leaked back as its own context message: %q\n%s", x, joined)
		}
	}
	if !contains(joined, "second question") {
		t.Errorf("tail message missing:\n%s", joined)
	}
}

// TestUndoToTailBoundary_RollsBackCompaction pins the intended UX: undoing a
// message that sits AT or BEFORE the checkpoint's tail boundary withdraws the
// checkpoint too, so the conversation is restored to its PRE-compaction shape
// (the folded prefix becomes visible again). Compression is not sticky.
func TestUndoToTailBoundary_RollsBackCompaction(t *testing.T) {
	db, _ := seedCompacted(t)

	// Undo u2 (the tail boundary) — the checkpoint sits after u2, so it goes too.
	if _, err := db.SetTip("t1", "s1", strOrNil("u2")); err != nil {
		t.Fatal(err)
	}
	texts := rebuild(t, db)
	joined := joinAll(texts)
	if contains(joined, "conversation-checkpoint") {
		t.Errorf("withdrawing past the tail boundary must drop the checkpoint:\n%s", joined)
	}
	// After the rollback the pre-compaction turns are ordinary messages again.
	foundQ, foundA := false, false
	for _, x := range texts {
		if x == "first question" {
			foundQ = true
		}
		if x == "first answer" {
			foundA = true
		}
	}
	if !foundQ || !foundA {
		t.Errorf("pre-compaction messages must be restored as messages (q=%v a=%v):\n%s", foundQ, foundA, joined)
	}
	if !contains(joined, "second question") {
		t.Errorf("tail message missing:\n%s", joined)
	}
}

// TestUndoIntoCompaction_UndoTailBoundary: undo exactly to `tail_from` (u2). The
// checkpoint preceded it, so the folded prefix must still be folded.
func TestUndoIntoCompaction_UndoTailBoundary(t *testing.T) {
	db, _ := seedCompacted(t)

	if _, err := db.SetTip("t1", "s1", strOrNil("a1")); err != nil { // undo u2 + a2
		t.Fatal(err)
	}
	texts := rebuild(t, db)
	joined := joinAll(texts)
	// a1 is BEFORE the checkpoint: the checkpoint no longer applies (we cut
	// behind it), so the prefix must be visible again.
	if !contains(joined, "first question") {
		t.Errorf("undoing behind the checkpoint must restore the folded prefix; got:\n%s", joined)
	}
}

// TestUndoTheCompactionItself: undo the checkpoint row itself. Whatever the
// chain then looks like, the rebuild must not panic and must not reference a
// checkpoint whose summary is gone.
func TestUndoTheCompactionItself(t *testing.T) {
	db, _ := seedCompacted(t)

	// Find the checkpoint id.
	tip := currentTip(t, db)
	msgs, _ := db.Chain("t1", tip, 0, "")
	var cpID, cpPrev string
	for _, m := range msgs {
		if m.Role == CompactionRole {
			cpID = m.ID
			if m.PrevID != nil {
				cpPrev = *m.PrevID
			}
		}
	}
	if cpID == "" {
		t.Fatal("no checkpoint found")
	}
	if _, err := db.SetTip("t1", "s1", strOrNil(cpPrev)); err != nil {
		t.Fatal(err)
	}
	texts := rebuild(t, db) // must not panic
	joined := joinAll(texts)
	if contains(joined, "conversation-checkpoint") {
		t.Errorf("a withdrawn checkpoint must not appear in context:\n%s", joined)
	}
	foundQ := false
	for _, x := range texts {
		if x == "first question" {
			foundQ = true
		}
	}
	if !foundQ {
		t.Errorf("after withdrawing the checkpoint the original messages must be back:\n%s", joined)
	}
}

func joinAll(s []string) string {
	out := ""
	for _, x := range s {
		out += x + "\n---\n"
	}
	return out
}

func contains(h, sub string) bool {
	return len(sub) == 0 || (len(h) >= len(sub) && indexOf(h, sub) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
