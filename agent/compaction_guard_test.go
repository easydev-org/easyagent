package agent

import (
	"fmt"
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/store"
)

// TestCompactBudgetsAreWindowFractions verifies the SINGLE rule: budgets are a
// fraction of the model window (20% / 10%), and a session whose model has NO
// configured window uses DefaultContextLimit (200k) — there is no fixed-budget
// fallback branch.
func TestCompactBudgetsAreWindowFractions(t *testing.T) {
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrate()
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"}) // no window configured

	rt := NewRuntime(RuntimeDeps{DB: db, Lease: NewMemoryLease()})
	tail, fold := rt.compactBudgets("t1", "s1")
	if tail != DefaultContextLimit*2/10 || fold != DefaultContextLimit/10 {
		t.Errorf("no-window budgets = %d/%d, want %d/%d",
			tail, fold, DefaultContextLimit*2/10, DefaultContextLimit/10)
	}

	// With an explicit window, the fraction scales with it.
	rt2 := NewRuntime(RuntimeDeps{DB: db, Lease: NewMemoryLease(), ContextLimit: 100_000})
	tail2, fold2 := rt2.compactBudgets("t1", "s1")
	if tail2 != 20_000 || fold2 != 10_000 {
		t.Errorf("100k-window budgets = %d/%d, want 20000/10000", tail2, fold2)
	}
}

// TestCheckpointShrinksRebuiltContext mirrors the TS #37 regression guard: a
// checkpoint must make the rebuilt context STRICTLY smaller than rebuilding the
// whole chain. If someone drops `tail_from` (so the folded prefix is rendered
// again), this fails.
func TestCheckpointShrinksRebuiltContext(t *testing.T) {
	const n = 40
	msgs := make([]store.Message, 0, n+1)
	parts := make([]store.Part, 0, n+1)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m%02d", i)
		msgs = append(msgs, store.Message{ID: id, Role: "user", Source: "user"})
		parts = append(parts, store.Part{
			MessageID: id, Type: "text",
			Data: `{"text":"` + strings.Repeat("x", 200) + `"}`,
		})
	}
	// A checkpoint folding everything before m30.
	msgs = append(msgs, store.Message{ID: "cm", Role: CompactionRole, Source: "system:compact"})
	parts = append(parts, store.Part{
		MessageID: "cm", Type: "summary",
		Data: `{"summary":"folded","tail_from":"m30"}`,
	})

	withCm := EstimateMessages(rebuildHistory(msgs, parts))
	// The SAME chain WITHOUT the checkpoint: the rebuild renders everything.
	withoutCm := EstimateMessages(rebuildHistory(msgs[:n], parts))

	if withCm >= withoutCm {
		t.Fatalf("checkpoint did not shrink the context: with=%d without=%d", withCm, withoutCm)
	}
	// The folded prefix (m00..m29) must be GONE: only the checkpoint block +
	// the verbatim tail (m30..m39) survive.
	out := rebuildHistory(msgs, parts)
	if len(out) != 11 {
		t.Fatalf("rebuilt messages = %d, want 11 (checkpoint + 10-message tail)", len(out))
	}
	if !strings.HasPrefix(out[0].Content, "<conversation-checkpoint>") {
		t.Errorf("first message is not the checkpoint block: %.40q", out[0].Content)
	}
	for _, m := range out[1:] {
		if strings.Contains(m.Content, strings.Repeat("x", 200)) == false {
			t.Errorf("tail message unexpectedly empty: %.40q", m.Content)
		}
	}
}
