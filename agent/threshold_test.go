package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// TestProactiveCompactionAtThreshold verifies that when the estimated context
// reaches the threshold fraction of the model window, the Go agent compacts
// BEFORE calling the provider (emitting a `compacted` event with reason
// "threshold"), rather than waiting for an overflow error.
func TestProactiveCompactionAtThreshold(t *testing.T) {
	var calls int32
	var sawCompactionMsg int32
	var promptLen int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		// Detect whether the request already carried a folded checkpoint.
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "conversation-checkpoint") {
			atomic.StoreInt32(&sawCompactionMsg, 1)
		}
		// Structural proof the folded prefix is dropped: the rebuilt prompt is
		// short (checkpoint + tail), NOT the full ~41-message chain.
		var req struct {
			Prompt []any `json:"prompt"`
		}
		if json.Unmarshal(body, &req) == nil {
			atomic.StoreInt32(&promptLen, int32(len(req.Prompt)))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"ok\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrate()
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"})

	// Seed a LONG chain so the estimate exceeds a tiny window.
	tip := ""
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("m%02d", i)
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		if _, err := db.InsertMessage("t1", id, role, strOrNil(tip), "user"); err != nil {
			t.Fatal(err)
		}
		_ = db.InsertPart("t1", id, "text", 0, map[string]any{"text": strings.Repeat("x", 400)})
		if _, err := db.AppendMessageTip("t1", "s1", id); err != nil {
			t.Fatal(err)
		}
		tip = id
	}

	rec := &recorder{}
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Emit: rec, Presets: stubPresets{},
		// A tiny window so 90% is crossed immediately.
		ContextLimit: 500, CompactAtFraction: 0.9,
	})
	rt.compactTailTokens = 50
	rt.compactFoldTokens = 5000
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	ev, ok := rec.first("compacted")
	if !ok {
		t.Fatal("expected a proactive `compacted` event before the provider call")
	}
	if asString(ev.Params["reason"]) != "threshold" {
		t.Errorf("compacted reason = %q, want threshold", ev.Params["reason"])
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Error("the provider should still have been called after compaction")
	}
	if atomic.LoadInt32(&sawCompactionMsg) == 0 {
		t.Error("the request sent to the provider should carry the folded checkpoint")
	}
	// The folded prefix is DROPPED: the prompt is the checkpoint + the small
	// verbatim tail, far fewer than the ~41-message chain.
	if pl := atomic.LoadInt32(&promptLen); pl == 0 || pl > 10 {
		t.Errorf("prompt length = %d, want a short (checkpoint + tail) prompt", pl)
	}
}

// TestNoProactiveCompactionWithoutLimit verifies the feature is OFF when no
// context limit is known (0).
func TestNoProactiveCompactionWithoutLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"ok\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrate()
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"})
	rec := &recorder{}
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Emit: rec, Presets: stubPresets{}, // no ContextLimit
	})
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.first("compacted"); ok {
		t.Error("no proactive compaction should happen without a context limit")
	}
}

// TestForceThresholdAndContinuation verifies:
//  1. the ABSOLUTE force threshold triggers compaction even with NO window limit
//     (reason "force"); and
//  2. the running turn CONTINUES after compaction and persists onto the NEW
//     chain — the assistant step's parent is the compaction checkpoint, not the
//     pre-compaction tip (no branch).
func TestForceThresholdAndContinuation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"after-compaction\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrate()
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"})

	// A long chain so there is something to fold.
	tip := ""
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("m%02d", i)
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		if _, err := db.InsertMessage("t1", id, role, strOrNil(tip), "user"); err != nil {
			t.Fatal(err)
		}
		_ = db.InsertPart("t1", id, "text", 0, map[string]any{"text": strings.Repeat("x", 300)})
		if _, err := db.AppendMessageTip("t1", "s1", id); err != nil {
			t.Fatal(err)
		}
		tip = id
	}

	rec := &recorder{}
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Emit: rec, Presets: stubPresets{},
		// No window limit; only the absolute force threshold applies.
		ForceCompactTokens: 100,
	})
	rt.compactTailTokens = 50
	rt.compactFoldTokens = 5000
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	ev, ok := rec.first("compacted")
	if !ok {
		t.Fatal("expected a forced compaction")
	}
	if asString(ev.Params["reason"]) != "force" {
		t.Errorf("compacted reason = %q, want force", ev.Params["reason"])
	}

	// The running turn must have CONTINUED and persisted onto the NEW chain:
	// the final tip is the assistant step, whose parent is the compaction
	// checkpoint.
	newTip, _ := db.SessionTip("t1", "s1")
	if newTip == tip {
		t.Fatal("the chain tip should have advanced past the pre-compaction tip")
	}
	msgs, err := db.Chain("t1", newTip, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	var assistant *store.Message
	for i := range msgs {
		if msgs[i].Role == "assistant" && msgs[i].ID != "" {
			assistant = &msgs[i]
		}
	}
	if assistant == nil {
		t.Fatal("no assistant step persisted after compaction")
	}
	parentID := ""
	if assistant.PrevID != nil {
		parentID = *assistant.PrevID
	}
	parentRole := ""
	for i := range msgs {
		if msgs[i].ID == parentID {
			parentRole = msgs[i].Role
		}
	}
	if parentRole != CompactionRole {
		t.Errorf("assistant step parent = %q (role %q), want the compaction checkpoint", parentID, parentRole)
	}
}
