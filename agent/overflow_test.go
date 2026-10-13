package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

func TestTurnRecoversFromContextOverflow(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		if atomic.AddInt32(&n, 1) == 1 {
			// First call: a context-overflow error part.
			fmt.Fprint(w, "data: {\"type\":\"error\",\"error\":{\"message\":\"This model's maximum context length is 8192 tokens\"}}\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
			if fl != nil {
				fl.Flush()
			}
			return
		}
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"recovered\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		if fl != nil {
			fl.Flush()
		}
	}))
	defer srv.Close()

	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Migrate()
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m"})
	// seed a couple of turns so compaction has something to fold
	_, _ = db.InsertMessage("t1", "u1", "user", nil, "user")
	_ = db.InsertPart("t1", "u1", "text", 0, map[string]any{"text": "q1"})
	_, _ = db.AppendMessageTip("t1", "s1", "u1")
	_, _ = db.InsertMessage("t1", "a1", "assistant", strOrNil("u1"), "")
	_ = db.InsertPart("t1", "a1", "text", 0, map[string]any{"text": "a1"})
	_, _ = db.AppendMessageTip("t1", "s1", "a1")
	_, _ = db.InsertMessage("t1", "u2", "user", strOrNil("a1"), "user")
	_ = db.InsertPart("t1", "u2", "text", 0, map[string]any{"text": "q2"})
	_, _ = db.AppendMessageTip("t1", "s1", "u2")

	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Emit: &recorder{}, MaxTurns: 3,
	})
	// Force compaction to actually fold by shrinking the tail budget.
	rt.compactTailTokens = 1

	rec := &recorder{}
	rt.deps.Emit = rec
	// enqueue a trigger and run
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u3", "text": "go"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	if !has(rec.names(), "compacted") {
		t.Errorf("expected a `compacted` event; events=%v", rec.names())
	}
	// The retried step produced text.
	var text string
	for _, e := range rec.events {
		if e.Name == "text-delta" {
			text += asString(e.Params["text"])
		}
	}
	if text != "recovered" {
		t.Errorf("text = %q, want recovered", text)
	}
}
