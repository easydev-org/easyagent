package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

func TestStatusBusyIdleAndPreview(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"the answer\"}\n\n")
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
		Lease: NewMemoryLease(), Emit: rec, Presets: stubPresets{},
	})
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	// B1: busy then idle, in order.
	var statuses []string
	for _, e := range rec.events {
		if e.Name == "status" {
			statuses = append(statuses, asString(e.Params["type"]))
		}
	}
	if len(statuses) < 2 || statuses[0] != "busy" || statuses[len(statuses)-1] != "idle" {
		t.Errorf("status events = %v, want busy...idle", statuses)
	}
	// B4: preview + turn-end reason recorded.
	row, _, _ := db.GetSession("t1", "s1")
	if row.LastMessagePreview != "the answer" {
		t.Errorf("preview = %q", row.LastMessagePreview)
	}
	if row.LastTurnReason != "stop" || row.LastTurnAt == nil {
		t.Errorf("turn end = %q / %v", row.LastTurnReason, row.LastTurnAt)
	}
}
