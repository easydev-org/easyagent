package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
)

// mockV4 is a minimal AI SDK v4 gateway endpoint: it streams a fixed script.
func mockV4(t *testing.T, script ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, p := range script {
			fmt.Fprintf(w, "data: %s\n\n", p)
			if fl != nil {
				fl.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

func testStore(t *testing.T) *store.DB {
	t.Helper()
	d, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRuntimeWakeFoldsPromptAndPersistsStep(t *testing.T) {
	q := mailbox.NewMemory()
	db := testStore(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m", Preset: "default"}
	if err := db.CreateSession("t1", sess); err != nil {
		t.Fatal(err)
	}

	srv := mockV4(t,
		`{"type":"reasoning-delta","id":"r","delta":"think"}`,
		`{"type":"text-delta","id":"t","delta":"Hello "}`,
		`{"type":"text-delta","id":"t","delta":"world"}`,
		`{"type":"finish","finishReason":{"unified":"stop"},"usage":{"inputTokens":{"total":5},"outputTokens":{"total":2}}}`,
	)
	defer srv.Close()

	client := llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	rec := &recorder{}
	kv := NewMemoryKV()
	presets := NewPresets(kv)
	presets.SeedSystem("t1", PresetRow{ID: "default", SystemPrompt: "sys", IsSystem: true})
	rt := NewRuntime(RuntimeDeps{
		Mailbox: q,
		DB:      db, Client: client, Lease: NewMemoryLease(),
		Tools: &stubTools{}, Emit: rec, MaxTurns: 3,
		Presets: PresetStore{P: presets}, Config: ConfigStore{C: NewConfig(kv)},
	})

	// enqueue a user trigger
	if _, err := q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{
		"message_id": "u1", "text": "hi",
	}, "user"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}

	// chain: user(u1) -> assistant(step)
	tip, _ := db.SessionTip("t1", sess.ID)
	if tip == "" {
		t.Fatal("tip should be set")
	}
	chain, _ := db.Chain("t1", tip, 10, "")
	if len(chain) != 2 || chain[0].ID != "u1" || chain[0].Role != "user" {
		t.Fatalf("chain = %+v", chain)
	}
	if chain[1].Role != "assistant" {
		t.Errorf("second message role = %q", chain[1].Role)
	}
	// the assistant step persisted text + reasoning under the SAME id it streamed
	parts, _ := db.PartsByMessages("t1", []string{chain[1].ID})
	var text, reasoning string
	for _, p := range parts {
		switch p.Type {
		case "text":
			text += jsonField(p.Data, "text")
		case "reasoning":
			reasoning += jsonField(p.Data, "text")
		}
	}
	if text != "Hello world" {
		t.Errorf("persisted text = %q", text)
	}
	if reasoning != "think" {
		t.Errorf("persisted reasoning = %q", reasoning)
	}
	// message_seq advanced by 2 (user + assistant)
	s, _, _ := db.GetSession("t1", sess.ID)
	if s.MessageSeq != 2 {
		t.Errorf("message_seq = %d, want 2", s.MessageSeq)
	}
	if s.LastInputTokens != 5 || s.LastOutputTokens != 2 {
		t.Errorf("usage = %d/%d", s.LastInputTokens, s.LastOutputTokens)
	}
	// the durable event stream carried the user fold + the step lifecycle
	if !has(rec.names(), "message-added") || !has(rec.names(), "text-delta") {
		t.Errorf("events = %v", rec.names())
	}
}

func TestRuntimeSecondWakeIsIdempotentForFoldedPrompt(t *testing.T) {
	q := mailbox.NewMemory()
	db := testStore(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	srv := mockV4(t, `{"type":"text-delta","id":"t","delta":"ok"}`, `{"type":"finish","finishReason":{"unified":"stop"}}`)
	defer srv.Close()
	client := llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	rt := NewRuntime(RuntimeDeps{Mailbox: q, DB: db, Client: client, Lease: NewMemoryLease(), Tools: &stubTools{}})

	// Two triggers with the SAME message id must fold the user prompt once.
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	tip, _ := db.SessionTip("t1", sess.ID)
	chain, _ := db.Chain("t1", tip, 10, "")
	var users int
	for _, m := range chain {
		if m.Role == "user" {
			users++
		}
	}
	if users != 1 {
		t.Errorf("user messages = %d, want 1 (idempotent fold)", users)
	}
}

func TestRuntimeLeaseSerializes(t *testing.T) {
	q := mailbox.NewMemory()
	db := testStore(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	srv := mockV4(t, `{"type":"text-delta","id":"t","delta":"ok"}`, `{"type":"finish","finishReason":{"unified":"stop"}}`)
	defer srv.Close()
	client := llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	lease := NewMemoryLease()
	rt := NewRuntime(RuntimeDeps{Mailbox: q, DB: db, Client: client, Lease: lease, Tools: &stubTools{}})

	// Hold the lease, then Wake must be a no-op (no drain).
	if ok, _ := lease.Claim(context.Background(), "t1", sess.ID); !ok {
		t.Fatal("claim failed")
	}
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	pending := hasPending(t, q, "t1", sess.ID)
	if !pending {
		t.Error("a lease-held Wake must NOT drain the mailbox")
	}
	_ = lease.Release(context.Background(), "t1", sess.ID)
	// now it drains
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	if hasPending(t, q, "t1", sess.ID) {
		t.Error("mailbox should be drained after the lease is free")
	}
}

// jsonField reads a string field from a JSON object blob.
func jsonField(data, key string) string {
	var m map[string]any
	if json.Unmarshal([]byte(data), &m) != nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}
