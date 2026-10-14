package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// slowSSE blocks until the request context is cancelled, then closes (so a
// cancelled turn returns). It signals when the request has ARRIVED so the test
// can cancel deterministically.
func slowSSE(t *testing.T) (*httptest.Server, *sync.WaitGroup) {
	t.Helper()
	arrived := &sync.WaitGroup{}
	arrived.Add(1)
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { arrived.Done() })
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		if fl != nil {
			fl.Flush()
		}
		<-r.Context().Done() // park until the client (turn) is cancelled
	}))
	t.Cleanup(srv.Close)
	return srv, arrived
}

func newCancelRuntime(t *testing.T, srvURL string) (*Runtime, *store.DB) {
	t.Helper()
	db := testStore(t)
	if err := db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m", Preset: "default"}); err != nil {
		t.Fatal(err)
	}
	kv := NewMemoryKV()
	presets := NewPresets(kv)
	presets.SeedSystem("t1", PresetRow{ID: "default", SystemPrompt: "sys", IsSystem: true})
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srvURL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Tools: &stubTools{}, Emit: &recorder{}, MaxTurns: 3,
		Presets: PresetStore{P: presets}, Config: ConfigStore{C: NewConfig(kv)},
	})
	return rt, db
}

// An interrupt during a turn records last_turn_reason = "interrupted".
func TestInterruptRecordsReason(t *testing.T) {
	srv, arrived := slowSSE(t)
	rt, db := newCancelRuntime(t, srv.URL)

	if _, err := db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = rt.Wake(context.Background(), "t1", "s1") }()
	// Wait until the turn is streaming, then interrupt it.
	waitOrFail(t, arrived, "turn did not start")
	if !rt.Interrupt("t1", "s1") {
		t.Fatal("Interrupt found no active run")
	}
	waitReason(t, db, "t1", "s1", "interrupted")
}

// A lost lease records last_turn_reason = "locklost".
func TestLeaseLostRecordsReason(t *testing.T) {
	srv, arrived := slowSSE(t)
	rt, db := newCancelRuntime(t, srv.URL)
	// A lease that reports LOST on the first renew, then stays lost.
	rt.deps.Lease = &loseOnRenewLease{}
	rt.deps.LeaseTTL = 30 * time.Millisecond // renew at TTL/3 = 10ms

	if _, err := db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = rt.Wake(context.Background(), "t1", "s1") }()
	waitOrFail(t, arrived, "turn did not start")
	waitReason(t, db, "t1", "s1", "locklost")
}

// A normal turn still records "stop" (regression guard).
func TestNormalTurnRecordsStop(t *testing.T) {
	db := testStore(t)
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m", Preset: "default"})
	srv := mockV4(t,
		`{"type":"text-delta","id":"t","delta":"ok"}`,
		`{"type":"finish","finishReason":{"unified":"stop"}}`,
	)
	defer srv.Close()
	kv := NewMemoryKV()
	presets := NewPresets(kv)
	presets.SeedSystem("t1", PresetRow{ID: "default", SystemPrompt: "sys", IsSystem: true})
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Tools: &stubTools{}, Emit: &recorder{}, MaxTurns: 3,
		Presets: PresetStore{P: presets}, Config: ConfigStore{C: NewConfig(kv)},
	})
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	waitReason(t, db, "t1", "s1", "stop")
}

// A truncated generation (provider finishReason "length") records "length",
// not "stop".
func TestLengthFinishRecordsReason(t *testing.T) {
	db := testStore(t)
	_ = db.CreateSession("t1", store.SessionInput{Name: "s1", Model: "gw/m", Preset: "default"})
	srv := mockV4(t,
		`{"type":"text-delta","id":"t","delta":"half a thou"}`,
		`{"type":"finish","finishReason":{"unified":"length"}}`,
	)
	defer srv.Close()
	kv := NewMemoryKV()
	presets := NewPresets(kv)
	presets.SeedSystem("t1", PresetRow{ID: "default", SystemPrompt: "sys", IsSystem: true})
	rt := NewRuntime(RuntimeDeps{
		DB: db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Tools: &stubTools{}, Emit: &recorder{}, MaxTurns: 3,
		Presets: PresetStore{P: presets}, Config: ConfigStore{C: NewConfig(kv)},
	})
	_, _ = db.Enqueue("t1", "s1", "trigger", map[string]any{"message_id": "u1", "text": "hi"}, "user")
	if err := rt.Wake(context.Background(), "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	waitReason(t, db, "t1", "s1", "length")
}

// An error is classified: a retryable transport failure -> "retryable", a
// permanent failure -> "error".
func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"retryable 5xx", &llm.APIError{StatusCode: 503, Message: "unavailable"}, "retryable"},
		{"retryable transport", &llm.APIError{Message: "connection reset by peer"}, "retryable"},
		{"permanent 400", &llm.APIError{StatusCode: 400, Message: "bad request"}, "error"},
		{"permanent 401", &llm.APIError{StatusCode: 401, Message: "unauthorized"}, "error"},
	}
	for _, c := range cases {
		got := classifyTurnError(c.err)
		if got != c.want {
			t.Errorf("%s: classify(%v) = %q, want %q", c.name, c.err, got, c.want)
		}
	}
}

// loseOnRenewLease claims successfully but reports the lock LOST on renew.
type loseOnRenewLease struct{}

func (l *loseOnRenewLease) Claim(context.Context, string, string) (bool, error) { return true, nil }
func (l *loseOnRenewLease) Renew(context.Context, string, string) (bool, error) { return false, nil }
func (l *loseOnRenewLease) Release(context.Context, string, string) error       { return nil }

func waitOrFail(t *testing.T, wg *sync.WaitGroup, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(msg)
	}
}

func waitReason(t *testing.T, db *store.DB, tenant, session, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok, _ := db.GetSession(tenant, session); ok && s.LastTurnReason == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s, _, _ := db.GetSession(tenant, session)
	t.Fatalf("last_turn_reason = %q, want %q", s.LastTurnReason, want)
}
