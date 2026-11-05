package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
)

// slowV4 streams a text delta then blocks until the request context is
// cancelled (a long turn).
func slowV4(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"partial\"}\n\n")
		if fl != nil {
			fl.Flush()
		}
		<-r.Context().Done()
	}))
}

// fakeLease lets a test drive renew outcomes.
type fakeLease struct {
	held   atomic.Bool
	lostAt atomic.Int32 // renew calls before reporting loss (-1 = never)
	renews atomic.Int32
}

func (f *fakeLease) Claim(_ context.Context, _, _ string) (bool, error) {
	return f.held.CompareAndSwap(false, true), nil
}
func (f *fakeLease) Renew(_ context.Context, _, _ string) (bool, error) {
	n := f.renews.Add(1)
	if f.lostAt.Load() >= 0 && n > f.lostAt.Load() {
		return false, nil // lost
	}
	return f.held.Load(), nil
}
func (f *fakeLease) Release(_ context.Context, _, _ string) error        { f.held.Store(false); return nil }
func (f *fakeLease) IsHeld(_ context.Context, _, _ string) (bool, error) { return f.held.Load(), nil }

func newRuntimeDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLeaseLossAbortsTurn(t *testing.T) {
	q := mailbox.NewMemory()
	srv := slowV4(t)
	defer srv.Close()
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	lease := &fakeLease{}
	lease.lostAt.Store(0) // lose on the FIRST renew
	rt := NewRuntime(RuntimeDeps{
		Mailbox: q,
		DB:      db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: lease, LeaseTTL: 30 * time.Millisecond, Emit: &recorder{},
		Presets: stubPresets{},
	})
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	start := time.Now()
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	// The turn must abort promptly (renew fires at TTL/3 ≈ 10ms), not hang.
	if time.Since(start) > 3*time.Second {
		t.Errorf("turn did not abort on lease loss (took %v)", time.Since(start))
	}
	// A lost lease must NOT be released (another replica owns it).
	if !lease.held.Load() {
		t.Error("a lost lease must not be released by the old holder")
	}
}

func TestCrossReplicaInterruptAbortsTurn(t *testing.T) {
	q := mailbox.NewMemory()
	srv := slowV4(t)
	defer srv.Close()
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	// The interrupt watcher delivers an `interrupt` shortly after the run starts.
	watch := func(ctx context.Context, _, _ string) (<-chan string, func(), error) {
		ch := make(chan string, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			select {
			case ch <- "interrupt":
			case <-ctx.Done():
			}
		}()
		return ch, func() {}, nil
	}
	rt := NewRuntime(RuntimeDeps{
		Mailbox: q,
		DB:      db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), InterruptWatch: watch, Emit: &recorder{},
		Presets: stubPresets{},
	})
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	start := time.Now()
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("cross-replica interrupt did not abort the turn (took %v)", time.Since(start))
	}
}

func TestDrainLoopChainsWithoutReleasingLease(t *testing.T) {
	q := mailbox.NewMemory()
	// Each turn appends a text step; a second trigger enqueued DURING the first
	// turn must be processed in the SAME busy period (one claim, one release).
	var n int32
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if atomic.AddInt32(&n, 1) == 1 {
			// On the first call, enqueue a second trigger (simulating a prompt
			// that lands mid-turn). It must be picked up by the drain loop.
			_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u2", "text": "more"}, "user")
		}
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"ok\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	lease := NewMemoryLease()
	rt := NewRuntime(RuntimeDeps{
		Mailbox: q,
		DB:      db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: lease, Emit: &recorder{}, MaxTurns: 2, Presets: stubPresets{},
	})
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "go"}, "user")
	if err := rt.Wake(context.Background(), "t1", sess.ID); err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Errorf("drain loop did not process the mid-turn trigger (calls=%d)", n)
	}
	if hasPending(t, q, "t1", sess.ID) {
		t.Error("mailbox should be drained at the end of the busy period")
	}
}

func TestRecoverOnBootWakesPending(t *testing.T) {
	q := mailbox.NewMemory()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"ok\"}\n\n")
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"stop\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	_ = db.CreateSession("t1", sess)
	_, _ = q.Enqueue(context.Background(), "t1", sess.ID, "", "trigger", map[string]any{"message_id": "u1", "text": "left over"}, "user")
	rt := NewRuntime(RuntimeDeps{
		Mailbox: q,
		DB:      db, Client: llm.New(llm.ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil),
		Lease: NewMemoryLease(), Emit: &recorder{}, Presets: stubPresets{},
	})
	rt.RecoverOnBoot(context.Background())
	if hasPending(t, q, "t1", sess.ID) {
		t.Error("boot recovery should have drained the pending wake")
	}
}
