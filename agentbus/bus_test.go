package agentbus

import (
	"context"
	"testing"
	"time"

	abcbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/natsrun"
)

// startBus boots an embedded nats-server and connects a Bus. Skips when the
// nats-server binary is unavailable (set EASYBUS_NATS_SERVER_BIN).
func startBus(t *testing.T) *Bus {
	t.Helper()
	srv, err := natsrun.Start(natsrun.Config{Port: 0})
	if err != nil {
		t.Skipf("nats-server unavailable: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	b, err := Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestLeaseMutualExclusion(t *testing.T) {
	b1 := startBus(t)
	// A second connection to the SAME broker.
	b2, err := Connect(b1.URL())
	if err != nil {
		t.Skipf("second connect: %v", err)
	}
	t.Cleanup(func() { _ = b2.Close() })

	ctx := context.Background()
	ok1, err := b1.Claim(ctx, "t1", "s1")
	if err != nil || !ok1 {
		t.Fatalf("b1 claim = %v, %v", ok1, err)
	}
	ok2, err := b2.Claim(ctx, "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Error("b2 must NOT be able to claim a held lease")
	}
	// renew succeeds while held
	if held, _ := b1.Renew(ctx, "t1", "s1"); !held {
		t.Error("renew should hold")
	}
	// release frees it
	if err := b1.Release(ctx, "t1", "s1"); err != nil {
		t.Fatal(err)
	}
	ok2, _ = b2.Claim(ctx, "t1", "s1")
	if !ok2 {
		t.Error("b2 should claim after release")
	}
}

func TestMailboxPublishConsume(t *testing.T) {
	// The queue PublishMailbox writes to before signalling. PublishMailbox refuses to run without
	// one, deliberately: a signal-only publish looks like success from every angle except the
	// session waking up.
	b := startBus(t).WithMailboxQueue(newMemQueue())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// THE SIGNAL IS A WAKE ON ITS OWN SUBJECT. `PublishMailbox` no longer republishes the body on
	// `mailbox.<session>`; that path belongs to extensions announcing, which the bridge reads. This
	// test asserted the old shared-subject behaviour and could only ever pass where it was skipped.
	got := make(chan string, 1)
	stop, err := b.ConsumeWake(ctx, func(tenant, session string) error {
		select {
		case got <- tenant + "/" + session:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// give the consumer a moment to bind
	time.Sleep(300 * time.Millisecond)

	if err := b.PublishMailbox(ctx, "t1", "s1", "trigger", map[string]any{"text": "hi"}, "user"); err != nil {
		t.Fatal(err)
	}
	select {
	case who := <-got:
		if who != "t1/s1" {
			t.Errorf("woke %q, want t1/s1", who)
		}
	case <-ctx.Done():
		t.Fatal("did not receive the wake")
	}
}

func TestSessionEventsPublishSubscribe(t *testing.T) {
	b := startBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch, stop, err := b.SubscribeSessionEvents(ctx, "t1", "s1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	time.Sleep(300 * time.Millisecond) // let the subscription bind

	if err := b.PublishSessionEvent(ctx, "t1", "s1", "e1", map[string]any{
		"event": "text-delta", "params": map[string]any{"text": "hi"},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case raw := <-ch:
		if len(raw) == 0 {
			t.Error("empty frame")
		}
	case <-ctx.Done():
		t.Fatal("did not receive the session event")
	}
}

func TestNATSKVRoundTrip(t *testing.T) {
	b := startBus(t)
	kv := NewNATSKV(b, "agent-go-test-presets")

	if _, ok := kv.Get("t.t1.default"); ok {
		t.Fatal("a fresh bucket should not have the key")
	}
	kv.Put("t.t1.default", `{"id":"default","system_prompt":"hi"}`)
	kv.Put("t.t1.u.custom", `{"id":"custom"}`)
	kv.Put("t.t2.default", `{"id":"default"}`)

	if v, ok := kv.Get("t.t1.default"); !ok || v == "" {
		t.Fatalf("get after put = %q, %v", v, ok)
	}
	// Keys lists by prefix (JetStream KV has no native listing).
	keys := kv.Keys("t.t1.")
	if len(keys) != 2 {
		t.Fatalf("keys(t.t1.) = %v", keys)
	}
	// Delete removes.
	kv.Delete("t.t1.default")
	if _, ok := kv.Get("t.t1.default"); ok {
		t.Error("key should be gone after delete")
	}
}

func TestLifecyclePublishConsume(t *testing.T) {
	b := startBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify our publish path lands on the right subject per kind.
	sub, err := b.Transport().Subscribe(ctx, "easybus.*.session.lifecycle.>", abcbus.SubscribeOpts{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	time.Sleep(300 * time.Millisecond)

	if err := b.PublishLifecycle(ctx, "t1", "created", "s1", map[string]any{"kind": "created"}); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishLifecycle(ctx, "t1", "deleted", "s1", map[string]any{"kind": "deleted"}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		env, ok := sub.Next(ctx)
		if !ok {
			t.Fatalf("subscription closed (seen=%v)", seen)
		}
		seen[env.Ch] = true
	}
	if !seen["easybus.t1.session.lifecycle.created"] || !seen["easybus.t1.session.lifecycle.deleted"] {
		t.Errorf("lifecycle subjects = %v", seen)
	}
}

/**
 * A LIST CHANGE PUBLISHED ON ONE REPLICA REACHES A SUBSCRIBER ON ANOTHER.
 *
 * This is the cross-replica contract, exercised on the bus alone: two subscribers, one publish, both see it. The
 * server-level test cannot show this because it would need two processes — what it CAN show is that the server
 * publishes at all, and this shows the transport carries it.
 */
func TestListChangeCrossesReplicas(t *testing.T) {
	b := startBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two subscribers standing in for two replicas' watchers.
	a, stopA, err := b.SubscribeListChanges(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	defer stopA()
	c, stopC, err := b.SubscribeListChanges(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	defer stopC()

	if err := b.PublishListChange(ctx, "t1", SessionListChange{
		Kind: ListKindSession, Name: "sess-1",
	}); err != nil {
		t.Fatal(err)
	}

	for name, ch := range map[string]<-chan SessionListChange{"first": a, "second": c} {
		select {
		case got := <-ch:
			if got.Kind != ListKindSession || got.Name != "sess-1" {
				t.Errorf("%s subscriber got %+v", name, got)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("%s subscriber received nothing", name)
		}
	}
}

// A CHANGE FOR ANOTHER TENANT DOES NOT ARRIVE.
func TestListChangeIsTenantScoped(t *testing.T) {
	b := startBus(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, stop, err := b.SubscribeListChanges(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if err := b.PublishListChange(ctx, "t2", SessionListChange{Kind: ListKindGroup, Name: "g"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		t.Fatalf("a change for another tenant arrived: %+v", got)
	case <-time.After(300 * time.Millisecond):
		// expected
	}
}
