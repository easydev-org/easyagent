package bus

import (
	"context"
	"os"
	"testing"
	"time"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

// The full path, with every component real: a subscription in the KV bucket, an index
// watching it, a distributor, and a channel event.
//
// This is deliberately an integration test. Every part of this path has already been
// individually correct while the whole delivered nothing — the pieces have to be shown
// working TOGETHER or the test proves nothing about the feature.
func TestChannelEndToEnd(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := nats.Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. The index, watching the same bucket a subscription is written to.
	idx := NewSubscriptionIndex(b)
	stopIdx, err := idx.Start(ctx)
	if err != nil {
		t.Fatalf("index start: %v", err)
	}
	defer stopIdx()

	// 2. A subscription, exactly as the store writes it.
	store := NewSubscriptionStore(b, idx)
	if _, err := store.Put(ctx, "alice", "s1", "gitea", "tag.agentdev/mrflow.>", true); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Wait for the KV watch to feed the index: this is real broker round-trip latency, and
	// assuming it is instant is how the earlier failures stayed invisible.
	deadline := time.Now().Add(5 * time.Second)
	for idx.Len("alice") == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the index never saw the subscription (Len=%d)", idx.Len("alice"))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 3. The distributor, publishing into a mailbox we can observe.
	type delivered struct{ session, typ string }
	got := make(chan delivered, 4)
	dist := NewDistributor(idx, func(_ context.Context, _, session, typ string, _ any, _ string) error {
		got <- delivered{session: session, typ: typ}
		return nil
	})

	// 4. The consumer, on the real channel subject.
	ch := sdkbus.NewChannels(b)
	stopCon, err := dist.Start(ctx, ch, "e2e-channels")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stopCon()
	time.Sleep(750 * time.Millisecond)

	if err := ch.PublishChannel(ctx, "alice", "gitea", "tag.agentdev/mrflow.v1.2.3",
		map[string]any{"tag": "v1.2.3"}, "gitea"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case d := <-got:
		if d.session != "s1" {
			t.Errorf("delivered to %q, want s1", d.session)
		}
		if d.typ != "trigger" {
			t.Errorf("wake=true should produce a trigger, got %q", d.typ)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no delivery (index Len=%d)", idx.Len("alice"))
	}
}

// The same path with wake=false: delivered, as an event, not a trigger.
func TestChannelEndToEndNonWaking(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := nats.Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx := NewSubscriptionIndex(b)
	stopIdx, err := idx.Start(ctx)
	if err != nil {
		t.Fatalf("index start: %v", err)
	}
	defer stopIdx()

	store := NewSubscriptionStore(b, idx)
	if _, err := store.Put(ctx, "alice", "s1", "group-chat", "eng.>", false); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for idx.Len("alice") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("subscription never reached the index")
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := make(chan string, 4)
	dist := NewDistributor(idx, func(_ context.Context, _, _, typ string, _ any, _ string) error {
		got <- typ
		return nil
	})
	ch := sdkbus.NewChannels(b)
	stopCon, err := dist.Start(ctx, ch, "e2e-chan2")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stopCon()
	time.Sleep(750 * time.Millisecond)

	if err := ch.PublishChannel(ctx, "alice", "group-chat", "eng.general", map[string]any{}, "chat"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case typ := <-got:
		if typ != "event" {
			t.Errorf("wake=false should produce an event, got %q", typ)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery")
	}
}

// The default subscription the agent writes must route session-channel traffic.
func TestDefaultSubscriptionEndToEnd(t *testing.T) {
	url := os.Getenv("EASYBUS_NATS_URL")
	if url == "" {
		s, err := natsrun.Start(natsrun.Config{Storage: natsrun.Memory})
		if err != nil {
			t.Skipf("no nats-server: %v", err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		url = s.URL()
	}
	b, err := nats.Connect(url)
	if err != nil {
		t.Skipf("nats unavailable: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx := NewSubscriptionIndex(b)
	stopIdx, err := idx.Start(ctx)
	if err != nil {
		t.Fatalf("index start: %v", err)
	}
	defer stopIdx()

	// What the agent writes when a session is created.
	if err := EnsureDefaultSubscription(ctx, b, "alice", "sess-a"); err != nil {
		t.Fatalf("default subscription: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for idx.Len("alice") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the default subscription never reached the index")
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := make(chan string, 4)
	dist := NewDistributor(idx, func(_ context.Context, _, session, typ string, _ any, _ string) error {
		got <- session + "/" + typ
		return nil
	})
	ch := sdkbus.NewChannels(b)
	stopCon, err := dist.Start(ctx, ch, "e2e-default")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	defer stopCon()
	time.Sleep(750 * time.Millisecond)

	// Anything reaching one session by name goes through this channel and topic.
	topic := DefaultChannelTopic("sess-a", "")
	if err := ch.PublishChannel(ctx, "alice", DefaultChannel, topic, map[string]any{"hello": "world"}, "user"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case d := <-got:
		if d != "sess-a/trigger" {
			t.Errorf("delivered %q, want sess-a/trigger", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no delivery (topic=%q, Len=%d)", topic, idx.Len("alice"))
	}
	_ = protocol.ChannelKVBucket
}
