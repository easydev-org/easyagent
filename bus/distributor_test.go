package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
)

// The distributor is the seam where a channel event becomes a mailbox message, so the
// things worth pinning are: who receives it, how many times, and whether it wakes.

type delivery struct {
	tenant, session, typ string
	payload              any
	source               string
}

func newTestDistributor(t *testing.T, subs func(*SubscriptionIndex)) (*Distributor, *[]delivery) {
	t.Helper()
	idx := NewSubscriptionIndex(nil)
	subs(idx)
	var got []delivery
	d := NewDistributor(idx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
		got = append(got, delivery{tenant: tenant, session: session, typ: typ, payload: payload, source: source})
		return nil
	})
	return d, &got
}

func TestDistributeToOneSession(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-1", "gitea", "tag.agentdev/mrflow.>", true)
	})
	n, err := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.agentdev/mrflow.v1.2.3",
		Payload: map[string]any{"tag": "v1.2.3"}, Source: "gitea",
	})
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(*got) != 1 || (*got)[0].session != "s1" {
		t.Fatalf("deliveries = %+v", *got)
	}
	// Wake=true becomes a TRIGGER: the whole point of a channel subscription is that the
	// session reacts.
	if (*got)[0].typ != "trigger" {
		t.Errorf("wake=true should produce a trigger, got %q", (*got)[0].typ)
	}
	if (*got)[0].source != "channel:gitea" {
		t.Errorf("source = %q, want channel:gitea", (*got)[0].source)
	}
}

// Wake=false must still deliver, just without starting a turn.
func TestDistributeNonWakingIsEvent(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-1", "gitea", "tag.>", false)
	})
	_, _ = d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.v1",
	})
	if len(*got) != 1 || (*got)[0].typ != "event" {
		t.Fatalf("wake=false should produce an event, got %+v", *got)
	}
}

// A session following the same topic twice must see the event ONCE. Two deliveries would
// mean the session processes the same event twice — for a wake-up that is two turns.
func TestDistributeOncePerSession(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-a", "gitea", "tag.>", true)
		put(t, idx, "alice", "s1", "sub-b", "gitea", "tag.agentdev/mrflow.>", true)
	})
	n, _ := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.agentdev/mrflow.v1",
	})
	if n != 1 || len(*got) != 1 {
		t.Fatalf("expected exactly one delivery for one session, got n=%d deliveries=%+v", n, *got)
	}
}

// If ONE of a session's matching subscriptions wakes, the session wakes. A session has a
// single mailbox, so "half awake" is not a state it can be in.
func TestDistributeWakesIfAnySubscriptionDoes(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-a", "gitea", "tag.>", false)
		put(t, idx, "alice", "s1", "sub-b", "gitea", "tag.agentdev/mrflow.>", true)
	})
	_, _ = d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.agentdev/mrflow.v1",
	})
	if len(*got) != 1 || (*got)[0].typ != "trigger" {
		t.Fatalf("one waking subscription should wake the session, got %+v", *got)
	}
}

func TestDistributeFansOut(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-1", "group-chat", "eng.>", true)
		put(t, idx, "alice", "s2", "sub-1", "group-chat", "eng.>", true)
	})
	n, _ := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "group-chat", Topic: "eng.general",
	})
	if n != 2 || len(*got) != 2 {
		t.Fatalf("expected two deliveries, got n=%d %+v", n, *got)
	}
}

// A message nobody follows is normal, not an error: channels are broadcast, and a channel
// with no listeners is a quiet channel, not a failure.
func TestDistributeNobodyListening(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {})
	n, err := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.v1",
	})
	if err != nil || n != 0 || len(*got) != 0 {
		t.Fatalf("n=%d err=%v deliveries=%+v", n, err, *got)
	}
}

// A message must never be delivered across tenants, even when the channel and topic match.
func TestDistributeNeverCrossesTenants(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "bob", "s-bob", "sub-1", "gitea", "tag.>", true)
	})
	n, _ := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.v1",
	})
	if n != 0 || len(*got) != 0 {
		t.Fatalf("alice's message reached bob's session: n=%d %+v", n, *got)
	}
}

// One session's failed delivery must not stop the others.
func TestDistributeContinuesAfterFailure(t *testing.T) {
	idx := NewSubscriptionIndex(nil)
	put(t, idx, "alice", "s1", "sub-1", "group-chat", "eng.>", true)
	put(t, idx, "alice", "s2", "sub-1", "group-chat", "eng.>", true)

	var reached []string
	d := NewDistributor(idx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
		if session == "s1" {
			return errors.New("mailbox unavailable")
		}
		reached = append(reached, session)
		return nil
	})
	n, err := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "group-chat", Topic: "eng.general",
	})
	if err != nil {
		t.Fatalf("a single failed delivery should not fail the message: %v", err)
	}
	if n != 1 || len(reached) != 1 || reached[0] != "s2" {
		t.Fatalf("the healthy session should still receive it: n=%d reached=%v", n, reached)
	}
}

// The delivered payload must name where it came from: a session reading the event needs to
// know the channel and topic without guessing from the content.
func TestDistributePayloadCarriesProvenance(t *testing.T) {
	d, got := newTestDistributor(t, func(idx *SubscriptionIndex) {
		put(t, idx, "alice", "s1", "sub-1", "gitea", "tag.>", true)
	})
	_, _ = d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.v1", Payload: "the-tag", Source: "gitea",
	})
	body, ok := (*got)[0].payload.(map[string]any)
	if !ok {
		t.Fatalf("payload shape = %T", (*got)[0].payload)
	}
	if body["channel"] != "gitea" || body["topic"] != "tag.v1" || body["payload"] != "the-tag" {
		t.Errorf("payload lost provenance: %+v", body)
	}
}
