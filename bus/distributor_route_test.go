package bus

import (
	"context"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
)

// A channel event must reach the session whose subscription covers it, END TO END through the
// index and the mailbox publisher.
//
// Written because a deployment showed the consumer delivering and acking 12 channel messages
// while no session ever received one, and nothing logged a reason. Every part was individually
// correct; the failure was in a step with no observability.
func TestDistributeRoutesToMatchingSession(t *testing.T) {
	ctx := context.Background()

	idx := NewSubscriptionIndex(nil)
	// Build the index the same way the KV watch does, so a bug in either is caught.
	idx.apply(bus.KvEvent{
		Key:   "t.alice.s.tag-watcher.sub-gitea",
		Value: `{"channel":"gitea","pattern":"tag.agentdev/mrflow.>","wake":true}`,
	})

	var got []struct {
		tenant  string
		session string
		typ     string
	}
	d := NewDistributor(idx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
		got = append(got, struct {
			tenant  string
			session string
			typ     string
		}{tenant, session, typ})
		return nil
	})

	n, err := d.Distribute(ctx, bus.ChannelMessage{
		Tenant:  "alice",
		Channel: "gitea",
		Topic:   "tag.agentdev/mrflow.v1.2.3",
		Payload: map[string]any{"repo": "agentdev/mrflow"},
		Source:  "gitea",
	})
	if err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if n != 1 {
		t.Fatalf("delivered to %d sessions, want 1 (index holds %d subscriptions for alice)",
			n, idx.Len("alice"))
	}
	if len(got) != 1 {
		t.Fatalf("publisher called %d times, want 1", len(got))
	}
	if got[0].session != "tag-watcher" {
		t.Fatalf("routed to session %q, want tag-watcher", got[0].session)
	}
	if got[0].typ != "trigger" {
		t.Fatalf("delivered as type %q, want trigger (the subscription asks to wake)", got[0].typ)
	}
}

// An unparseable subject must be REPORTED, not silently dropped.
func TestDistributeReportsUnparseableSubject(t *testing.T) {
	idx := NewSubscriptionIndex(nil)
	d := NewDistributor(idx, func(context.Context, string, string, string, any, string) error {
		t.Fatal("publisher must not be called for an unparseable subject")
		return nil
	})
	n, err := d.Distribute(context.Background(), bus.ChannelMessage{
		UnparseableSubject: "easybus.alice.mailbox.sess-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("delivered to %d, want 0", n)
	}
}
