package bus

import (
	"context"
	"strings"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
)

// A waking channel event must carry TEXT, because a trigger becomes a user message.
//
// Observed in a deployment: the event was delivered, `Wake` drained it, and the turn failed with
//
//	llm: the parameter `messages` ... an array with minimum length 1, but got an empty array
//
// The channel payload has no `text` field, so the trigger folded to a user message with no text
// part. Every earlier stage reported success; only the model refused.
func TestWakingChannelEventCarriesText(t *testing.T) {
	idx := NewSubscriptionIndex(nil)
	idx.apply(bus.KvEvent{
		Key:   "t.alice.s.sess.sub-1",
		Value: `{"channel":"gitea","pattern":"tag.acme/app.>","wake":true}`,
	})

	var got map[string]any
	d := NewDistributor(idx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
		got, _ = payload.(map[string]any)
		return nil
	})

	if _, err := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant:  "alice",
		Channel: "gitea",
		Topic:   "tag.acme/app.v1.0.0",
		Payload: map[string]any{"repo": "acme/app", "tag": "v1.0.0"},
		Source:  "gitea",
	}); err != nil {
		t.Fatalf("distribute: %v", err)
	}

	if got == nil {
		t.Fatal("publisher was not called")
	}
	text, _ := got["text"].(string)
	if strings.TrimSpace(text) == "" {
		t.Fatal("a waking event carried no text: the trigger would fold into an empty user " +
			"message, and a session with no history would send the model an empty messages array")
	}
	// The summary must name what happened, or the model has nothing to act on.
	for _, want := range []string{"gitea", "tag.acme/app.v1.0.0"} {
		if !strings.Contains(text, want) {
			t.Errorf("text %q does not mention %q", text, want)
		}
	}
	// The authoritative body must still be present for tools that want it structurally.
	if _, ok := got["payload"]; !ok {
		t.Error("the structured payload was dropped; the summary must not replace it")
	}
}

// A NON-waking event is a log entry, not a prompt: giving it text would make it a user message.
func TestNonWakingChannelEventHasNoText(t *testing.T) {
	idx := NewSubscriptionIndex(nil)
	idx.apply(bus.KvEvent{
		Key:   "t.alice.s.sess.sub-1",
		Value: `{"channel":"gitea","pattern":"tag.acme/app.>","wake":false}`,
	})

	var got map[string]any
	d := NewDistributor(idx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
		got, _ = payload.(map[string]any)
		return nil
	})
	if _, err := d.Distribute(context.Background(), bus.ChannelMessage{
		Tenant: "alice", Channel: "gitea", Topic: "tag.acme/app.v1.0.0", Source: "gitea",
	}); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if _, ok := got["text"]; ok {
		t.Error("a non-waking event carried text; it would be folded into a user message " +
			"and start a turn nobody asked for")
	}
}
