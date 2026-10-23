package bus

import (
	"context"
	"encoding/json"
	"testing"

	sdkbus "github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// fakeKV records writes so the default-subscription writer can be tested without a broker.
type fakeKV struct {
	writes map[string]string
}

func newFakeKV() *fakeKV { return &fakeKV{writes: map[string]string{}} }

func (f *fakeKV) KVPut(_ context.Context, _ string, key, value string, _ int64) error {
	f.writes[key] = value
	return nil
}

func (f *fakeKV) KVDelete(_ context.Context, _ string, key string) error {
	delete(f.writes, key)
	return nil
}

// Every session must be reachable by name, which means it must carry a default subscription
// to session-channel.<its-name>.>.
func TestEnsureDefaultSubscriptionWritesTheChannel(t *testing.T) {
	kv := newFakeKV()
	if err := EnsureDefaultSubscription(context.Background(), kv, "alice", "perm-parent"); err != nil {
		t.Fatal(err)
	}
	key := protocol.SubscriptionKVKey("alice", "perm-parent", DefaultSubscriptionID)
	raw, ok := kv.writes[key]
	if !ok {
		t.Fatalf("no subscription written; got keys %v", kv.writes)
	}
	var v subscriptionValue
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("value is not JSON: %q", raw)
	}
	if v.Channel != "session-channel" {
		t.Errorf("channel = %q, want session-channel", v.Channel)
	}
	if v.Pattern != "perm-parent.>" {
		t.Errorf("pattern = %q, want perm-parent.>", v.Pattern)
	}
	// Wake must be on: this is how anything reaches the session, and a message that did not
	// wake it would wait for an unrelated turn.
	if !v.Wake {
		t.Error("the default subscription must wake the session")
	}
	// System marks it as not removable by a client.
	if !v.System {
		t.Error("the default subscription must be marked system")
	}
}

// The written subscription must actually route traffic addressed to the session.
func TestDefaultSubscriptionRoutesToTheSession(t *testing.T) {
	kv := newFakeKV()
	if err := EnsureDefaultSubscription(context.Background(), kv, "alice", "s1"); err != nil {
		t.Fatal(err)
	}

	idx := NewSubscriptionIndex(nil)
	for k, v := range kv.writes {
		idx.apply(kvEventFor(t, k, v))
	}
	topic := DefaultChannelTopic("s1", "")
	got := idx.Match("alice", DefaultChannel, topic)
	if len(got["s1"]) != 1 {
		t.Fatalf("a message on the default topic must reach the session, got %+v (topic=%q)", got, topic)
	}
}

// A message addressed to one session must NOT reach another session's default channel.
func TestDefaultSubscriptionIsPerSession(t *testing.T) {
	kv := newFakeKV()
	_ = EnsureDefaultSubscription(context.Background(), kv, "alice", "s1")
	_ = EnsureDefaultSubscription(context.Background(), kv, "alice", "s2")

	idx := NewSubscriptionIndex(nil)
	for k, v := range kv.writes {
		idx.apply(kvEventFor(t, k, v))
	}
	got := idx.Match("alice", DefaultChannel, DefaultChannelTopic("s1", ""))
	if len(got["s2"]) != 0 {
		t.Errorf("s1's message reached s2: %+v", got)
	}
}

// Idempotent: writing twice must leave ONE subscription, not two. Session creation and fork
// both call this, and a re-run (or a retried request) must not accumulate.
func TestEnsureDefaultSubscriptionIsIdempotent(t *testing.T) {
	kv := newFakeKV()
	for i := 0; i < 3; i++ {
		if err := EnsureDefaultSubscription(context.Background(), kv, "alice", "s1"); err != nil {
			t.Fatal(err)
		}
	}
	if len(kv.writes) != 1 {
		t.Fatalf("expected one KV key, got %d: %v", len(kv.writes), kv.writes)
	}

	idx := NewSubscriptionIndex(nil)
	for k, v := range kv.writes {
		idx.apply(kvEventFor(t, k, v))
	}
	if got := idx.List("alice", "s1"); len(got) != 1 {
		t.Fatalf("expected one subscription after repeats, got %+v", got)
	}
}

// A session name that cannot be a subject segment is REFUSED rather than silently
// producing a subscription that matches the wrong things.
//
// "a.b" would build the pattern "a.b.>" — two segments — which no longer means "this
// session". ">" would subscribe the session to everything.
func TestEnsureDefaultSubscriptionRejectsBadNames(t *testing.T) {
	for _, name := range []string{"a.b", "a>b", "a b", "a*b", ""} {
		kv := newFakeKV()
		if err := EnsureDefaultSubscription(context.Background(), kv, "alice", name); err == nil {
			t.Errorf("session name %q should have been refused", name)
		}
		if len(kv.writes) != 0 {
			t.Errorf("session name %q wrote a subscription anyway: %v", name, kv.writes)
		}
	}
}

// A nil store means channels are not configured; that is not an error, it is "no broker".
func TestEnsureDefaultSubscriptionNilStore(t *testing.T) {
	if err := EnsureDefaultSubscription(context.Background(), nil, "alice", "s1"); err != nil {
		t.Fatalf("a nil store must be a no-op, got %v", err)
	}
}

func TestDefaultChannelTopic(t *testing.T) {
	if got := DefaultChannelTopic("s1", ""); got != "s1.msg" {
		t.Errorf("bare topic = %q, want s1.msg", got)
	}
	if got := DefaultChannelTopic("s1", "urgent"); got != "s1.urgent" {
		t.Errorf("sub topic = %q, want s1.urgent", got)
	}
	if got := DefaultChannelTopic("s1", ".urgent"); got != "s1.urgent" {
		t.Errorf("leading dot should be trimmed, got %q", got)
	}
	// The generated topic must always match the default pattern.
	for _, sub := range []string{"", "urgent"} {
		topic := DefaultChannelTopic("s1", sub)
		if !protocol.MatchSubject(DefaultPattern("s1"), topic) {
			t.Errorf("topic %q does not match pattern %q", topic, DefaultPattern("s1"))
		}
	}
}

// kvEventFor builds the watch event a KV put would produce.
func kvEventFor(t *testing.T, key, value string) sdkbus.KvEvent {
	t.Helper()
	return sdkbus.KvEvent{Key: key, Value: value}
}
