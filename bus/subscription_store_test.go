package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// The store writes to KV and never to the index: the index is a mirror fed by the watch, so
// writing both would create two sources of truth that can disagree.
//
// These tests therefore check the KV write, and separately that applying that write to an
// index produces the subscription a user expects.

func TestSubscriptionStorePut(t *testing.T) {
	kv := newFakeKV()
	idx := NewSubscriptionIndex(nil)
	st := NewSubscriptionStore(kv, idx)

	sub, err := st.Put(context.Background(), "alice", "s1", "gitea", "tag.agentdev/mrflow.>", true)
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID == "" {
		t.Fatal("a subscription needs an id, or it cannot be removed")
	}

	key := protocol.SubscriptionKVKey("alice", "s1", sub.ID)
	raw, ok := kv.writes[key]
	if !ok {
		t.Fatalf("nothing written; keys = %v", kv.writes)
	}
	// Applying the write the watch would deliver must produce the intended subscription.
	idx.apply(kvEventFor(t, key, raw))
	got := idx.List("alice", "s1")
	if len(got) != 1 {
		t.Fatalf("index has %d subscriptions", len(got))
	}
	if got[0].Channel != "gitea" || got[0].Pattern != "tag.agentdev/mrflow.>" || !got[0].Wake {
		t.Errorf("subscription = %+v", got[0])
	}
}

// Subscribing twice to the same thing UPDATES rather than accumulating. A user who turns
// wake on is adjusting one listener, not adding a second — and two listeners would deliver
// every event twice.
func TestSubscriptionStorePutIsIdempotent(t *testing.T) {
	kv := newFakeKV()
	st := NewSubscriptionStore(kv, NewSubscriptionIndex(nil))

	a, _ := st.Put(context.Background(), "alice", "s1", "gitea", "tag.>", false)
	b, _ := st.Put(context.Background(), "alice", "s1", "gitea", "tag.>", true)

	if a.ID != b.ID {
		t.Errorf("the same channel+pattern must produce the same id: %q vs %q", a.ID, b.ID)
	}
	if len(kv.writes) != 1 {
		t.Fatalf("expected one KV entry after re-subscribing, got %d", len(kv.writes))
	}
}

// Different patterns on the same channel are different subscriptions.
func TestSubscriptionStoreDistinctPatterns(t *testing.T) {
	st := NewSubscriptionStore(newFakeKV(), NewSubscriptionIndex(nil))
	a, _ := st.Put(context.Background(), "alice", "s1", "gitea", "tag.agentdev/mrflow.>", true)
	b, _ := st.Put(context.Background(), "alice", "s1", "gitea", "tag.other/repo.>", true)
	if a.ID == b.ID {
		t.Fatal("different patterns must not share an id")
	}
}

// The built-in subscription must not be removable: it is how anything reaches the session
// by name.
func TestSubscriptionStoreRefusesSystemDelete(t *testing.T) {
	st := NewSubscriptionStore(newFakeKV(), NewSubscriptionIndex(nil))
	err := st.Delete(context.Background(), "alice", "s1", DefaultSubscriptionID)
	if !errors.Is(err, ErrSystemSubscription) {
		t.Fatalf("err = %v, want ErrSystemSubscription", err)
	}
}

func TestSubscriptionStoreDeleteRemoves(t *testing.T) {
	kv := newFakeKV()
	st := NewSubscriptionStore(kv, NewSubscriptionIndex(nil))
	sub, _ := st.Put(context.Background(), "alice", "s1", "gitea", "tag.>", true)

	if err := st.Delete(context.Background(), "alice", "s1", sub.ID); err != nil {
		t.Fatal(err)
	}
	if len(kv.writes) != 0 {
		t.Fatalf("the KV entry survived deletion: %v", kv.writes)
	}
}

// An id containing dots or slashes must not change the KV key's segment count, or the
// subscription would be filed under a different session/path than it was written to.
func TestSubscriptionIDEscapes(t *testing.T) {
	// "tag.agentdev/mrflow.>" contains dots AND a slash.
	id := SubscriptionID("gitea", "tag.agentdev/mrflow.>")
	if containsRaw(id, '.') {
		t.Errorf("id %q must not contain a raw dot", id)
	}
	key := protocol.SubscriptionKVKey("alice", "s1", id)
	tenant, session, parsed, ok := parseSubscriptionKey(key)
	if !ok || tenant != "alice" || session != "s1" || parsed != id {
		t.Fatalf("key %q did not round trip: (%q,%q,%q,%v)", key, tenant, session, parsed, ok)
	}
}

func containsRaw(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}
