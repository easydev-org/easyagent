package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// Channel subscriptions, agent side.
//
// One consumer receives EVERY channel message of every tenant, and the agent routes each one
// to the sessions that follow it. That shape was chosen over "one consumer per
// subscription" for two reasons that matter at scale:
//
//   - a JetStream consumer's filter is fixed at creation, so a per-subscription consumer
//     would have to be recreated every time a user changed what they follow;
//   - N consumers with N pull loops cost N times the idle traffic; one costs one.
//
// The price is that routing happens here, against an in-memory index that mirrors the NATS
// KV bucket. The bucket is the source of truth: every replica watches it, so they converge
// without coordination and a restart rebuilds the index from the same snapshot.

// Subscription is one session's interest in a channel.
type Subscription struct {
	// ID is the KV key segment; it identifies this subscription for removal.
	ID string
	// Channel is the channel name, e.g. "gitea".
	Channel string
	// Pattern is matched against the publisher's topic with NATS subject semantics
	// ("eng.*", "tag.agentdev/mrflow.>").
	Pattern string
	// Wake decides whether a matching message drives a turn.
	//
	// TRUE by default: that is the point of a subscription. Something happened and the
	// session should react. A subscription that never wakes is a log, not a channel.
	Wake bool
	// System marks the built-in subscription every session gets, which a client must not
	// offer to remove.
	System bool
}

// subscriptionValue is the KV value shape.
type subscriptionValue struct {
	Channel string `json:"channel"`
	Pattern string `json:"pattern"`
	Wake    bool   `json:"wake"`
	System  bool   `json:"system,omitempty"`
}

// SubscriptionIndex routes channel messages to sessions.
//
// Holds a local mirror of every subscription in every tenant, kept current by a KV watch.
// Reads are lock-guarded map lookups: for a few thousand sessions the match takes
// microseconds, which is why there is no cache to invalidate wrongly.
type SubscriptionIndex struct {
	mu sync.RWMutex
	// tenant -> session -> subscriptions
	byTenant map[string]map[string][]Subscription

	// kv is the source of truth; nil means subscriptions are not configured, and every
	// message is dropped rather than guessed at.
	kv bus.Bus
}

// NewSubscriptionIndex returns an empty index.
func NewSubscriptionIndex(kv bus.Bus) *SubscriptionIndex {
	return &SubscriptionIndex{byTenant: map[string]map[string][]Subscription{}, kv: kv}
}

// Start loads the snapshot and follows changes.
//
// Snapshot and updates go through the SAME apply path. Deliberate: a restart must produce
// exactly the state a long-running process converges to, and two code paths are how those
// drift apart.
func (x *SubscriptionIndex) Start(ctx context.Context) (func(), error) {
	if x.kv == nil {
		return func() {}, nil
	}
	ch, stop, err := x.kv.KvWatch(ctx, protocol.ChannelKVBucket, protocol.SubscriptionKVPattern)
	if err != nil {
		return nil, fmt.Errorf("watch subscriptions: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				x.apply(ev)
			}
		}
	}()
	return stop, nil
}

// apply folds one KV event into the index.
func (x *SubscriptionIndex) apply(ev bus.KvEvent) {
	tenant, session, id, ok := parseSubscriptionKey(ev.Key)
	if !ok || ev.Done {
		return
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	sessions := x.byTenant[tenant]
	if sessions == nil {
		sessions = map[string][]Subscription{}
		x.byTenant[tenant] = sessions
	}

	// Remove any previous version of this ID first: a put is an UPSERT, and keeping the old
	// entry would make a subscription keep matching a pattern the user has changed.
	existing := sessions[session]
	kept := existing[:0]
	for _, s := range existing {
		if s.ID != id {
			kept = append(kept, s)
		}
	}

	if ev.Deleted {
		if len(kept) == 0 {
			delete(sessions, session)
		} else {
			sessions[session] = kept
		}
		return
	}

	var v subscriptionValue
	if err := json.Unmarshal([]byte(ev.Value), &v); err != nil || v.Channel == "" || v.Pattern == "" {
		// A malformed value is dropped rather than defaulted: guessing "pattern = >" would
		// subscribe a session to everything.
		sessions[session] = kept
		return
	}
	sessions[session] = append(kept, Subscription{
		ID: id, Channel: v.Channel, Pattern: v.Pattern, Wake: v.Wake, System: v.System,
	})
}

// Match returns the subscriptions interested in one message, keyed by session.
//
// Only the message's own tenant is consulted, which is where cross-tenant delivery is
// prevented: the tenant comes from the subject (the broker's routing key), not from anything
// the publisher controls.
func (x *SubscriptionIndex) Match(tenant, channel, topic string) map[string][]Subscription {
	x.mu.RLock()
	defer x.mu.RUnlock()
	sessions := x.byTenant[tenant]
	if len(sessions) == 0 {
		return nil
	}
	out := make(map[string][]Subscription, 4)
	for session, subs := range sessions {
		for _, s := range subs {
			if s.Channel != channel {
				continue
			}
			if !protocol.MatchSubject(s.Pattern, topic) {
				continue
			}
			out[session] = append(out[session], s)
		}
	}
	return out
}

// Len reports how many subscriptions the index holds for a tenant.
//
// Exists so a "delivered to nobody" log can distinguish an EMPTY index (the watch is not
// feeding it) from a live index with no matching subscription (nothing follows this topic).
// Those two failures are indistinguishable without it, which is how a broken watch can look
// like a quiet channel.
func (x *SubscriptionIndex) Len(tenant string) int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n := 0
	for _, subs := range x.byTenant[tenant] {
		n += len(subs)
	}
	return n
}

// List returns a session's subscriptions, in a stable order.
func (x *SubscriptionIndex) List(tenant, session string) []Subscription {
	x.mu.RLock()
	defer x.mu.RUnlock()
	subs := x.byTenant[tenant][session]
	out := append([]Subscription(nil), subs...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Channel != out[j].Channel {
			return out[i].Channel < out[j].Channel
		}
		return out[i].Pattern < out[j].Pattern
	})
	return out
}

// parseSubscriptionKey splits t.<tenant>.s.<session>.<id>.
func parseSubscriptionKey(key string) (tenant, session, id string, ok bool) {
	if !strings.HasPrefix(key, "t.") {
		return "", "", "", false
	}
	rest := key[2:]
	i := strings.Index(rest, ".s.")
	if i <= 0 {
		return "", "", "", false
	}
	tenant = rest[:i]
	rest = rest[i+len(".s."):]
	j := strings.LastIndexByte(rest, '.')
	if j <= 0 || j == len(rest)-1 {
		return "", "", "", false
	}
	session = protocol.UnescapeKVSegment(rest[:j])
	return tenant, session, rest[j+1:], true
}
