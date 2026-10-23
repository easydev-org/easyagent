package bus

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// SubscriptionStore is the writable side of the subscription index.
//
// Writes go to the KV bucket, never to the in-memory index: the index is a MIRROR, and the
// watch that keeps it current is what applies the change. Writing both would mean two
// sources of truth that can disagree, and a UI that shows a subscription the router does
// not have.
type SubscriptionStore struct {
	kv    kvPutter
	index *SubscriptionIndex
}

// ErrSystemSubscription is returned when a caller tries to remove the built-in one.
var ErrSystemSubscription = errors.New("the built-in session-channel subscription cannot be removed")

// NewSubscriptionStore returns a store writing to the same bucket the index watches.
func NewSubscriptionStore(kv kvPutter, index *SubscriptionIndex) *SubscriptionStore {
	return &SubscriptionStore{kv: kv, index: index}
}

// List returns a session's subscriptions, read from the index.
//
// Read from the INDEX rather than the bucket: the index is already the merged view of the
// snapshot plus every subsequent write, so a subscription the user just added is visible
// immediately rather than after a round trip to the broker.
func (s *SubscriptionStore) List(tenant, session string) []Subscription {
	if s == nil || s.index == nil {
		return nil
	}
	return s.index.List(tenant, session)
}

// Put writes one subscription.
//
// The ID is derived from the channel and pattern, so subscribing twice to the same thing
// updates one entry instead of accumulating duplicates. A user who re-subscribes is
// adjusting (perhaps turning wake on), not adding a second listener.
func (s *SubscriptionStore) Put(ctx context.Context, tenant, session, channel, pattern string, wake bool) (Subscription, error) {
	if s == nil || s.kv == nil {
		return Subscription{}, errors.New("channels are not configured")
	}
	id := SubscriptionID(channel, pattern)
	val, err := json.Marshal(subscriptionValue{Channel: channel, Pattern: pattern, Wake: wake})
	if err != nil {
		return Subscription{}, err
	}
	if err := s.kv.KVPut(ctx, protocol.ChannelKVBucket,
		protocol.SubscriptionKVKey(tenant, session, id), string(val), 0); err != nil {
		return Subscription{}, err
	}
	return Subscription{ID: id, Channel: channel, Pattern: pattern, Wake: wake}, nil
}

// Delete removes one subscription.
//
// Refuses the built-in one: it is how anything reaches the session by name, and a session
// that cannot be addressed is a state the user did not ask for.
func (s *SubscriptionStore) Delete(ctx context.Context, tenant, session, id string) error {
	if s == nil || s.kv == nil {
		return errors.New("channels are not configured")
	}
	if id == DefaultSubscriptionID {
		return ErrSystemSubscription
	}
	return s.kv.KVDelete(ctx, protocol.ChannelKVBucket,
		protocol.SubscriptionKVKey(tenant, session, id))
}

// SubscriptionID derives a stable id from what the subscription listens to.
//
// Deterministic so re-subscribing is an update: the KV key is built from it, and a random
// id would leave the old entry behind matching the same messages — the session would receive
// each event twice.
func SubscriptionID(channel, pattern string) string {
	// A dot or slash in either part would change the key's segment count, so both are
	// escaped with the same helper the session name uses.
	return "sub-" + protocol.EscapeKVSegment(channel) + "-" + protocol.EscapeKVSegment(pattern)
}
