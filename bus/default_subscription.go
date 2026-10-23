package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// The built-in channel every session follows.
//
// `session-channel.<name>` is how anything reaches ONE session now. The mailbox stays as the
// agent's internal delivery mechanism, but a sender no longer has to know about it: they
// publish to this channel like any other. That keeps one concept in the model ("a session
// follows channels") instead of two ("channels, except for direct messages").
const (
	// DefaultChannel is the channel name every session subscribes to.
	DefaultChannel = "session-channel"
	// DefaultSubscriptionID is stable so re-provisioning updates rather than duplicates.
	DefaultSubscriptionID = "system-default"
)

// sessionNameSegment rejects a session name that cannot be a subject segment.
//
// The default subscription's pattern is built from the session name, so a name containing a
// dot would silently widen the pattern ("a.b" -> "a.b.>" matches "a.b.x" but is really two
// segments) and a name containing ">" would subscribe the session to everything.
var sessionNameSegment = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// DefaultPattern is the pattern of the built-in subscription: everything addressed to this
// session within its own channel.
func DefaultPattern(session string) string {
	return session + ".>"
}

// EnsureDefaultSubscription writes the built-in subscription for a session.
//
// Idempotent: the KV key is derived from the session, so calling it on every session
// creation (and again on a fork) updates one entry rather than growing a list.
//
// Wake is TRUE: this is the channel other sessions and users use to reach this one, and a
// message that did not wake it would sit unread until the session happened to run for some
// unrelated reason.
//
// A session name that cannot be a subject segment is REPORTED rather than silently skipped:
// the session would otherwise exist but be unreachable, with nothing to point at.
func EnsureDefaultSubscription(ctx context.Context, kv kvPutter, tenant, session string) error {
	if kv == nil {
		return nil
	}
	if !sessionNameSegment.MatchString(session) {
		return fmt.Errorf("session name %q must match %s to be addressable on a channel",
			session, sessionNameSegment.String())
	}
	val, err := json.Marshal(subscriptionValue{
		Channel: DefaultChannel,
		Pattern: DefaultPattern(session),
		Wake:    true,
		System:  true,
	})
	if err != nil {
		return err
	}
	return kv.KVPut(ctx, protocol.ChannelKVBucket,
		protocol.SubscriptionKVKey(tenant, session, DefaultSubscriptionID), string(val), 0)
}

// kvPutter is the slice of the bus this file needs, so tests can supply a fake.
type kvPutter interface {
	KVPut(ctx context.Context, bucket, key, value string, ttlMs int64) error
	KVDelete(ctx context.Context, bucket, key string) error
}

// DefaultChannelTopic builds the topic for a message addressed to one session.
//
// The session name is the first topic segment so the default pattern (`<name>.>`) catches it
// together with any sub-addressing the sender wants to add.
func DefaultChannelTopic(session, sub string) string {
	if sub == "" {
		// A topic needs at least one segment after the channel, and the default pattern is
		// `<name>.>`, so a bare session name would not match. "msg" is the neutral filler.
		return session + ".msg"
	}
	return session + "." + strings.TrimPrefix(sub, ".")
}
