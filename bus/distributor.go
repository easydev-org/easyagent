package bus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/easydev-org/easybus-sdk/bus"
)

// Distributor turns channel messages into mailbox messages.
//
// This is the last step of the channel design, and the one that makes channels useful
// without duplicating anything: once a message is in a session's mailbox, the EXISTING
// pipeline takes over — the same consumer, the same trigger/interrupt/event semantics, the
// same wake-up path that already handles user prompts. A channel is therefore a new WAY IN,
// not a second input system.
//
// The alternative — delivering channel events through a separate path — would have meant a
// second set of rules for what wakes a session, what queues when it is busy, and what
// survives a restart. That duplication is what this design avoids.
type Distributor struct {
	index *SubscriptionIndex
	// publish is the mailbox publisher; nil disables distribution.
	publish func(ctx context.Context, tenant, session, typ string, payload any, source string) error
	// log is optional; nil disables the delivery diagnostics.
	log *slog.Logger
}

// NewDistributor wires an index to a mailbox publisher.
func NewDistributor(index *SubscriptionIndex, publish func(ctx context.Context, tenant, session, typ string, payload any, source string) error) *Distributor {
	return &Distributor{index: index, publish: publish}
}

// WithLogger attaches a logger for delivery diagnostics.
func (d *Distributor) WithLogger(l *slog.Logger) *Distributor {
	d.log = l
	return d
}

// Distribute delivers one channel message to every interested session.
//
// Returns the number of sessions it reached, so the caller can log a message that reached
// nobody — which is normal (nobody subscribed) but worth being able to see.
func (d *Distributor) Distribute(ctx context.Context, msg bus.ChannelMessage) (int, error) {
	if msg.UnparseableSubject != "" {
		// The transport could not read the subject. It still acked the message, so without this
		// the drop was invisible: the consumer reported delivered-and-acked while nothing was
		// routed anywhere.
		if d.log != nil {
			d.log.Warn("channel subject unparseable; event dropped",
				"subject", msg.UnparseableSubject)
		}
		return 0, nil
	}
	if d.index == nil || d.publish == nil {
		if d.log != nil {
			d.log.Warn("channel distributor not wired; event dropped",
				"index_nil", d.index == nil, "publish_nil", d.publish == nil,
				"tenant", msg.Tenant, "channel", msg.Channel, "topic", msg.Topic)
		}
		return 0, nil
	}
	matches := d.index.Match(msg.Tenant, msg.Channel, msg.Topic)
	if d.log != nil {
		// Logged at INFO on every message: the previous silence here is why "delivered but never
		// distributed" took a day to find. The index size separates "nothing subscribed" from
		// "the watch is not feeding the index", which look identical from outside.
		d.log.Info("channel event routing",
			"tenant", msg.Tenant, "channel", msg.Channel, "topic", msg.Topic,
			"matches", len(matches), "subscriptions_known", d.index.Len(msg.Tenant))
	}
	if len(matches) == 0 {
		// Logged at debug: a channel nobody follows is normal, but "nobody follows" and
		// "the index is empty" look identical from here, and that ambiguity hid a real bug
		// once already.
		if d.log != nil {
			// INFO, not debug: "a channel event arrived and reached nobody" is the single
			// fact needed to tell a working-but-quiet channel from a broken router, and it
			// is rare enough to be worth seeing by default. The index size is included
			// because an empty index and an unmatched subscription look identical otherwise.
			d.log.Info("channel event reached no subscriber",
				"tenant", msg.Tenant, "channel", msg.Channel, "topic", msg.Topic,
				"subscriptions_known", d.index.Len(msg.Tenant))
		}
		return 0, nil
	}

	n := 0
	for session, subs := range matches {
		// One delivery per session, not per matching subscription: a session that follows the
		// same topic twice should see the event once. Wake is true if ANY of its matching
		// subscriptions asks for it — a session that wants to be woken is woken.
		wake := false
		for _, s := range subs {
			if s.Wake {
				wake = true
				break
			}
		}
		typ := "event"
		if wake {
			typ = "trigger"
		}

		// The payload keeps the channel and topic alongside the event body, so the session
		// knows where the message came from without having to guess from its content.
		body := map[string]any{
			"channel": msg.Channel,
			"topic":   msg.Topic,
			"source":  msg.Source,
			"payload": msg.Payload,
		}
		if err := d.publish(ctx, msg.Tenant, session, typ, body, "channel:"+msg.Channel); err != nil {
			// One failed delivery must not stop the others: a session whose mailbox is
			// temporarily unavailable should not silence every other subscriber.
			//
			// Logged, not swallowed: a silent continue here is indistinguishable from "nobody
			// subscribed", and that ambiguity is exactly what hid this failure.
			if d.log != nil {
				d.log.Warn("channel delivery failed",
					"tenant", msg.Tenant, "session", session, "channel", msg.Channel, "err", err)
			}
			continue
		}
		n++
	}
	return n, nil
}

// Start begins consuming channels and distributing to mailboxes.
//
// One consumer for all channels of all tenants. A failure here disables channels but leaves
// the rest of the agent working: channels are additive, so a broker that cannot serve them
// must not take prompts down with it.
func (d *Distributor) Start(ctx context.Context, ch *bus.Channels, queue string) (func(), error) {
	if ch == nil {
		return func() {}, nil
	}
	return ch.ConsumeChannels(ctx, queue, func(msg bus.ChannelMessage) error {
		if _, err := d.Distribute(ctx, msg); err != nil {
			return fmt.Errorf("distribute %s/%s: %w", msg.Channel, msg.Topic, err)
		}
		return nil
	})
}
