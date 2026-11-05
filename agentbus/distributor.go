package agentbus

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/mailbox"
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
	publish func(ctx context.Context, tenant, session string, typ mailbox.Type, payload any, source string) error
	// log is optional; nil disables the delivery diagnostics.
	log *slog.Logger
}

// NewDistributor wires an index to a mailbox publisher.
func NewDistributor(index *SubscriptionIndex, publish func(ctx context.Context, tenant, session string, typ mailbox.Type, payload any, source string) error) *Distributor {
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
		if d.log != nil {
			names := make([]string, 0, len(subs))
			for _, s := range subs {
				names = append(names, s.Pattern)
			}
			// WHICH SESSIONS AND ON WHAT GROUNDS — the counts alone cannot distinguish "one subscriber with three
			// patterns" from "three subscribers", and those are very different problems.
			d.log.Info("channel match", "session", session, "patterns", names)
		}
		/**
		 * A SENDER DOES NOT HEAR ITS OWN MESSAGE.
		 *
		 * The coordinator's pattern (`<group>.>`) matches the dispatches it publishes (`<group>.to.<member>`),
		 * because the same pattern has to match the members' replies. Without this check the coordinator
		 * received its own dispatch back, read it as a new instruction, and dispatched again — three rounds of
		 * near-identical replies, one per level of self-echo, each one costing a turn.
		 *
		 * `Origin` is the publisher's own label (`session:<id>` for a session, `group:<name>` for the group
		 * RPC). Comparing it to the recipient is what makes the echo impossible rather than unlikely.
		 */
		if msg.Channel == GroupChannel && senderOf(msg.Source) == session {
			continue
		}
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
		// A WAKE SUBSCRIPTION RUNS A TURN; anything else is context for the next one. Named types rather
		// than literals, because this value decides whether a conversation wakes at all.
		typ := mailbox.Event
		if wake {
			typ = mailbox.Trigger
		}

		/**
		 * THE BODY IS A mailbox.Body, with the channel context on its own fields.
		 *
		 * It used to be an ad-hoc map, which worked only because the runtime's decoder happened to read
		 * the same names. Three producers, three shapes, one consumer guessing — `mailbox.BodyOf` is now
		 * the only place that knows, and a producer has one type to write.
		 *
		 * PROVENANCE IS KEPT, not folded into the sentence: a tool that wants the event itself should
		 * not have to parse prose written for the model.
		 */
		body := mailbox.Body{Channel: msg.Channel, Topic: msg.Topic, Origin: msg.Source, Data: msg.Payload}
		// A `trigger` becomes a USER MESSAGE, and the runtime builds it from `text`. Without this
		// the trigger folded to an empty text part, so a session with no history sent the model an
		// empty `messages` array and the request was rejected outright:
		//
		//   llm: the parameter `messages` ... an array with minimum length 1, but got an empty array
		//
		// The event is therefore rendered into words. It is a SUMMARY, not a re-encoding: the full
		// payload rides alongside under "payload", so nothing is lost for a tool that wants to read
		// it structurally.
		if wake {
			body.Text = channelEventText(msg)
		}
		/**
		 * THE SOURCE NAMES THE SENDER WHEN THERE IS ONE.
		 *
		 * `channel:<name>` told the console only that a channel produced the message, so a group message
		 * rendered as the reader's OWN prompt: right-aligned, no author, no way to jump back to who said it.
		 * A group message has an author — `from` in the payload — and naming it as `session:<id>` is the form
		 * the console already understands for "another conversation said this".
		 *
		 * A message with no author (the user posted to the group) keeps the generic channel label, because
		 * there is nobody to attribute it to.
		 */
		source := "channel:" + msg.Channel
		if msg.Channel == GroupChannel {
			/**
			 * `group:<name>/<sender>` — THE CONSOLE NEEDS BOTH HALVES.
			 *
			 * The name says which conversation this came from, so a reader can open the group; the sender
			 * says who spoke, so the message is attributable. `session:<id>` alone rendered a group message as
			 * a private hand-off between two sessions, which is exactly what it is not.
			 */
			group := payloadString(msg.Payload, "group")
			sender := payloadString(msg.Payload, "from")
			if group != "" {
				source = "group:" + group
				if sender != "" {
					source += "/" + sender
				}
			}
		} else if sender := payloadString(msg.Payload, "from"); sender != "" {
			source = "session:" + sender
		}
		if err := d.publish(ctx, msg.Tenant, session, typ, body, source); err != nil {
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

/**
 * senderOf extracts the session id from a channel message's origin.
 *
 * `session:<id>` is what a session-built message carries; `group:<name>` and `user:group` are the group RPC's
 * labels and name no session. An origin that does not parse belongs to nobody, so it never suppresses a
 * delivery — a message whose sender cannot be identified is not a message to drop.
 */
func payloadString(payload any, key string) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

func senderOf(origin string) string {
	if rest, ok := strings.CutPrefix(origin, "session:"); ok {
		return rest
	}
	return ""
}

// channelEventText renders a channel event as the prompt a woken session receives.
//
// A `trigger` becomes a USER MESSAGE, and the runtime builds that message from `text`. Leaving it
// empty produced a user turn with no text part, so a session with no history sent the model an
// empty `messages` array and the request was refused:
//
//	llm: the parameter `messages` ... an array with minimum length 1, but got an empty array
//
// This is a SUMMARY, not a re-encoding: the authoritative body still travels under "payload", so a
// tool that wants to inspect the event structurally reads that instead of parsing prose.
func channelEventText(msg bus.ChannelMessage) string {
	var b strings.Builder

	/**
	 * A GROUP MESSAGE IS RENDERED AS WHAT IT IS: something a person said.
	 *
	 * The generic form below ("A <source> event arrived on the <channel> channel…") describes a machine event,
	 * and the model read it as one — it spent turns trying to work out what the payload meant instead of
	 * answering it. A group has a sender and a body; saying so is the whole rendering.
	 */
	if msg.Channel == GroupChannel {
		group, _ := msg.Payload.(map[string]any)["group"].(string)
		if group == "" {
			group = strings.SplitN(msg.Topic, ".", 2)[0]
		}
		from, _ := msg.Payload.(map[string]any)["from"].(string)
		fromName, _ := msg.Payload.(map[string]any)["from_name"].(string)
		text, _ := msg.Payload.(map[string]any)["text"].(string)

		/**
		 * NAME THE SENDER, BY THE NAME A READER RECOGNISES.
		 *
		 * "Another member of the group" told the model nothing it could act on: it could not tell a
		 * coordinator from a peer, could not attribute a claim, and could not decide whether a reply was
		 * expected from it. The session's display name is what the member list shows, so it is what the
		 * model should see.
		 *
		 * The ID RIDES ALONGSIDE the name, because the name is a label two sessions may share and the ID is
		 * the only thing that identifies one. A model that needs to address the sender back needs the ID.
		 */
		switch {
		case from == "":
			b.WriteString("The user posted to group " + strconv.Quote(group) + ":")
		case fromName != "":
			fmt.Fprintf(&b, "%s (%s) posted to group %q:", fromName, from, group)
		default:
			fmt.Fprintf(&b, "Session %s posted to group %q:", from, group)
		}
		b.WriteString("\n\n")
		b.WriteString(text)
		return b.String()
	}

	// THE PREFIX IS WRITTEN ONCE. It used to be written twice for an empty source — the second Fprintf had no
	// else — so every such event began with two nearly identical sentences.
	switch {
	case msg.Source != "":
		fmt.Fprintf(&b, "A %s event arrived on the %q channel", msg.Source, msg.Channel)
	default:
		fmt.Fprintf(&b, "A channel event arrived on the %q channel", msg.Channel)
	}
	fmt.Fprintf(&b, " (topic %s).", msg.Topic)

	// Payload keys, sorted: map order is random in Go, and a prompt that changes order between two
	// identical events would defeat any prompt-level caching and make logs hard to compare.
	if m, ok := msg.Payload.(map[string]any); ok && len(m) > 0 {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "\n- %s: %v", k, m[k])
		}
	}
	return b.String()
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
