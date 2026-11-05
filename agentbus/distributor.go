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
		/**
		 * THE SELF-ECHO CHECK READS THE PAYLOAD, NOT THE SOURCE.
		 *
		 * `Source` is what the CONSOLE renders (`group:<name>/<sender>`, or `session:<id>` for a private
		 * hand-off) and its shape is a presentation decision that has already changed once. Routing must not
		 * depend on it: the payload carries `from` on every group message, and that is the sender.
		 */
		if msg.Channel == GroupChannel && payloadString(msg.Payload, "from") == session {
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
 * payloadAttachment is one file reference carried by a group message.
 *
 * `Mime` IS NOT CARRIED BY THE WIRE, and it is not invented here either: it is left empty so a reader that
 * needs it asks the blob store, which is the only place that knows. A guessed type is worse than a missing
 * one — a voice note rendered as an unknown file is honest; one rendered as an image is broken.
 */
type payloadAttachment struct {
	code string
	name string
	mime string
}

/**
 * payloadAttachments reads the `attachments` array from a channel payload.
 *
 * A MISSHAPEN ENTRY IS SKIPPED, not reported: the message and its words are still worth delivering, and a
 * single malformed reference must not cost the reader the whole prompt.
 */
func payloadAttachments(payload any) []payloadAttachment {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := m["attachments"].([]map[string]any)
	if !ok {
		// The payload may have crossed a JSON boundary, where every array element is `any`.
		list, okAny := m["attachments"].([]any)
		if !okAny {
			return nil
		}
		out := make([]payloadAttachment, 0, len(list))
		for _, item := range list {
			if a, ok := item.(map[string]any); ok {
				if att, ok := payloadAttachmentOf(a); ok {
					out = append(out, att)
				}
			}
		}
		return out
	}
	out := make([]payloadAttachment, 0, len(raw))
	for _, a := range raw {
		if att, ok := payloadAttachmentOf(a); ok {
			out = append(out, att)
		}
	}
	return out
}

func payloadAttachmentOf(a map[string]any) (payloadAttachment, bool) {
	code, _ := a["code"].(string)
	if code == "" {
		return payloadAttachment{}, false
	}
	name, _ := a["name"].(string)
	mime, _ := a["mime"].(string)
	return payloadAttachment{code: code, name: name, mime: mime}, true
}

/**
 * payloadString reads one string field from a channel payload, or "" when it is absent.
 *
 * A channel payload is `any` on the wire; a group message carries `from`, `from_name` and `group` as strings,
 * and every reader here wants one of them. A missing or wrongly-typed field yields "", which callers treat as
 * "not stated" rather than as a value.
 */
func payloadString(payload any, key string) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	s, _ := m[key].(string)
	return s
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
		attachments := payloadAttachments(msg.Payload)

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
		if strings.TrimSpace(text) != "" {
			b.WriteString(text)
		} else {
			// A PHOTO WITH NO CAPTION IS STILL A MESSAGE. Saying so is better than an empty body followed by a
			// list of files, which reads as "the text is missing" rather than "there was no text".
			b.WriteString("(no text — see the attachments)")
		}
		/**
		 * THE FILES ARE NAMED, WITH THEIR CODES.
		 *
		 * A member woken by a picture needs to know one was sent and which blob to read; the code is the only
		 * thing that names it. A caption-only prompt would have it answer the words and never look at what
		 * came with them.
		 */
		if len(attachments) > 0 {
			b.WriteString("\n\nAttachments:")
			for _, a := range attachments {
				label := a.name
				if label == "" {
					label = "(unnamed)"
				}
				fmt.Fprintf(&b, "\n- %s [file:%s] %s", label, a.code, a.mime)
			}
		}

		/**
		 * THE REPLY INSTRUCTION RIDES WITH THE MESSAGE, NOT IN THE TOOL DESCRIPTION.
		 *
		 * A session that reads a group message and answers it in its own conversation has done nothing anyone
		 * in the group can see — the group shows the message and no reply, which is indistinguishable from
		 * being ignored. The tool description says so too, but a description is read once, when the tool list
		 * is built; this arrives at the moment the message does.
		 *
		 * EVERY GROUP MESSAGE CARRIES IT, not just a user's: a member reporting back gets the same trap, and so
		 * does a coordinator reading that report. The two ways to answer are spelled out because they reach
		 * different readers — a reply (no `to`) is read in the group, an addressed message (with `to`) is read
		 * by the sessions named. The tool enforces who may address whom.
		 */
		fmt.Fprintf(&b, "\n\n[Posted in group %q. Anything you write in your own conversation is NOT visible in the "+
			"group — to answer, call `group-send` with group=%q: leave `to` empty to reply to the group, or name "+
			"the recipients to address them. Do that even if your answer is \"I cannot do this\": a blocker "+
			"report is an answer, and it is the most important one to deliver.]", group, group)
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
