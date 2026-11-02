package bus

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/easydev-org/easybus-sdk/bus"
)

/**
 * BridgeExtensionMail bridges mailbox messages published BY EXTENSIONS into this agent's queue.
 *
 * WHY THIS EXISTS AT ALL, AND IT IS A GAP RATHER THAN A FEATURE.
 *
 * The SDK offers two mailbox paths that look interchangeable and are not:
 *
 *	extension.PublishMailboxEvent   publishes onto the NATS subject only
 *	agent.ConsumeMailbox            hands a LIVE SUBSCRIBER the message
 *
 * This agent stores mailbox messages in POSTGRES, and `Wake` drains THAT TABLE. A message that
 * exists only on the subject is, to the runtime, a message that never arrived: the wake fires, the
 * drain finds nothing, and the turn does not happen. `PublishMailbox`'s own comment records this
 * failing once already from the other direction — a publisher that signalled without enqueueing —
 * and it is why that function refuses to publish without a queue.
 *
 * An extension is a separate process with no database handle, so it CANNOT enqueue. The choice was
 * to give every extension a new RPC for delivery, or to have the agent read the subject extensions
 * already publish to. This reads the subject: the publisher is unchanged, and the message keeps the
 * durability the table gives it.
 *
 * NOTHING IS PARSED OUT OF THE SUBJECT. The envelope already carries `Tenant` and `SessionName`;
 * splitting `easybus.<tenant>.mailbox.<session>` would be a second, weaker opinion about who a
 * message is for — and the subject's session segment is escaped, so a naive split would enqueue
 * under a name no session has.
 */
func (b *Bus) BridgeExtensionMail(ctx context.Context) (func(), error) {
	if b.db == nil {
		// WITHOUT A QUEUE THERE IS NOWHERE TO PUT THEM, and waking a runtime would find an empty
		// table — the exact failure this bridge exists to prevent.
		return nil, fmt.Errorf("mailbox bridge: no queue configured (call WithMailboxQueue)")
	}
	sub, err := b.transport.Subscribe(ctx, "easybus.*.mailbox.*", bus.SubscribeOpts{})
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			tenant := env.GetTenant()
			session := env.GetSessionName()
			if tenant == "" || session == "" {
				// A MAILBOX MESSAGE WITH NO ADDRESS IS UNDELIVERABLE. The SDK fills both when it
				// publishes, so an empty one means a publisher that built the envelope by hand —
				// worth a warning, not worth guessing a destination for.
				slog.WarnContext(ctx, "mailbox bridge: message has no tenant or session",
					"ch", env.GetCh(), "tenant", tenant, "session", session)
				continue
			}
			typ, payload, source := extensionMailParts(env.GetPayload().AsInterface())
			// WHAT ACTUALLY ARRIVED, at the level that decides whether the message has a body. A
			// delivered trigger with an empty text folds an empty user message — the conversation
			// gains a turn with nothing in it, which reads as the model ignoring the notification.
			slog.InfoContext(ctx, "mailbox bridge: payload", "tenant", tenant, "session", session,
				"type", typ, "payload", fmt.Sprintf("%v", payload))
			/**
			 * ENQUEUE IS THE WHOLE JOB. `Wake` is driven by the row existing, so a delivered
			 * message needs nothing else — and the boot backstop drains it if no wake races
			 * through in time.
			 */
			if _, eerr := b.db.Enqueue(tenant, session, typ, payload, source); eerr != nil {
				slog.WarnContext(ctx, "mailbox bridge: cannot enqueue",
					"tenant", tenant, "session", session, "err", eerr)
				continue
			}
			slog.InfoContext(ctx, "mailbox bridge: delivered",
				"tenant", tenant, "session", session, "type", typ)
		}
	}()
	return func() { _ = sub.Close() }, nil
}

/**
 * extensionMailParts pulls the type, body and source out of a published payload.
 *
 * `PublishMailboxEvent` sends an `easybus.MailboxMessage` — {id, type, payload, source} — so the
 * body is the `payload` field, NOT the whole object. Treating the whole object as the body would
 * put the envelope's own bookkeeping in front of the model as part of its message.
 *
 * A payload that does not have that shape is passed through as the body with type "event", which
 * is what the SDK itself defaults to when a caller names no type.
 */
func extensionMailParts(v any) (typ string, payload any, source string) {
	m, ok := v.(map[string]any)
	if !ok {
		return "event", v, ""
	}
	if t, ok := m["type"].(string); ok && t != "" {
		typ = t
	} else {
		typ = "event"
	}
	payload = m["payload"]
	if s, ok := m["source"].(string); ok {
		source = s
	}
	return typ, payload, source
}
