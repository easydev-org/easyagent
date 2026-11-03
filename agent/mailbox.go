package agent

import (
	"context"
	"encoding/json"

	"github.com/easydev-org/easybus-sdk/mailbox"
)

/**
 * decodeWakePayload reads a mailbox payload into a wake payload.
 *
 * IT TOLERATES BOTH SHAPES, because the queue holds whatever the producer wrote: an object from the
 * extension path, and historically a JSON string from the row-based table. Encoding to JSON and decoding
 * back handles both without the caller having to know which it got — and a producer that sends a string
 * containing JSON is not making a mistake, it is using the shape the old storage required.
 */
func decodeWakePayload(v any, p *WakePayload) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, p)
}

/**
 * MailboxQueue is the runtime's view of the delivery queue.
 *
 * FIVE METHODS, WHICH IS WHAT A TURN ACTUALLY ASKS OF IT: take the pending batch, find the sessions that
 * have work after a restart, and check for an interrupt before a turn starts. The queue offers more — a
 * page, an enqueue — but those belong to producers, not to the consumer this file is about.
 *
 * DECLARED HERE, NOT IMPORTED FROM THE QUEUE, so the runtime depends on the shape it needs rather than
 * on the implementation. A test supplies a slice; production supplies the KV queue.
 */
type MailboxQueue interface {
	// Drain takes every pending message for one session, oldest first, and marks each consumed.
	Drain(ctx context.Context, tenant, session string) ([]mailbox.Message, error)
	/**
	 * Enqueue adds a message.
	 *
	 * THE RUNTIME WRITES TO ITS OWN QUEUE in exactly one place: the resumer, which nudges a conversation
	 * whose turn ended for a reason worth retrying. Everything else that enqueues is a producer outside
	 * the turn — an API prompt, a subsession hand-off, an extension's completion notice.
	 */
	Enqueue(ctx context.Context, tenant, session, id string, typ mailbox.Type, payload any, source string) (string, error)
	// Pending reports what is waiting without taking it.
	Pending(ctx context.Context, tenant, session string) ([]mailbox.Message, error)
	// SessionsWithPending lists conversations with work waiting, narrowed to the given message types.
	SessionsWithPending(ctx context.Context, types []mailbox.Type) ([]struct{ Tenant, SessionName string }, error)
	// HasInterrupt reports whether an interrupt is waiting.
	HasInterrupt(ctx context.Context, tenant, session string) (bool, error)
}
