package agent

import (
	"context"

	"github.com/easydev-org/easybus-sdk/mailbox"
)

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
	 * the turn — an API prompt, a spawned worker's report, an extension's completion notice.
	 */
	Enqueue(ctx context.Context, tenant, session, id string, typ mailbox.Type, payload any, source string) (string, error)
	// Pending reports what is waiting without taking it.
	Pending(ctx context.Context, tenant, session string) ([]mailbox.Message, error)
	// SessionsWithPending lists conversations with work waiting, narrowed to the given message types.
	SessionsWithPending(ctx context.Context, types []mailbox.Type) ([]struct{ Tenant, SessionName string }, error)
	// HasInterrupt reports whether an interrupt is waiting.
	HasInterrupt(ctx context.Context, tenant, session string) (bool, error)
	/**
	 * Watch streams this session's queue entries while a turn is running.
	 *
	 * IT IS NOT `Drain` IN A LOOP, and that distinction is the whole point: a cancel has to reach a
	 * turn that is ALREADY running. Draining answers "what is waiting now" — one wake too late for an
	 * interrupt, because by the time the next drain happens the turn it was meant to stop has
	 * finished. This is what makes `Interrupt` work across replicas.
	 *
	 * A NIL IMPLEMENTATION IS VALID: a deployment without a watchable queue simply cannot be
	 * interrupted from another replica, which is what it could not do before either.
	 */
	Watch(ctx context.Context, tenant, session string) (<-chan mailbox.Message, func(), error)
}
