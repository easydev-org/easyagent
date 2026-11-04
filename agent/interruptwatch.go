package agent

import (
	"context"
)

/**
 * NewMailboxInterruptWatch builds the source `InterruptWatch` needs from the queue itself.
 *
 * WHY THE QUEUE AND NOT A BUS SUBJECT.
 *
 * There IS an `easybus.<tenant>.ctl.interrupt.<extId>` subject, but it is addressed to an EXTENSION —
 * it tells a long-running tool to stop — and the agent never subscribes to it. Routing a session
 * cancel through it would mean inventing an extension id for a conversation.
 *
 * The interrupt that matters here is the one `Server.Interrupt` already writes: a durable
 * `mailbox.Interrupt` row. It lands in the same bucket the turn drains, so watching that bucket is
 * how a replica holding the turn learns about a cancel produced anywhere.
 *
 * WHAT IT REPORTS: the type of every NEWLY WRITTEN entry, so the runtime can filter for `interrupt`
 * itself. It does not decide what is interesting — that belongs to the caller that knows whether a
 * turn is running.
 *
 * CONSUMED ROWS ARE SKIPPED. A drain marks a message Consumed, and reporting that would tell the
 * runtime an interrupt had just arrived when in fact it had just been handled.
 */
func NewMailboxInterruptWatch(q MailboxQueue) func(ctx context.Context, tenant, session string) (<-chan string, func(), error) {
	if q == nil {
		return nil
	}
	return func(ctx context.Context, tenant, session string) (<-chan string, func(), error) {
		entries, stop, err := q.Watch(ctx, tenant, session)
		if err != nil {
			return nil, nil, err
		}
		out := make(chan string, 8)
		go func() {
			defer close(out)
			for m := range entries {
				// ONLY PENDING. A consumed row is one somebody already handled, and the snapshot
				// a watch replays at start includes those — reporting one as a fresh interrupt would
				// abort a turn the user had just asked for.
				if !m.IsPending() {
					continue
				}
				select {
				case out <- string(m.Type):
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, stop, nil
	}
}
