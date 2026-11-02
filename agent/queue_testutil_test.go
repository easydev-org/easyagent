package agent

import (
	"context"
	"testing"

	"github.com/easydev-org/easybus-sdk/mailbox"
)

/**
 * enqueue is the ONE WAY A TEST PUTS A MESSAGE WHERE THE TURN WILL FIND IT.
 *
 * IT EXISTS BECAUSE THE TWO USED TO BE THE SAME OBJECT. A test called `db.Enqueue` and the runtime read
 * that table, so "queue a message" was one call. Now the queue is its own store, and a test that writes
 * to the database instead puts its message nowhere — the failure reads as "the turn did not run", which
 * is indistinguishable from a real bug and sends the reader hunting in the wrong place.
 *
 * A TEST THAT WANTS A QUEUED MESSAGE ALSO HAS TO PASS THIS QUEUE TO NewRuntime. That coupling is the
 * point: there is no second way to be right.
 */
func enqueue(t *testing.T, q *mailbox.Memory, tenant, session, typ string, payload any) {
	t.Helper()
	if _, err := q.Enqueue(context.Background(), tenant, session, "", typ, payload, "user"); err != nil {
		t.Fatal(err)
	}
}

/**
 * hasPending reports whether the session still has undelivered work.
 *
 * IT ASKS THE QUEUE. The tests that assert this used to call `db.HasPending`, which was right while the
 * queue was a table and silently wrong afterwards: it queried a table nothing wrote to and answered
 * "nothing pending", which is the answer the negative cases want and the positive ones do not.
 */
func hasPending(t *testing.T, q *mailbox.Memory, tenant, session string) bool {
	t.Helper()
	pending, err := q.Pending(context.Background(), tenant, session)
	if err != nil {
		t.Fatal(err)
	}
	return len(pending) > 0
}
