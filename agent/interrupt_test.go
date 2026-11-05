package agent

import (
	"context"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
)

/**
 * AN INTERRUPT QUEUED WHILE THE SESSION WAS IDLE MUST NOT KILL THE NEXT PROMPT.
 *
 * `Server.Interrupt` writes a durable interrupt even when nothing is running, and drain is
 * oldest-first — so the stale interrupt is folded FIRST and cancels the busy period before the
 * prompt drained beside it is ever dispatched. The prompt is then accepted, acked, written to the
 * transcript, and no turn runs: from the console that is a message that was sent and silently
 * ignored.
 *
 * The interrupt here predates the trigger, which is exactly the idle-Stop-then-type sequence.
 */
func TestStaleInterruptDoesNotDropTheNextPrompt(t *testing.T) {
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	if err := db.CreateSession("t1", sess); err != nil {
		t.Fatal(err)
	}
	rt := NewRuntime(RuntimeDeps{DB: db, Mailbox: mailbox.NewMemory(), Lease: NewMemoryLease()})

	base := time.Now().UnixMilli()
	items := []mailbox.Message{
		{ID: "old-stop", Type: mailbox.Interrupt, CreatedAt: base},
		{ID: "prompt", Type: mailbox.Trigger, CreatedAt: base + 5000},
	}

	// Wake registers the busy period BEFORE draining, and that is what makes an interrupt able to
	// cancel the turn. Reproduce that, then let handleBatch dispatch the batch.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.setActive("t1", sess.ID, cancel)
	defer rt.clearActive("t1", sess.ID)

	_ = rt.handleBatch(ctx, "t1", sess.ID, items)

	if rt.takeCancelReason("t1", sess.ID) != "" {
		t.Fatal("a stale interrupt cancelled the busy period, so the prompt beside it would be dropped")
	}
	if ctx.Err() != nil {
		t.Fatal("a stale interrupt cancelled the busy period's context")
	}
}

/**
 * A REAL INTERRUPT — one that arrives AFTER the prompt — must still cancel.
 *
 * This is the pair the fix has to keep working: the user typed, the turn started, and THEN they
 * pressed Stop. Ordering alone cannot tell this from the stale case; the timestamp can.
 */
func TestInterruptAfterPromptStillCancels(t *testing.T) {
	db := newRuntimeDB(t)
	sess := &store.SessionInput{Name: "s1", Model: "gw/m"}
	if err := db.CreateSession("t1", sess); err != nil {
		t.Fatal(err)
	}
	rt := NewRuntime(RuntimeDeps{DB: db, Mailbox: mailbox.NewMemory(), Lease: NewMemoryLease()})

	base := time.Now().UnixMilli()
	items := []mailbox.Message{
		{ID: "prompt", Type: mailbox.Trigger, CreatedAt: base},
		{ID: "stop", Type: mailbox.Interrupt, CreatedAt: base + 5000},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.setActive("t1", sess.ID, cancel)
	defer rt.clearActive("t1", sess.ID)

	_ = rt.handleBatch(ctx, "t1", sess.ID, items)

	if ctx.Err() == nil {
		t.Fatal("an interrupt issued after the prompt did not cancel the turn")
	}
	if got := rt.takeCancelReason("t1", sess.ID); got != "interrupted" {
		t.Fatalf("cancel reason = %q, want interrupted", got)
	}
}

/**
 * A PENDING INTERRUPT FROM EARLIER MUST NOT BE DELIVERED AS A FRESH CANCEL.
 *
 * The runtime starts a watch and THEN drains. If the watch reported the bucket's snapshot, a pending
 * interrupt left behind by an idle "stop" would arrive as a live cancel and abort the turn the user
 * had just asked for — the very bug the drain-side ordering fix removes, reintroduced through a
 * different door.
 *
 * Found in the cluster, not in tests: the first version of this test started the watch before the
 * drain, so the row was already consumed and the snapshot had nothing to report.
 */
func TestInterruptWatchIgnoresWhatAlreadyExisted(t *testing.T) {
	ctx := context.Background()
	q := mailbox.NewMemory()

	// The idle-Stop case: an interrupt is queued and nothing is running.
	if _, err := q.Enqueue(ctx, "t1", "s1", "old-stop", mailbox.Interrupt, nil, "user"); err != nil {
		t.Fatal(err)
	}

	watch := NewMailboxInterruptWatch(q)
	ch, stop, err := watch(ctx, "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	select {
	case got := <-ch:
		t.Fatalf("an interrupt queued before the watch started was reported as fresh: %q", got)
	case <-time.After(150 * time.Millisecond):
	}

	// A genuinely live one must still arrive.
	if _, err := q.Enqueue(ctx, "t1", "s1", "live", mailbox.Interrupt, nil, "user"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != string(mailbox.Interrupt) {
			t.Fatalf("reported %q, want interrupt", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a live interrupt never reached the watch")
	}
}

/**
 * The interrupt watch must report a PENDING interrupt and skip anything already handled.
 *
 * The watch replays the bucket's snapshot at start, which is full of consumed rows from earlier
 * turns — reporting one as a fresh cancel would abort a turn the user had just asked for.
 */
func TestInterruptWatchReportsOnlyPendingInterrupts(t *testing.T) {
	ctx := context.Background()
	q := mailbox.NewMemory()

	// One already-consumed interrupt, as an earlier turn would leave behind.
	if _, err := q.Enqueue(ctx, "t1", "s1", "done", mailbox.Interrupt, nil, "user"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Drain(ctx, "t1", "s1"); err != nil {
		t.Fatal(err)
	}

	watch := NewMailboxInterruptWatch(q)
	if watch == nil {
		t.Fatal("watch is nil")
	}
	ch, stop, err := watch(ctx, "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	// A live interrupt must arrive.
	if _, err := q.Enqueue(ctx, "t1", "s1", "live", mailbox.Interrupt, nil, "user"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got != string(mailbox.Interrupt) {
			t.Fatalf("reported %q, want interrupt", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a live interrupt never reached the watch")
	}

	// The consumed one must NOT have been reported. Give the snapshot a moment to have been
	// skipped; a wrongly-reported row would already be buffered.
	select {
	case got := <-ch:
		t.Fatalf("a consumed interrupt was reported as fresh: %q", got)
	case <-time.After(150 * time.Millisecond):
	}
}
