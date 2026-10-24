package bus

import (
	"context"
	"testing"
	"time"
)

// PublishMailbox must do BOTH: write the durable row AND signal.
//
// The two halves are not interchangeable, and getting one without the other is invisible:
//
//   - enqueue only  → the row sits unread until something nudges a runtime, so a session that is
//     idle never wakes. This is what the bundled tools did.
//   - signal only   → `Wake` runs, drains an empty table, and returns. A consumer ACKS the NATS
//     message and everything reports success while no turn ever runs. This is what the channel
//     distributor did, and it took a day to find because every observable signal was green.
func TestPublishMailboxEnqueuesAndSignals(t *testing.T) {
	q := newMemQueue()
	b := startBus(t).WithMailboxQueue(q)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got := make(chan MailboxMessage, 1)
	stop, err := b.ConsumeMailbox(ctx, func(m MailboxMessage) error {
		select {
		case got <- m:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	time.Sleep(300 * time.Millisecond) // let the consumer bind

	if err := b.PublishMailbox(ctx, "t1", "s1", "trigger", map[string]any{"text": "hi"}, "channel:test"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Half one: the durable row. Without it Wake finds nothing to run.
	if n := q.count(); n != 1 {
		t.Errorf("queue holds %d rows, want 1: the message was signalled but never enqueued, "+
			"so the runtime would drain an empty table and the session would not wake", n)
	}

	// Half two: the signal. Without it the row waits for something else to nudge a runtime.
	select {
	case m := <-got:
		if m.Tenant != "t1" || m.SessionName != "s1" || m.Type != "trigger" {
			t.Errorf("message = %+v", m)
		}
	case <-ctx.Done():
		t.Fatal("no signal: the message was enqueued but nothing woke the runtime")
	}
}

// A bus with no queue must REFUSE rather than signal-only.
func TestPublishMailboxRefusesWithoutQueue(t *testing.T) {
	b := startBus(t)
	err := b.PublishMailbox(context.Background(), "t1", "s1", "trigger", nil, "test")
	if err == nil {
		t.Fatal("a bus with no queue signalled anyway; that combination delivers nothing and " +
			"reports success")
	}
}
