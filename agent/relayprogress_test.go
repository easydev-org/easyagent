package agent

import (
	"context"
	"sync"
	"testing"
	"time"
)

// captureEmitter records events for assertions.
//
// Guarded by a mutex: the relay emits from its own goroutine while the test reads,
// and `-race` rightly flags the unsynchronised pair.
type captureEmitter struct {
	mu  sync.Mutex
	got []Event
}

func (c *captureEmitter) Emit(_ context.Context, ev Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, ev)
	return nil
}

func (c *captureEmitter) snapshot() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Event, len(c.got))
	copy(out, c.got)
	return out
}

// TestRelayPublishesOutputSnapshots is the regression test for tool output the
// console could not see, and for the shape it now arrives in.
//
// Two things had to be true and neither was:
//
//   - the frame must carry `message_id`, because the console routes every stream
//     event by it and silently drops anything without one;
//   - the content must be a SNAPSHOT of the output so far, not a delta, because a
//     dropped delta corrupts the result permanently while a dropped snapshot does
//     not. The AI SDK expresses this as repeated `tool-output-available` frames
//     with `preliminary: true`, followed by one without the flag.
func TestRelayPublishesOutputSnapshots(t *testing.T) {
	em := &captureEmitter{}

	src := func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error) {
		ch := make(chan ToolProgressUpdate, 2)
		ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: "line1"}
		ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: "line2"}
		close(ch)
		return ch, func() {}, nil
	}

	deps := Deps{Emit: em, ToolProgress: src}
	in := Input{Tenant: "t1", Session: "s1", RunID: "run-1"}
	stream := newResultStream()

	stop := relayInto(context.Background(), deps, in, "call-9", "step-7", "sandbox-job-start", stream)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-time.After(10 * time.Millisecond):
		}
		if len(em.snapshot()) >= 2 {
			break
		}
	}
	stop()

	got := em.snapshot()
	if len(got) < 2 {
		t.Fatalf("emitted %d frames, want at least 2", len(got))
	}
	for i, ev := range got {
		if ev.Name != "tool-output-available" {
			t.Fatalf("frame %d name = %q, want tool-output-available", i, ev.Name)
		}
		if asString(ev.Params["message_id"]) != "step-7" {
			t.Errorf("frame %d has no message_id; the console drops such frames", i)
		}
		if asString(ev.Params["toolCallId"]) != "call-9" {
			t.Errorf("frame %d toolCallId = %q", i, asString(ev.Params["toolCallId"]))
		}
		// Relayed frames are by definition provisional.
		if ev.Params["preliminary"] != true {
			t.Errorf("frame %d is not marked preliminary", i)
		}
	}

	// The last snapshot holds BOTH lines: snapshots accumulate, they do not
	// replace the content with the newest fragment.
	lastOut, _ := got[len(got)-1].Params["output"].(map[string]any)
	content := asString(lastOut["content"])
	if content != "line1\nline2" {
		t.Errorf("final snapshot content = %q, want both lines", content)
	}
}
