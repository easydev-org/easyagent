package agent

import (
	"context"
	"fmt"
	"strings"
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

// TestRelayPublishesOutputDeltas is the regression test for tool output the console
// could not see, and for the shape it now arrives in.
//
// Three things have to hold:
//
//   - the frame carries `message_id`, because the console routes every stream event by
//     it and silently drops anything without one;
//   - a LIVE frame carries only the NEW lines (`output.delta`). Sending the whole
//     output every time is O(n²) in bytes — a 50k-line build sent gigabytes — which is
//     why the relay batches;
//   - the TERMINAL frame carries the complete `output.content`, so a client that
//     missed intermediate frames still converges on the real result.
func TestRelayPublishesOutputDeltas(t *testing.T) {
	em := &captureEmitter{}

	// Exactly one full batch. A partial batch is flushed by the ticker, so using one
	// would make this test timing-dependent; the ticker's own behaviour is separate.
	n := liveFlushLines
	src := func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error) {
		ch := make(chan ToolProgressUpdate, n)
		for i := 0; i < n; i++ {
			ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: fmt.Sprintf("line%d", i+1)}
		}
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
		if len(em.snapshot()) >= 1 {
			break
		}
	}
	stop()

	got := em.snapshot()
	if len(got) == 0 {
		t.Fatal("emitted no frames")
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
		out, _ := ev.Params["output"].(map[string]any)
		if _, ok := out["delta"]; !ok {
			t.Errorf("frame %d carries no delta; live frames must send only the new lines", i)
		}
		if _, ok := out["content"]; ok {
			t.Errorf("frame %d carries full content; that is O(n^2) and belongs only on the terminal frame", i)
		}
	}

	// The deltas, concatenated, must add up to the whole output — that is what makes
	// the live view correct without ever resending anything.
	var all []string
	for _, ev := range got {
		out, _ := ev.Params["output"].(map[string]any)
		if d := asString(out["delta"]); d != "" {
			all = append(all, d)
		}
	}
	joined := strings.Join(all, "\n")
	if !strings.Contains(joined, "line1") || !strings.Contains(joined, fmt.Sprintf("line%d", n)) {
		t.Errorf("deltas do not cover the output: %q", joined)
	}
	if len(all) != 1 {
		t.Errorf("expected ONE batched frame for %d lines, got %d", n, len(all))
	}
}
