package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNoDeltaAfterTerminalFrame pins the ORDER of the frames a tool call emits.
//
// The relay used to be stopped by `defer stopRelay()`, which runs when execTool
// RETURNS — after the terminal frame had already been published. In that window the
// relay goroutine could still drain the progress channel and send deltas, and the
// client appended them to output it had just been given in full. The visible result
// was a tool card where every line appeared TWICE.
//
// The invariant is simple and is what makes the delta/terminal split work at all: once
// the terminal frame is out, nothing may follow it.
func TestNoDeltaAfterTerminalFrame(t *testing.T) {
	em := &captureEmitter{}

	// A source that keeps producing while the tool "runs", so the relay has work left
	// to do at the moment the tool returns — which is the race being pinned.
	stop := make(chan struct{})
	src := func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error) {
		ch := make(chan ToolProgressUpdate, 256)
		go func() {
			defer close(ch)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: fmt.Sprintf("line%d", i)}:
				}
			}
		}()
		return ch, func() { close(stop) }, nil
	}

	tools := &slowStubTools{
		delay: 20 * time.Millisecond,
		out:   ToolOutcome{Content: "done"},
	}
	deps := Deps{Emit: em, ToolProgress: src, Tools: tools, Locale: "en"}
	in := Input{Tenant: "t1", Session: "s1", RunID: "r1"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = execTool(context.Background(), deps, in, "tool-x", "call-1", "step-1", "{}")
	}()

	// Wait for a terminal frame to appear.
	deadline := time.Now().Add(3 * time.Second)
	var sawTerminal bool
	for time.Now().Before(deadline) {
		for _, ev := range em.snapshot() {
			if ev.Name != "tool-output-available" {
				continue
			}
			if ev.Params["preliminary"] != true {
				sawTerminal = true
			}
		}
		if sawTerminal {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	// Give a stray relay goroutine room to misbehave.
	time.Sleep(100 * time.Millisecond)

	if !sawTerminal {
		t.Fatal("no terminal frame was emitted")
	}

	got := em.snapshot()
	termIdx := -1
	for i, ev := range got {
		if ev.Name == "tool-output-available" && ev.Params["preliminary"] != true {
			termIdx = i
		}
	}
	for i := termIdx + 1; i < len(got); i++ {
		ev := got[i]
		if ev.Name != "tool-output-available" {
			continue
		}
		out, _ := ev.Params["output"].(map[string]any)
		if d := asString(out["delta"]); d != "" {
			t.Fatalf("delta published AFTER the terminal frame (frame %d): %q\n"+
				"the client appends deltas, so this line appears twice", i, d)
		}
	}

	// And the terminal frame must be the last word on the output.
	lastOut, _ := got[len(got)-1].Params["output"].(map[string]any)
	if !strings.Contains(asString(lastOut["content"]), "done") {
		t.Errorf("last frame content = %q, want it to carry the trimmed result",
			asString(lastOut["content"]))
	}
}

// slowStubTools returns a fixed outcome after a delay.
type slowStubTools struct {
	delay time.Duration
	out   ToolOutcome
}

func (s *slowStubTools) Execute(_ context.Context, tenant, session, callID, name, args string) (ToolOutcome, error) {
	time.Sleep(s.delay)
	return s.out, nil
}
