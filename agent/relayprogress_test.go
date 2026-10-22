package agent

import (
	"context"
	"testing"
	"time"
)

// captureEmitter records events for assertions.
type captureEmitter struct{ got []Event }

func (c *captureEmitter) Emit(_ context.Context, ev Event) error {
	c.got = append(c.got, ev)
	return nil
}

// TestRelayProgressCarriesMessageID is the regression test for live tool output
// that never appeared in the console.
//
// The console routes every stream event by its server-authored `message_id`:
//
//	const streamMsgId = () => params['message_id'] ...   // null => drop
//
// `tool-progress` was the one event that did not carry it, so every frame was
// dropped on arrival. The service side looked perfect — the frames were published
// to the durable stream in order — while the tool card stayed empty until the
// final result landed. From the outside that reads as "streaming is broken".
func TestRelayProgressCarriesMessageID(t *testing.T) {
	em := &captureEmitter{}

	// A source that emits two frames then closes.
	src := func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error) {
		ch := make(chan ToolProgressUpdate, 2)
		ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: "line1"}
		ch <- ToolProgressUpdate{CallID: callID, Phase: "running", Text: "line2"}
		close(ch)
		return ch, func() {}, nil
	}

	deps := Deps{
		Emit:         em,
		ToolProgress: src,
	}
	in := Input{Tenant: "t1", Session: "s1", RunID: "run-1"}

	relayProgress(context.Background(), deps, in, "call-9", "step-7")

	// The relay is asynchronous; wait for both frames.
	deadline := time.Now().Add(2 * time.Second)
	for len(em.got) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if len(em.got) != 2 {
		t.Fatalf("emitted %d progress frames, want 2", len(em.got))
	}
	for i, ev := range em.got {
		if ev.Name != "tool-progress" {
			t.Fatalf("event %d name = %q", i, ev.Name)
		}
		if got := asString(ev.Params["message_id"]); got != "step-7" {
			t.Errorf("event %d message_id = %q, want step-7 — the console groups by this and drops frames without it", i, got)
		}
		if got := asString(ev.Params["toolCallId"]); got != "call-9" {
			t.Errorf("event %d toolCallId = %q, want call-9", i, got)
		}
	}
}
