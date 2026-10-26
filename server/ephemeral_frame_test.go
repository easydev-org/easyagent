package server

import (
	"testing"

	"github.com/easydev-org/easyagent/agent"
)

// A streaming chunk must not be written to the durable events stream; its final form must.
//
// Measured on a real turn, `tool-output-available` chunks with `preliminary: true` were 95% of the
// bytes in EASYBUS_EVENTS — and every one is superseded by the final chunk plus `message-added`
// plus the message row the database stores. Keeping them means the stream's ceiling is set by how
// much a turn streams, and that a reconnecting client replays a half-rendered answer it must
// immediately discard.
func TestStreamingChunksAreNotPersisted(t *testing.T) {
	cases := []struct {
		name string
		ev   agent.Event
		want bool // true = ephemeral (not persisted)
	}{
		{
			name: "preliminary tool output",
			ev:   agent.Event{Name: "tool-output-available", Params: map[string]any{"preliminary": true}},
			want: true,
		},
		{
			name: "preliminary tool error",
			ev:   agent.Event{Name: "tool-output-error", Params: map[string]any{"preliminary": true}},
			want: true,
		},
		{
			// The LAST chunk of a streamed call is not preliminary, and it is the one a replay
			// should see. Dropping it would lose the result.
			name: "final tool output",
			ev:   agent.Event{Name: "tool-output-available", Params: map[string]any{"output": "done"}},
			want: false,
		},
		{
			name: "explicitly not preliminary",
			ev:   agent.Event{Name: "tool-output-available", Params: map[string]any{"preliminary": false}},
			want: false,
		},
		{
			name: "message-added",
			ev:   agent.Event{Name: "message-added", Params: map[string]any{"message_id": "m1"}},
			want: false,
		},
		{
			name: "status",
			ev:   agent.Event{Name: "status", Params: map[string]any{"type": "busy"}},
			want: false,
		},
		{
			name: "compacted",
			ev:   agent.Event{Name: "compacted", Params: map[string]any{"reason": "auto"}},
			want: false,
		},
		// The per-token deltas. An earlier version of the filter covered only `tool-output-*` and
		// let every one of these through, which is the largest volume of all on a turn that does
		// not call tools.
		{
			name: "text-delta",
			ev:   agent.Event{Name: "text-delta", Params: map[string]any{"text": "ok"}},
			want: true,
		},
		{
			name: "reasoning-delta",
			ev:   agent.Event{Name: "reasoning-delta", Params: map[string]any{"text": "thinking"}},
			want: true,
		},
		{
			name: "tool-input-delta",
			ev:   agent.Event{Name: "tool-input-delta", Params: map[string]any{"delta": "{"}},
			want: true,
		},
		// Structural frames stay: a replay needs the step boundaries to assemble what it gets.
		{
			name: "start-step",
			ev:   agent.Event{Name: "start-step", Params: map[string]any{"message_id": "m"}},
			want: false,
		},
		{
			name: "finish-step",
			ev:   agent.Event{Name: "finish-step", Params: map[string]any{"message_id": "m"}},
			want: false,
		},
		{
			name: "turn-complete",
			ev:   agent.Event{Name: "turn-complete", Params: map[string]any{}},
			want: false,
		},
	}
	for _, c := range cases {
		if got := isEphemeralFrame(c.ev); got != c.want {
			t.Errorf("%s: isEphemeralFrame = %v, want %v", c.name, got, c.want)
		}
	}
}

// The frame published to the backplane must still reach LOCAL subscribers: a client watching live
// has to see its stream. Only the durable copy is skipped.
func TestEphemeralFrameStillReachesLocalSubscribers(t *testing.T) {
	h := NewEventHub()
	ch, cancel := h.Subscribe("t1", "s1")
	defer cancel()

	h.Publish("t1", "s1", agent.Event{
		Name:   "tool-output-available",
		Params: map[string]any{"preliminary": true, "output": "chunk"},
	})

	select {
	case ev := <-ch:
		if ev.Name != "tool-output-available" {
			t.Fatalf("got %q, want the live chunk", ev.Name)
		}
	default:
		t.Fatal("an ephemeral frame was withheld from a LIVE subscriber; only the durable copy " +
			"should be skipped, otherwise streaming stops working")
	}
}
