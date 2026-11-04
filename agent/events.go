// Package agent is the turn loop: the SDK-core layer that drives one user
// prompt through model steps, executes tools, and publishes the client-facing
// event stream frozen in EVENTS.md.
package agent

import "context"

// Event is one frame published on the session event stream:
// { event, params, eid, seq }.
//
// Eid AND Seq ARE ASSIGNED BY THE HUB, ONCE, when the event enters the stream — not when a client is
// handed it. That distinction is the whole reason they exist: a client resumes from `sinceSeq` and
// dedupes on `eid`, so both must identify the SAME frame in the durable log and on every live
// subscriber. They used to be stamped at send time, which gave each subscriber a different number
// for one event and left the live path with no id at all — so the anchor a client echoed back
// pointed at a sequence the durable log had never used.
type Event struct {
	Name   string
	Params map[string]any
	// Eid is the frame's dedup key. Assigned by the hub; "" on an event that was never published
	// (a test constructing one directly).
	Eid string
	// Seq is the frame's position in this replica's stream. Assigned by the hub.
	Seq int64
}

// Emitter publishes events. Implementations add the durable envelope, stamp
// `run_id`, and persist `eid`. The turn loop only decides WHAT to emit.
type Emitter interface {
	Emit(ctx context.Context, ev Event) error
}

// EmitterFunc adapts a function to Emitter (tests, simple callers).
type EmitterFunc func(ctx context.Context, ev Event) error

func (f EmitterFunc) Emit(ctx context.Context, ev Event) error { return f(ctx, ev) }
