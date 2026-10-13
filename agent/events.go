// Package agent is the turn loop: the SDK-core layer that drives one user
// prompt through model steps, executes tools, and publishes the client-facing
// event stream frozen in EVENTS.md.
package agent

import "context"

// Event is one frame published on the session event stream:
// { event, params } (the durable bus adds the envelope + `eid`).
type Event struct {
	Name   string
	Params map[string]any
}

// Emitter publishes events. Implementations add the durable envelope, stamp
// `run_id`, and persist `eid`. The turn loop only decides WHAT to emit.
type Emitter interface {
	Emit(ctx context.Context, ev Event) error
}

// EmitterFunc adapts a function to Emitter (tests, simple callers).
type EmitterFunc func(ctx context.Context, ev Event) error

func (f EmitterFunc) Emit(ctx context.Context, ev Event) error { return f(ctx, ev) }
