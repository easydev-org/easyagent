package agent

import (
	"context"
	"sync"
)

// resultStream accumulates a tool's output and publishes it as
// `tool-output-available` SNAPSHOTS.
//
// WHY snapshots rather than deltas: this follows the AI SDK's UI message stream,
// where streaming tool output is expressed as repeated
// `tool-output-available` frames with `preliminary: true`, each carrying the
// output SO FAR, followed by one frame without the flag. Deltas would be smaller
// but a single dropped frame would corrupt the result permanently; a snapshot is
// idempotent, so the last frame that arrives is always correct.
//
// The accumulator also owns the interrupt story: whatever arrived before a timeout
// or cancellation is still the tool's output, and it is already on the wire.
type resultStream struct {
	mu    sync.Mutex
	lines []string
	// state is the terminal outcome once known ("ok" | "timeout" | "cancelled"),
	// empty while running. It rides inside `output` so a client can distinguish
	// "finished" from "stopped early" without parsing an error string.
	state string
	// closed stops further appends: a frame arriving after the terminal one would
	// resurrect a tool card the user has already seen finish.
	closed bool
}

func newResultStream() *resultStream { return &resultStream{} }

// Append adds one line (which may itself contain newlines) and returns the new
// snapshot. The caller decides when to publish it.
func (r *resultStream) Append(text string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || text == "" {
		return r.snapshotLocked()
	}
	for _, line := range splitLines(text) {
		if line != "" {
			r.lines = append(r.lines, line)
		}
	}
	return r.snapshotLocked()
}

// Snapshot returns the output accumulated so far.
func (r *resultStream) Snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

// Text joins the accumulated lines the way a result is rendered.
func (r *resultStream) Text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return joinLines(r.lines)
}

// Close marks the stream terminal and records why. Called from every exit of the
// tool call — success, error, timeout, cancellation — so a card never stays
// "running" after the turn has moved on.
func (r *resultStream) Close(state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.state = state
}

func (r *resultStream) snapshotLocked() []string {
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

// splitLines normalises a chunk into non-empty lines.
//
// A chunk is an arbitrary byte slice, not a line: buildkit and job output both
// arrive in pieces that can hold many lines or half of one. Splitting here keeps
// the snapshot readable instead of gluing unrelated records together.
func splitLines(text string) []string {
	out := make([]string, 0, 8)
	start := 0
	for i := 0; i < len(text); i++ {
		if text[i] != '\n' {
			continue
		}
		line := trimCR(text[start:i])
		out = append(out, line)
		start = i + 1
	}
	if start < len(text) {
		out = append(out, trimCR(text[start:]))
	}
	return out
}

func trimCR(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func joinLines(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	n := 0
	for _, l := range lines {
		n += len(l) + 1
	}
	buf := make([]byte, 0, n)
	for i, l := range lines {
		if i > 0 {
			buf = append(buf, '\n')
		}
		buf = append(buf, l...)
	}
	return string(buf)
}

// emitOutput publishes one `tool-output-available` frame.
//
// `preliminary` is what tells the client whether this is the final answer or a
// snapshot of work in progress — the AI SDK's own mechanism, so a client that
// understands its stream already knows how to render this.
func emitOutput(ctx context.Context, deps Deps, in Input, stepID, callID, toolName string, r *resultStream, meta map[string]any) {
	if deps.Emit == nil {
		return
	}
	r.mu.Lock()
	lines := make([]string, len(r.lines))
	copy(lines, r.lines)
	state := r.state
	closed := r.closed
	r.mu.Unlock()

	output := map[string]any{"content": joinLines(lines)}
	if state != "" {
		output["state"] = state
	}
	for k, v := range meta {
		output[k] = v
	}
	params := map[string]any{
		"toolCallId": callID,
		"toolName":   toolName,
		"output":     output,
		"message_id": stepID,
		"run_id":     in.RunID,
	}
	if !closed {
		params["preliminary"] = true
	}
	_ = emitBg(deps.Emit, "tool-output-available", params)
}
