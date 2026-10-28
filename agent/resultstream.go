package agent

import (
	"context"
	"fmt"
	"sync"
)

// Result size policy.
//
// A tool's FULL output is visible while it streams: that is the point of streaming,
// and it costs nothing to show lines already on the wire. Once the call is OVER the
// output is stored, sent to the model, and rendered again from history — three places
// where a 100k-line build log is pure cost. So a finished result is trimmed to its
// first and last halves.
//
// BOTH halves, not just the tail: a build's errors are at the top and its outcome at
// the bottom, so keeping one end loses half the diagnosis. The omitted middle is
// gone — it was visible live and is not worth storing.
const (
	// resultKeepLines is the total a finished result may keep (250 + 250).
	resultKeepLines = 500
	// resultHalfLines is kept from each end.
	resultHalfLines = resultKeepLines / 2
)

// trimLinesForResult applies the policy: at or under the limit the lines are returned
// unchanged; above it, the head and tail survive with a marker between them.
func trimLinesForResult(lines []string, locale string) []string {
	if len(lines) <= resultKeepLines {
		return lines
	}
	head := lines[:resultHalfLines]
	tail := lines[len(lines)-resultHalfLines:]
	omitted := len(lines) - resultKeepLines

	out := make([]string, 0, resultKeepLines+1)
	out = append(out, head...)
	out = append(out, omittedMarker(omitted, locale))
	out = append(out, tail...)
	return out
}

// omittedMarker is the line that stands in for the dropped middle.
//
// It states the COUNT, because "some output was omitted" tells a reader nothing about
// whether they are seeing a rounding error or the middle of a stack trace.
func omittedMarker(omitted int, locale string) string {
	if primaryOf(NormalizeLocale(locale)) == "zh" {
		return fmt.Sprintf("[… 中间 %d 行输出已省略 …]", omitted)
	}
	return fmt.Sprintf("[… %d lines of tool output omitted …]", omitted)
}

// Live-output batching.
//
// WHY batching: the naive shape is one `tool-output-available` frame per line, each
// carrying the output SO FAR. That is O(n²) bytes for n lines — a 50k-line build sends
// gigabytes — and it is why streaming a large job felt heavy.
//
// Frames therefore carry only the NEW lines (`delta`), and the client appends them. The
// terminal frame still carries the authoritative `content`, so a dropped intermediate
// frame costs at most a gap in the live view; the result on completion is complete.
//
// The batch bounds exist because the relay can be handed a line per millisecond:
//
//	liveFlushLines — send once this many lines have accumulated
//	liveFlushChars — or once this many bytes have, whichever comes first
//
// and a timer flushes a partial batch so a slow trickle still appears promptly.
const (
	liveFlushLines = 64
	liveFlushChars = 8 << 10 // 8 KiB
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
	// pending holds lines appended since the last published frame. Publishing only
	// these keeps the live stream O(n) in bytes rather than O(n²).
	pending     []string
	pendingChar int
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
			r.pending = append(r.pending, line)
			r.pendingChar += len(line) + 1
		}
	}
	return r.snapshotLocked()
}

// TakePending returns the lines appended since the last call, and resets the batch.
//
// This is what a live frame carries: only what is NEW. The alternative — the whole
// output each time — is correct but quadratic, and a large job made it obvious.
func (r *resultStream) TakePending() (lines []string, chars int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return nil, 0
	}
	lines = r.pending
	chars = r.pendingChar
	r.pending = nil
	r.pendingChar = 0
	return lines, chars
}

// PendingFull reports whether the batch has reached a flush bound.
func (r *resultStream) PendingFull() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) >= liveFlushLines || r.pendingChar >= liveFlushChars
}

// SetText REPLACES the accumulated output with text.
//
// Used when the tool returns its result: the relayed lines were only a live preview of
// that same text, so extending the stream with it would double the output — and the
// trim would then keep the head of one copy and the tail of the other.
//
// Any pending (unsent) lines are dropped with the rest: they are preview material, and
// the terminal frame carries the authoritative text anyway.
func (r *resultStream) SetText(text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	lines := splitLines(text)
	out := lines[:0]
	for _, l := range lines {
		if l != "" {
			out = append(out, l)
		}
	}
	r.lines = out
	r.pending = nil
	r.pendingChar = 0
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

// TrimmedText is the FINISHED result: the same lines, reduced to the policy above.
//
// Called only after the tool has finished. While it runs, every line is published, so
// the trim is a storage and context decision, not a display one.
func (r *resultStream) TrimmedText(locale string) string {
	r.mu.Lock()
	lines := make([]string, len(r.lines))
	copy(lines, r.lines)
	r.mu.Unlock()
	return joinLines(trimLinesForResult(lines, locale))
}

// TrimToLimit reduces the stream the policy above describes and returns the result.
//
// The stream is trimmed IN PLACE rather than only in the returned value because the
// terminal snapshot is emitted from the stream's own lines — trimming a copy would
// leave the frame that the console persists holding the whole output.
//
// Idempotent: trimming a stream already within the limit changes nothing, so a second
// call (a retry, a duplicate close) is harmless.
func (r *resultStream) TrimToLimit(locale string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = trimLinesForResult(r.lines, locale)
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
// Two shapes, told apart by `preliminary`:
//
//   - PRELIMINARY carries `output.delta` — only the lines added since the previous
//     frame. The client appends them. This is what makes a live stream O(n) instead of
//     O(n²): sending the whole output every time is correct, but a 50k-line build would
//     send gigabytes.
//
//   - TERMINAL carries `output.content` — the complete (and size-limited) result. It is
//     authoritative: a client that lost preliminary frames, or reconnected midway, ends
//     up with exactly the right output. That redundancy is deliberate and costs one
//     frame.
func emitOutput(ctx context.Context, deps Deps, in Input, stepID, callID, toolName string, r *resultStream, meta map[string]any) {
	if deps.Emit == nil {
		return
	}
	r.mu.Lock()
	state := r.state
	closed := r.closed
	var lines []string
	if closed {
		// Terminal: the full (trimmed) output is the answer.
		lines = make([]string, len(r.lines))
		copy(lines, r.lines)
	}
	r.mu.Unlock()

	var output map[string]any
	if closed {
		output = map[string]any{"content": joinLines(lines)}
	} else {
		delta, _ := r.TakePending()
		if len(delta) == 0 && state == "" {
			// Nothing new to say. A frame with no content and no state would only make
			// the client redraw.
			return
		}
		output = map[string]any{"delta": joinLines(delta)}
	}
	if state != "" {
		output["state"] = state
	}
	for k, v := range meta {
		output[k] = v
	}
	params := map[string]any{
		"toolCallId": callID,
		// The same identifier under the other name a client may look for. A card is created from
		// `tool-input-start` (which carries `id`) and updated by this frame (which carries
		// `toolCallId`); if the two handlers read different keys and only one is present, the update
		// lands on a DIFFERENT card — the arguments stay on one and the output appears on another.
		"id":         callID,
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
