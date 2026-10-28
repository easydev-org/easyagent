package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// ToolExecutor runs one tool call. It is the seam between the turn loop and
// the extension bus / bundled tools. args is the raw JSON argument string as
// the model emitted it (the executor validates it).
type ToolExecutor interface {
	// Execute runs one tool. callID is the MODEL's tool-call id, and it must reach
	// the executor: an out-of-process tool addresses its progress at
	// <tenant>.tool.progress.<callID>, and the turn subscribes with the id it got
	// from the model. If the executor invents its own id, the two never meet and
	// every progress frame is published to a subject nobody reads — which is
	// indistinguishable, from the outside, from "the tool reported nothing".
	Execute(ctx context.Context, tenant, session, callID, name, args string) (ToolOutcome, error)
}

// ToolOutcome is a tool result.
type ToolOutcome struct {
	// Content is the natural-language text fed back to the model.
	Content string
	// Metadata is opaque structured data carried through verbatim (surfaced as
	// the `data` param and, when it has a change_id, the `change_id` param).
	Metadata any
}

// ToolProgressUpdate is one in-flight report from a running tool call.
//
// Deliberately a plain struct rather than the protocol type: the turn only needs
// "what to show", and keeping it decoded here avoids every caller of turn.go
// depending on the wire types.
type ToolProgressUpdate struct {
	CallID   string
	Phase    string
	Text     string
	Progress float64
	Metadata any
}

// Deps are the turn loop's collaborators.
type Deps struct {
	Client   *llm.Client
	Retry    llm.RetryPolicy
	Tools    ToolExecutor
	Emit     Emitter
	MaxTurns int
	// ToolProgress, when set, streams in-flight updates for one tool call.
	//
	// These updates are relayed to the session's durable event stream so a client
	// that reconnects can still see everything a long job produced. They are NOT
	// added to `messages`: the model only ever sees the tool's final result. That
	// separation is what makes live output affordable — no tokens are spent on it.
	//
	// nil disables relaying (the tool still runs; only the live view is missing).
	ToolProgress func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error)
	// Locale selects the language of text the AGENT generates for the model and the
	// transcript — currently only the "output omitted" marker inside a trimmed tool
	// result. It is not the model's reply language (that comes from the env block);
	// this is the agent's own prose.
	Locale string
	// Compact, when set, is invoked on a CONTEXT-OVERFLOW error: it folds the
	// session and returns the REBUILT history + the new chain tip. The step is
	// then retried ONCE with the trimmed context (transparent to the caller).
	Compact func(ctx context.Context, reason string) (history []llm.Message, newTip string, compacted bool, err error)
	// ToolTimeout bounds each tool call (0 = none). On timeout the tool yields an
	// error-content result so the model can recover instead of the turn hanging.
	ToolTimeout time.Duration
	// SystemTokens is the estimated cost of the system prompt (counted toward the
	// proactive-compaction budget).
	SystemTokens int
	// ContextLimit is the model's context window in tokens (0 = proactive
	// compaction disabled).
	ContextLimit int
	// CompactAtFraction overrides DefaultCompactAtFraction (0 = default 0.9).
	CompactAtFraction float64
	// ForceCompactTokens is the absolute proactive-compaction threshold in
	// tokens (0 = disabled). Whichever of the two thresholds is smaller wins.
	ForceCompactTokens int
}

// Input is one turn (one user prompt).
type Input struct {
	Tenant  string
	Session string
	RunID   string

	Model  string
	System string
	// PrevID is the chain anchor the first step's message hangs off (the
	// session's current tip; "" for the start of a chain).
	PrevID  string
	History []llm.Message
	Tools   []llm.ToolDef
	Extra   map[string]any
}

// Result summarises a completed turn.
type Result struct {
	Text         string
	FinishReason string
	Reason       string
	Steps        int
	InputTokens  int64
	OutputTokens int64
}

// maxStepsPrompt is injected on the final step (tools disabled) so the model
// produces a text wrap-up instead of the loop silently ending mid-task.
const maxStepsPrompt = "CRITICAL - MAXIMUM STEPS REACHED\n\n" +
	"The maximum number of steps allowed for this task has been reached. " +
	"Tools are disabled until the next user input. Respond with text only.\n\n" +
	"STRICT REQUIREMENTS:\n" +
	"1. Do NOT make any tool calls.\n" +
	"2. MUST provide a text response summarizing work done so far.\n" +
	"3. This overrides ALL other instructions.\n\n" +
	"Include: that the maximum steps were reached; what was accomplished; " +
	"what remains incomplete; and recommended next steps."

// RunTurn drives one prompt through up to MaxTurns model steps, executing any
// tool calls in model order, and emits the EVENTS.md event stream.
func RunTurn(ctx context.Context, deps Deps, in Input) (Result, error) {
	maxTurns := deps.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 1
	}
	var res Result
	messages := append([]llm.Message(nil), in.History...)
	prevID := in.PrevID
	overflowRetried := false

	for step := 0; step < maxTurns; step++ {
		// Final step: disable tools and nudge the model to wrap up in text.
		isLast := step+1 >= maxTurns
		tools := in.Tools
		if isLast {
			tools = nil
			messages = append(messages, llm.Message{Role: llm.RoleAssistant, Content: maxStepsPrompt})
		}

		// PROACTIVE compaction (Go-only): if the estimated context is already at
		// or above the effective threshold (window fraction OR the absolute
		// force ceiling, whichever is smaller), fold BEFORE calling the provider
		// instead of waiting for an overflow error. A no-op compaction (nothing
		// foldable) returns compacted=false and we proceed as-is.
		if deps.Compact != nil {
			budget, reason := compactThreshold(deps.ContextLimit, deps.CompactAtFraction, deps.ForceCompactTokens)
			est := EstimateMessages(messages) + deps.SystemTokens + EstimateToolSchema(tools)
			if budget > 0 && est >= budget {
				if newHistory, newTip, compacted, cerr := deps.Compact(ctx, reason); cerr == nil && compacted {
					messages = newHistory
					prevID = newTip
					_ = emit(ctx, deps.Emit, "compacted", map[string]any{
						"reason": reason, "estimated_tokens": est,
						"context_limit": deps.ContextLimit, "run_id": in.RunID,
					})
				}
			}
		}

		stepID := newID()
		// Announce the step's server-authored id BEFORE any delta, so the client
		// creates the bubble the deltas will carry (EVENTS.md rule 3).
		if err := emit(ctx, deps.Emit, "message-added", map[string]any{
			"message_id": stepID, "prev_id": prevID, "role": "assistant",
			"streaming": true, "source": "", "run_id": in.RunID,
		}); err != nil {
			return res, err
		}
		if err := emit(ctx, deps.Emit, "start-step", map[string]any{
			"run_id": in.RunID, "message_id": stepID,
		}); err != nil {
			return res, err
		}

		req := llm.ChatRequest{
			Model: in.Model, System: in.System, Messages: messages,
			Tools: tools, Extra: in.Extra,
		}
		stream, err := deps.Client.RetryingStream(ctx, req, deps.Retry,
			func(attempt int, delay time.Duration, err error) {
				// `retry` reuses the SAME message_id so the client clears the
				// partial bubble before the retried deltas arrive (rule 2).
				reason := ""
				if err != nil {
					reason = err.Error()
				}
				_ = emit(ctx, deps.Emit, "retry", map[string]any{
					"message_id": stepID, "attempt": attempt,
					"delay_ms": delay.Milliseconds(), "reason": reason, "run_id": in.RunID,
				})
			})
		if err != nil {
			_ = emit(ctx, deps.Emit, "error", map[string]any{
				"error": err.Error(), "message": err.Error(),
				"message_id": stepID, "run_id": in.RunID,
			})
			res.Reason = "error"
			return res, err
		}

		var text, reasoning string
		var calls []llm.ToolCall
		var finish string
		var overflowErr error
		for ev := range stream {
			switch ev.Type {
			case llm.EventTextDelta:
				text += ev.Text
				_ = emit(ctx, deps.Emit, "text-delta", map[string]any{
					// `id` is the PART id, and it must be stable for the whole step.
					//
					// The console groups deltas by it: same id, same block, appended.
					// It had been omitted entirely, and the client's delta handler
					// requires it — so every frame was dropped, and the text appeared
					// only through the per-step message insert, one fragment per
					// block. stepID is the step's own id, which is exactly the
					// granularity a part needs.
					"id": stepID, "text": ev.Text,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventReasoningDelta:
				reasoning += ev.Text
				_ = emit(ctx, deps.Emit, "reasoning-delta", map[string]any{
					// Same contract as text-delta; the client prefixes "r" so the
					// reasoning block and the text block stay separate.
					"id": stepID, "text": ev.Text,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolCall:
				calls = append(calls, *ev.ToolCall)
				input := decodeToolInput(ev.ToolCall.Args)
				_ = emit(ctx, deps.Emit, "tool-call", map[string]any{
					// BOTH keys carry the same value, on purpose.
					//
					// The client keys a tool card on `toolCallId` in one handler and `id` in another,
					// and an event that has only the OTHER key makes those handlers disagree about
					// which CARD a result belongs to. The result was one card holding the arguments
					// with an empty body, plus a second card named "tool" holding the output: the
					// same call, split in two.
					//
					// Sending both removes the disagreement at the source, instead of leaving each
					// client to fall back between field names and hope.
					"toolCallId": ev.ToolCall.ID, "id": ev.ToolCall.ID,
					"toolName": ev.ToolCall.Name,
					"input": input, "message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputStart:
				_ = emit(ctx, deps.Emit, "tool-input-start", map[string]any{
					"id": ev.ToolCall.ID, "toolName": ev.ToolCall.Name,
					"toolCallId": ev.ToolCall.ID,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputDelta:
				_ = emit(ctx, deps.Emit, "tool-input-delta", map[string]any{
					"id": ev.ToolCall.ID, "delta": ev.Text,
					"toolCallId": ev.ToolCall.ID,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputEnd:
				_ = emit(ctx, deps.Emit, "tool-input-end", map[string]any{
					"id": ev.ToolCall.ID, "toolCallId": ev.ToolCall.ID,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventFinishStep:
				finish = ev.FinishReason
				if ev.Usage != nil {
					res.InputTokens += ev.Usage.InputTokens
					res.OutputTokens += ev.Usage.OutputTokens
					_ = emit(ctx, deps.Emit, "finish-step", map[string]any{
						"usage": map[string]any{
							"inputTokens":  ev.Usage.InputTokens,
							"outputTokens": ev.Usage.OutputTokens,
						},
						"finishReason": finish, "message_id": stepID, "run_id": in.RunID,
					})
				} else {
					_ = emit(ctx, deps.Emit, "finish-step", map[string]any{
						"finishReason": finish, "message_id": stepID, "run_id": in.RunID,
					})
				}
			case llm.EventError:
				// A CONTEXT-OVERFLOW error is RECOVERED (compaction + one retry)
				// rather than surfaced — it is not a turn failure.
				if !overflowRetried && deps.Compact != nil && llm.IsContextOverflowError(ev.Err) {
					overflowErr = ev.Err
					break
				}
				_ = emit(ctx, deps.Emit, "error", map[string]any{
					"error": ev.Err.Error(), "message": ev.Err.Error(),
					"message_id": stepID, "run_id": in.RunID,
				})
				res.Reason = "error"
				return res, ev.Err
			}
			if overflowErr != nil {
				break
			}
		}
		if overflowErr != nil {
			// Compact, re-anchor this step onto the new tip, and retry ONCE.
			overflowRetried = true
			history, newTip, compacted, cerr := deps.Compact(ctx, "overflow")
			if cerr != nil || !compacted {
				_ = emit(ctx, deps.Emit, "error", map[string]any{
					"error": overflowErr.Error(), "message": overflowErr.Error(),
					"message_id": stepID, "run_id": in.RunID,
				})
				res.Reason = "error"
				if cerr != nil {
					return res, cerr
				}
				return res, overflowErr
			}
			messages = history
			prevID = newTip
			// Re-announce the SAME message id with the new prevId (the client
			// updates the bubble's position, never creates a second one).
			_ = emit(ctx, deps.Emit, "message-added", map[string]any{
				"message_id": stepID, "prev_id": prevID, "role": "assistant",
				"streaming": true, "source": "", "run_id": in.RunID,
			})
			step-- // retry this step
			continue
		}
		_ = reasoning // persisted for display only; excluded from rebuilt context
		res.Steps++
		res.Text += text
		res.FinishReason = finish

		// The step is durable under the same id it streamed under.
		if err := emit(ctx, deps.Emit, "message-added", map[string]any{
			"message_id": stepID, "prev_id": prevID, "role": "assistant",
			"streaming": false, "source": "", "run_id": in.RunID,
		}); err != nil {
			return res, err
		}
		prevID = stepID

		if len(calls) == 0 {
			_ = emit(ctx, deps.Emit, "finish", map[string]any{
				"finishReason": finish, "run_id": in.RunID,
			})
			res.Reason = "stop"
			// A truncated generation (the model hit its output-token ceiling,
			// provider finishReason "length") is NOT a natural stop — it stopped
			// mid-thought. Record it distinctly so a watcher can resume it.
			if finish == "length" {
				res.Reason = "length"
			}
			_ = emit(ctx, deps.Emit, "turn-complete", map[string]any{
				"reason": res.Reason, "run_id": in.RunID,
			})
			return res, nil
		}

		// Append the assistant's tool-call message, then run tools IN MODEL
		// ORDER (sequential: a side-effecting tool must not be overtaken).
		messages = append(messages, llm.Message{
			Role: llm.RoleAssistant, Content: text, ToolCalls: calls,
		})
		for _, call := range calls {
			// D1: repair a tool name the model got wrong (lowercase) — otherwise
			// hand the model an error result naming the available tools so it can
			// self-correct in the SAME turn instead of the turn dying.
			name := resolveToolName(call.Name, tools)
			if name == "" {
				execErr := fmt.Errorf("unknown tool %q (available: %s)", call.Name, toolNames(tools))
				messages = append(messages, llm.Message{
					Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name, Content: execErr.Error(),
				})
				emitError(ctx, deps, in, stepID, call.ID, call.Name, execErr)
				continue
			}
			// call.ID is the identity the extension reports progress under, so it
			// is what the relay subscribes to.
			outcome, execErr := execTool(ctx, deps, in, name, call.ID, stepID, call.Args)
			if execErr != nil {
				// A failed tool still pairs with its call id so the provider
				// never sees a dangling tool-call (rule 4).
				// The terminal `tool-output-error` frame was already published by
				// execTool; emitting a second error event here would make the client
				// render the same failure twice.
				messages = append(messages, llm.Message{
					Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name,
					Content: execErr.Error(),
				})
				continue
			}
			messages = append(messages, llm.Message{
				Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name,
				Content: outcome.Content,
			})
			// No separate result event: execTool already published the terminal
			// `tool-output-available` for this call. Emitting the old `tool-result`
			// alongside it would give the client two sources for one result, and
			// they would have to agree.
		}
	}

	// Step budget exhausted without a text-only finish.
	_ = emit(ctx, deps.Emit, "finish", map[string]any{
		"finishReason": "length", "run_id": in.RunID,
	})
	res.Reason = "max-steps"
	_ = emit(ctx, deps.Emit, "turn-complete", map[string]any{
		"reason": res.Reason, "run_id": in.RunID,
	})
	return res, nil
}

// execTool runs one tool with the per-tool wall-clock timeout.
//
// EVERY tool publishes its output the same way: as `tool-output-available`
// snapshots while it runs (preliminary) and one terminal frame at the end. Tools
// that report progress get their lines relayed as they arrive; tools that simply
// return get their whole result published at the end. The client therefore has one
// rendering path, and the output survives a refresh because the frames are durable
// session events.
//
// None of this reaches `messages`: the model still sees only the final result.
func execTool(ctx context.Context, deps Deps, in Input, name, callID, stepID, args string) (ToolOutcome, error) {
	stream := newResultStream()
	stopRelay := relayInto(ctx, deps, in, callID, stepID, name, stream)
	defer stopRelay()

	run := func(runCtx context.Context) (ToolOutcome, error) {
		return deps.Tools.Execute(runCtx, in.Tenant, in.Session, callID, name, args)
	}

	var out ToolOutcome
	var err error
	if deps.ToolTimeout <= 0 {
		out, err = run(ctx)
	} else {
		type result struct {
			out ToolOutcome
			err error
		}
		ch := make(chan result, 1)
		tctx, cancel := context.WithTimeout(ctx, deps.ToolTimeout)
		defer cancel()
		go func() {
			o, e := run(tctx)
			ch <- result{o, e}
		}()
		select {
		case r := <-ch:
			out, err = r.out, r.err
		case <-tctx.Done():
			// Interrupted, not failed. Whatever the tool printed is real output and
			// is already on the wire, so the result keeps it — the model needs to
			// know how far the command got, and the user needs to keep seeing it.
			state := "timeout"
			if ctx.Err() != nil {
				state = "cancelled"
			}
			stream.Close(state)
			// Same size policy as a normal result: an interrupted tool's output is
			// stored and handed to the model exactly like a finished one, so a job that
			// printed 100k lines before timing out must not smuggle them into either.
			partial := stream.TrimToLimit(deps.Locale)
			// Stop the relay before the terminal frame, for the same reason as the
			// success path: a delta published after it would be appended to output the
			// client had already been given in full.
			stopRelay()
			emitOutput(ctx, deps, in, stepID, callID, name, stream, map[string]any{
				"state": state,
				"error": fmt.Sprintf("%s after %s", state, deps.ToolTimeout),
			})
			note := fmt.Sprintf("[%s after %s]", state, deps.ToolTimeout)
			if partial != "" {
				partial = partial + "\n" + note
			} else {
				partial = note
			}
			out = ToolOutcome{Content: partial, Metadata: map[string]any{"state": state}}
			return out, nil
		}
	}

	// Terminal frame for the tool's own result.
	if err != nil {
		stream.Close("error")
		emitError(ctx, deps, in, stepID, callID, name, err)
		return out, err
	}
	// The tool's returned content is the RESULT; the relayed lines are only its live
	// preview. They are usually the same text, so the result REPLACES the stream rather
	// than extending it.
	//
	// Appending was a real defect: a streaming tool had already put 900 lines into the
	// stream, the returned Content was those same 900, and the sum (1800) was then
	// trimmed to 250 + 250 — taking the head of one copy and the tail of the other. The
	// card showed two overlapping runs of line numbers.
	//
	// Replacing is also what makes the model and the console agree: the model is given
	// out.Content, so that is what the card must show.
	if out.Content != "" {
		stream.SetText(out.Content)
	}
	// TRIM before anything is stored, sent, or shown again.
	//
	// Everything above ran while the tool was live, where every line matters and the
	// user is watching them arrive. From here the output goes into three places that
	// pay for it forever: the database, the model's context, and the transcript a
	// client rebuilds.
	//
	// The STREAM is trimmed, not just the value returned: emitOutput reads the stream's
	// lines, so trimming only the return value would leave the terminal SNAPSHOT
	// carrying the full output — and that snapshot is what the console stores.
	trimmed := stream.TrimToLimit(deps.Locale)
	if trimmed != stream.Text() {
		// The model gets exactly what the console gets. If they disagreed, the model
		// would reason about lines no reader can see.
		out.Content = trimmed
	}
	// STOP the relay BEFORE the terminal frame, not after.
	//
	// `defer stopRelay()` runs when execTool returns, which is AFTER emitOutput below.
	// In that window the relay goroutine could still be draining the progress channel
	// and publishing deltas — so the client accumulated MORE lines after the terminal
	// frame had already replaced them, and the card showed the output twice.
	//
	// Stopping here also makes the sequence deterministic: every delta is published
	// before the terminal frame, never after.
	stopRelay()
	stream.Close("ok")
	meta := map[string]any{}
	if m, ok := out.Metadata.(map[string]any); ok {
		for k, v := range m {
			meta[k] = v
		}
	}
	// Normalise the state a tool reports ("done" from a job, "finished" from a
	// build) to the one vocabulary the protocol uses. Without this the console
	// shows "stopped: done" for a job that simply finished.
	if s, ok := meta["state"].(string); ok && isSuccessState(s) {
		meta["state"] = "ok"
	}
	emitOutput(ctx, deps, in, stepID, callID, name, stream, meta)
	return out, nil
}

// isSuccessState reports whether a tool's own word for "finished" means success.
//
// Tools describe their outcome in their own vocabulary — a job says "done", a
// build says "finished", a command may say "exited" — and the protocol has one
// word for it. Everything else ("failed", "timeout", "cancelled") is passed
// through, because those genuinely are different outcomes.
func isSuccessState(s string) bool {
	switch s {
	case "ok", "done", "finished", "exited", "complete", "completed", "success", "succeeded":
		return true
	}
	return false
}

// emitError publishes the terminal frame for a failed tool.
func emitError(ctx context.Context, deps Deps, in Input, stepID, callID, toolName string, cause error) {
	if deps.Emit == nil {
		return
	}
	_ = emitBg(deps.Emit, "tool-output-error", map[string]any{
		"toolCallId": callID,
		"toolName":   toolName,
		"errorText":  cause.Error(),
		"message_id": stepID,
		"run_id":     in.RunID,
	})
}

// relayInto feeds a tool's in-flight updates into the result stream and publishes
// each new snapshot.
//
// The relay is what makes EVERY tool look streamed: a tool that reports progress
// gets its lines appended as they arrive, and one that reports nothing produces a
// single terminal frame at the end. Either way the client receives
// `tool-output-available`, so it has one path to render and the output survives a
// refresh (the frames are durable session events).
//
// Returns a stop function; safe to call once, after the tool call returns.
func relayInto(ctx context.Context, deps Deps, in Input, callID, stepID, toolName string, stream *resultStream) func() {
	if deps.ToolProgress == nil || deps.Emit == nil || callID == "" {
		return func() {}
	}
	// Derived from ctx, not Background: the relay must stop when the turn is
	// cancelled, and the channel closes on its own when the tool finishes.
	ch, cancel, err := deps.ToolProgress(ctx, in.Tenant, callID)
	if err != nil || ch == nil {
		return func() {}
	}
	done := make(chan struct{})
	var stopped sync.Once
	go func() {
		defer close(done)
		// A trickle must still appear promptly: without this ticker a job that prints
		// one line per second would sit invisible until 64 of them accumulated.
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				// Send whatever has accumulated, even if the batch is not full.
				emitOutput(ctx, deps, in, stepID, callID, toolName, stream, nil)
			case upd, ok := <-ch:
				if !ok {
					return
				}
				text := upd.Text
				// A phase-only frame is NOT output. "exited" is the extension saying
				// the process ended; rendering it as a line would put a status word in
				// the middle of the command's stdout. Status text that a human should
				// read ("waiting for rollout") carries it in Text, which is how the
				// extension already expresses it.
				if text == "" {
					continue
				}
				stream.Append(text)
				// Flush on a BATCH BOUND, not on every line. One frame per line with the
				// whole output is O(n²) bytes and was making large jobs heavy; this sends
				// each byte once.
				if stream.PendingFull() {
					emitOutput(ctx, deps, in, stepID, callID, toolName, stream, nil)
				}
			}
		}
	}()
	return func() {
		// Idempotent: execTool stops the relay explicitly before the terminal frame
		// and also defers a stop, so this runs twice on the success path. cancel is
		// safe to call repeatedly and `done` is already closed the second time.
		stopped.Do(func() {
			cancel()
			<-done
		})
	}
}

// resolveToolName maps a model-emitted tool name onto an offered tool: an exact
// match, else a case-insensitive match (the common "lowercased name" mistake).
// Returns "" when no offered tool matches.
func resolveToolName(name string, tools []llm.ToolDef) string {
	for _, t := range tools {
		if t.Name == name {
			return name
		}
	}
	lower := strings.ToLower(name)
	for _, t := range tools {
		if strings.ToLower(t.Name) == lower {
			return t.Name
		}
	}
	return ""
}

func toolNames(tools []llm.ToolDef) string {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.Name
	}
	return strings.Join(names, ", ")
}

func emit(ctx context.Context, e Emitter, name string, params map[string]any) error {
	if e == nil {
		return nil
	}
	return e.Emit(ctx, Event{Name: name, Params: params})
}

/**
 * decodeToolInput turns a provider's tool-call argument text into the OBJECT a client can read
 * fields from.
 *
 * Providers disagree about how many times arguments are encoded. The stream spec says `input` is a
 * JSON *string*, but some send an object; and at least one wraps its JSON in a string inside that
 * string. Each extra layer survives `json.Unmarshal` as a `string` — which is not an error, so
 * nothing complains, and every client that reads `input.todos` (or any other field) finds nothing
 * and renders an empty card.
 *
 * So: decode until the result is no longer a string, with a small bound. Returning a string after
 * that is the honest answer — a tool whose arguments really are a bare string exists — but the
 * common case is an object, and this makes it one.
 */
func decodeToolInput(args string) any {
	var v any
	if err := json.Unmarshal([]byte(args), &v); err != nil {
		// Not JSON at all. Hand it over as text rather than dropping it: a card can still show it,
		// and it means the model sent something that is not JSON, which is worth seeing.
		return args
	}
	// Two extra unwraps cover the observed triple encoding; the bound stops a pathological input
	// from looping.
	for i := 0; i < 2; i++ {
		s, ok := v.(string)
		if !ok {
			return v
		}
		var next any
		if err := json.Unmarshal([]byte(s), &next); err != nil {
			return v
		}
		v = next
	}
	return v
}

// newID returns the id for one assistant step.
//
// It must be unique across PROCESS RESTARTS, not just within one process: message
// ids are primary keys, so a counter that restarts at 1 reissues ids that already
// exist. InsertMessage ignores the collision (OnConflict DoNothing), so the new
// message is silently not stored — while its prev_id still points at the chain,
// which makes the session render old content and the tip advance nowhere. That is
// exactly "the message I just sent is gone", and it looked like a revert bug.
//
// The old id was `msg-<counter>`; rows from before this change keep that shape,
// which is fine — only the new ids need to be globally unique.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read from crypto/rand does not fail in practice; if it ever does,
		// falling back to a timestamp keeps ids unique instead of colliding.
		return fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	return "msg-" + hex.EncodeToString(b[:])
}
