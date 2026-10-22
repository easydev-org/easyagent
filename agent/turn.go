package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
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
				var input any
				_ = json.Unmarshal([]byte(ev.ToolCall.Args), &input)
				_ = emit(ctx, deps.Emit, "tool-call", map[string]any{
					"toolCallId": ev.ToolCall.ID, "toolName": ev.ToolCall.Name,
					"input": input, "message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputStart:
				_ = emit(ctx, deps.Emit, "tool-input-start", map[string]any{
					"id": ev.ToolCall.ID, "toolName": ev.ToolCall.Name,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputDelta:
				_ = emit(ctx, deps.Emit, "tool-input-delta", map[string]any{
					"id": ev.ToolCall.ID, "delta": ev.Text,
					"message_id": stepID, "run_id": in.RunID,
				})
			case llm.EventToolInputEnd:
				_ = emit(ctx, deps.Emit, "tool-input-end", map[string]any{
					"id": ev.ToolCall.ID, "message_id": stepID, "run_id": in.RunID,
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
				_ = emit(ctx, deps.Emit, "tool-error", map[string]any{
					"toolCallId": call.ID, "toolName": call.Name,
					"error": execErr.Error(), "message": execErr.Error(),
					"message_id": stepID, "run_id": in.RunID,
				})
				continue
			}
			// call.ID is the identity the extension reports progress under, so it
			// is what the relay subscribes to.
			outcome, execErr := execTool(ctx, deps, in, name, call.ID, call.Args)
			if execErr != nil {
				// A failed tool still pairs with its call id so the provider
				// never sees a dangling tool-call (rule 4).
				messages = append(messages, llm.Message{
					Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name,
					Content: execErr.Error(),
				})
				_ = emit(ctx, deps.Emit, "tool-error", map[string]any{
					"toolCallId": call.ID, "toolName": call.Name,
					"error": execErr.Error(), "message": execErr.Error(),
					"message_id": stepID, "run_id": in.RunID,
				})
				continue
			}
			messages = append(messages, llm.Message{
				Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name,
				Content: outcome.Content,
			})
			var input any
			_ = json.Unmarshal([]byte(call.Args), &input)
			params := map[string]any{
				"toolCallId": call.ID, "toolName": call.Name, "input": input,
				"output": map[string]any{
					"content": outcome.Content, "metadata": outcome.Metadata,
				},
				// Derived convenience fields the existing clients read.
				"formatted": outcome.Content, "data": outcome.Metadata,
				"message_id": stepID, "run_id": in.RunID,
			}
			if m, ok := outcome.Metadata.(map[string]any); ok {
				if cid, ok := m["change_id"].(string); ok {
					params["change_id"] = cid
				}
			}
			_ = emit(ctx, deps.Emit, "tool-result", params)
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
// While the tool runs, any in-flight progress it reports is relayed to the
// session's durable event stream as `tool-progress` frames. Those frames reach
// the UI and a reconnecting client replays them, but they never enter
// `messages` — the model still sees only the tool's final result.
func execTool(ctx context.Context, deps Deps, in Input, name, callID, args string) (ToolOutcome, error) {
	relayProgress(ctx, deps, in, callID)
	if deps.ToolTimeout <= 0 {
		return deps.Tools.Execute(ctx, in.Tenant, in.Session, callID, name, args)
	}
	type result struct {
		out ToolOutcome
		err error
	}
	ch := make(chan result, 1)
	tctx, cancel := context.WithTimeout(ctx, deps.ToolTimeout)
	defer cancel()
	go func() {
		o, e := deps.Tools.Execute(tctx, in.Tenant, in.Session, callID, name, args)
		ch <- result{o, e}
	}()
	select {
	case r := <-ch:
		return r.out, r.err
	case <-tctx.Done():
		return ToolOutcome{}, fmt.Errorf("tool %q timed out after %s", name, deps.ToolTimeout)
	}
}

// relayProgress forwards a tool's in-flight updates to the event stream.
//
// It returns immediately; the subscription lives until the progress channel
// closes. Relaying is best-effort by design: the tool's result is the contract,
// and a UI that misses a frame has lost nothing that matters.
func relayProgress(ctx context.Context, deps Deps, in Input, callID string) {
	if deps.ToolProgress == nil || deps.Emit == nil || callID == "" {
		return
	}
	// The subscription must not die with the turn's context if the turn returns
	// while a job keeps printing — but it must die if the session is cancelled.
	// Deriving from ctx (not Background) keeps that guarantee; the channel closes
	// on its own when the tool finishes.
	ch, cancel, err := deps.ToolProgress(ctx, in.Tenant, callID)
	if err != nil || ch == nil {
		return
	}
	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case upd, ok := <-ch:
				if !ok {
					return
				}
				params := map[string]any{
					"toolCallId": upd.CallID,
					"phase":      upd.Phase,
					"text":       upd.Text,
					"run_id":     in.RunID,
				}
				if upd.Progress > 0 {
					params["progress"] = upd.Progress
				}
				if upd.Metadata != nil {
					params["metadata"] = upd.Metadata
				}
				_ = emitBg(deps.Emit, "tool-progress", params)
			}
		}
	}()
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

// newID mints a message id. The real agent uses a UUID; the loop only needs a
// unique, opaque id, so a monotonic counter + prefix is enough here and keeps
// the package dependency-free. Atomic so concurrent turns cannot collide.
var idSeq atomic.Uint64

func newID() string {
	return fmt.Sprintf("msg-%d", idSeq.Add(1))
}
