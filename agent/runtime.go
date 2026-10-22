package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// WakeTypes are the mailbox message types that WAKE a session's turn loop.
// Every other type is CONTEXT-ONLY (folded into the chain lazily).
var WakeTypes = []string{"trigger", "compact"}

// WakePayload is a mailbox message body.
type WakePayload struct {
	Type        string    `json:"type"`
	MessageID   string    `json:"message_id"`
	Text        string    `json:"text"`
	Attachments []FileRef `json:"attachments,omitempty"`
}

// FileRef is a resolved attachment reference (metadata comes from the stored
// record, never the caller).
type FileRef struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Mime string `json:"mime"`
	Size int    `json:"size"`
}

// RuntimeDeps are the session runtime's collaborators.
type RuntimeDeps struct {
	DB       *store.DB
	Client   *llm.Client
	Resolver ProviderResolver
	Lease    Lease
	Tools    ToolExecutor
	// ToolProgress streams in-flight updates for a running tool call.
	//
	// Injected rather than subscribed here because RuntimeDeps deliberately has no
	// bus: the runtime drives turns, and everything that touches the transport is
	// wired in main. nil disables the live view; the tool still runs.
	ToolProgress ToolProgressSource
	// Registry supplies the tools offered to the model (filtered per turn by
	// the preset whitelist + the denylist).
	Registry ToolRegistry
	// Presets/Config resolve the turn's system prompt, locale and tool
	// whitelist. Optional (a session with no preset is unrestricted).
	Presets PresetProvider
	Config  ConfigProvider
	// DeniedTools is the host hard-denylist of qualified `<extId>-<name>` ids.
	DeniedTools []string
	// ExtensionAllowlist, when set, returns the extIds a tenant's delegation
	// subtree may use (nil = unrestricted). Enforces the per-subadmin
	// "which extension servers" whitelist at turn time.
	ExtensionAllowlist func(tenant string) []string
	// ToolTimeout bounds each tool call (0 = none).
	ToolTimeout time.Duration
	// CompactAtFraction overrides the proactive-compaction threshold (0 = 0.9).
	CompactAtFraction float64
	// ContextLimit overrides the model's context window (0 = derive from the
	// provider's model metadata; both 0 = proactive compaction disabled).
	ContextLimit int
	// ForceCompactTokens is the absolute proactive-compaction threshold (0 =
	// default 900k; negative = disabled).
	ForceCompactTokens int
	// Emit is a fixed sink (tests). EmitFor, when set, takes precedence and
	// builds a per-session sink so live subscribers receive the run's events.
	Emit    Emitter
	EmitFor func(tenant, session string) Emitter
	// InterruptWatch, when set, streams a session's mailbox wake frames while a
	// turn runs, so an `interrupt` published by ANOTHER replica aborts the run.
	InterruptWatch func(ctx context.Context, tenant, session string) (<-chan string, func(), error)
	Retry          llm.RetryPolicy
	MaxTurns       int
	Now            func() time.Time
	// LeaseTTL is the run-lease TTL (0 = 30s). The lock is renewed at TTL/3.
	LeaseTTL time.Duration
}

// emitterFor returns the sink for one session.
func (r *Runtime) emitterFor(tenant, session string) Emitter {
	if r.deps.EmitFor != nil {
		return r.deps.EmitFor(tenant, session)
	}
	return r.deps.Emit
}

// Runtime drives sessions: it drains the mailbox under the run lease, folds
// messages into the chain, runs the turn loop, and persists each step.
type Runtime struct {
	deps   RuntimeDeps
	mu     sync.Mutex
	active map[string]context.CancelFunc
	// cancelReason records WHY an in-flight run was cancelled (per
	// tenant\x00session), so the turn-end reason is accurate instead of blank:
	// "interrupted" (a user/client interrupt) or "locklost" (the run lease was
	// lost to another replica). Set just before cancel(); read once at turn end.
	cancelReason map[string]string
	// compaction budgets (0 = defaults; injectable for tests).
	compactTailTokens int
	compactFoldTokens int
}

// NewRuntime builds a Runtime.
func NewRuntime(deps RuntimeDeps) *Runtime {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Runtime{deps: deps, active: map[string]context.CancelFunc{}, cancelReason: map[string]string{}}
}

// markCancel records why a session's run is being cancelled, then cancels it.
// A no-op when no run is active (so the reason map never leaks a stale entry).
func (r *Runtime) markCancel(tenant, session, reason string) {
	r.mu.Lock()
	cancel, ok := r.active[tenant+"\x00"+session]
	if ok {
		r.cancelReason[tenant+"\x00"+session] = reason
	}
	r.mu.Unlock()
	if ok {
		cancel()
	}
}

// takeCancelReason returns and clears a session's recorded cancel reason.
func (r *Runtime) takeCancelReason(tenant, session string) string {
	k := tenant + "\x00" + session
	r.mu.Lock()
	reason := r.cancelReason[k]
	delete(r.cancelReason, k)
	r.mu.Unlock()
	return reason
}

// Wake processes a session: claim the lease, then loop (drain the mailbox, run
// a turn when the batch carried a trigger, persist each step) until the mailbox
// is empty. The lease is renewed while the run holds it, and released ONCE at
// the end of the busy period — so a chained continuation never flaps
// busy→idle→busy. Safe to call concurrently for the same session (only one
// wins the lease).
func (r *Runtime) Wake(ctx context.Context, tenant, session string) error {
	ok, err := r.deps.Lease.Claim(ctx, tenant, session)
	if err != nil {
		slog.Error("lease claim failed", "tenant", tenant, "session", session, "err", err)
		return err
	}
	if !ok {
		return nil // another replica owns the session
	}

	runCtx, cancel := context.WithCancel(ctx)
	r.setActive(tenant, session, cancel)
	defer r.clearActive(tenant, session)
	defer cancel()
	// B1: the busy period begins — announce `status:busy` (run-scoped).
	_ = emitBg(r.emitterFor(tenant, session), "status", map[string]any{"type": "busy"})

	// A1: renew the lease on a timer (TTL/3). A definitive loss aborts the run;
	// a transient error is NOT a loss (retry next tick).
	ttl := r.deps.LeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	var leaseLost atomic.Bool
	renewStop := make(chan struct{})
	go func() {
		t := time.NewTicker(ttl / 3)
		defer t.Stop()
		for {
			select {
			case <-renewStop:
				return
			case <-runCtx.Done():
				return
			case <-t.C:
				held, rerr := r.deps.Lease.Renew(runCtx, tenant, session)
				if rerr == nil && !held {
					leaseLost.Store(true)
					r.markCancel(tenant, session, "locklost")
					return
				}
			}
		}
	}()

	// A3: watch the mailbox for a cross-replica `interrupt` and abort the run.
	if r.deps.InterruptWatch != nil {
		if ch, stop, werr := r.deps.InterruptWatch(runCtx, tenant, session); werr == nil {
			defer stop()
			go func() {
				for {
					select {
					case <-runCtx.Done():
						return
					case typ, ok := <-ch:
						if !ok {
							return
						}
						if typ == "interrupt" {
							r.markCancel(tenant, session, "interrupted")
						}
					}
				}
			}()
		}
	}

	// A4: drain in a loop while the lease is held. Only an empty queue ends the
	// busy period.
	for {
		if leaseLost.Load() || runCtx.Err() != nil {
			break
		}
		items, derr := r.deps.DB.DrainAll(tenant, session)
		if derr != nil {
			r.releaseIfHeld(ctx, tenant, session, leaseLost.Load())
			return derr
		}
		if len(items) == 0 {
			break
		}
		if !r.handleBatch(runCtx, tenant, session, items, cancel) {
			break
		}
	}

	// Release only a lock we still hold (a lost lock belongs to another replica).
	r.releaseIfHeld(ctx, tenant, session, leaseLost.Load())
	if leaseLost.Load() {
		return nil
	}
	// A4 (re-claim): a wake may have arrived while we held the lock (its row is
	// pending but the waking invocation could not claim). Re-claim once; if we
	// get it and work is pending, keep holding and loop as a FRESH busy period.
	ok, err = r.deps.Lease.Claim(ctx, tenant, session)
	if err != nil || !ok {
		return err
	}
	items, derr := r.deps.DB.DrainAll(tenant, session)
	if derr != nil || len(items) == 0 {
		r.releaseIfHeld(ctx, tenant, session, false)
		return derr
	}
	// A late prompt arrived: process it in a fresh busy period.
	runCtx2, cancel2 := context.WithCancel(ctx)
	r.setActive(tenant, session, cancel2)
	defer r.clearActive(tenant, session)
	_ = r.handleBatch(runCtx2, tenant, session, items, cancel2)
	r.releaseIfHeld(ctx, tenant, session, false)
	cancel2()
	return nil
}

// releaseIfHeld releases the lease unless it was lost.
func (r *Runtime) releaseIfHeld(ctx context.Context, tenant, session string, lost bool) {
	if lost {
		return
	}
	// B1: publish `status:idle` BEFORE releasing the lease, so a mailbox wake for
	// the next turn cannot claim the freed lease and land its `busy` first.
	_ = emitBg(r.emitterFor(tenant, session), "status", map[string]any{"type": "idle"})
	_ = r.deps.Lease.Release(context.WithoutCancel(ctx), tenant, session)
}

// handleBatch folds the drained batch and runs a turn when it carried a
// trigger. Returns true when the busy period should continue draining.
func (r *Runtime) handleBatch(ctx context.Context, tenant, session string, items []store.Mailbox, cancel context.CancelFunc) bool {
	hasTrigger := false
	for _, it := range items {
		switch it.MsgType {
		case "trigger":
			hasTrigger = true
			if err := r.foldUserMessage(tenant, session, it); err != nil {
				return false
			}
		case "interrupt":
			r.markCancel(tenant, session, "interrupted")
		case "compact":
			if _, err := r.CompactSession(context.WithoutCancel(ctx), tenant, session, "manual"); err != nil {
				return false
			}
		default:
			if err := r.foldEvent(tenant, session, it); err != nil {
				return false
			}
		}
	}
	if !hasTrigger {
		return true // context-only batch: keep draining
	}
	if ctx.Err() != nil {
		return false
	}
	res, err := r.runTurn(ctx, tenant, session)
	if err != nil {
		slog.Warn("turn failed", "tenant", tenant, "session", session, "err", err)
		// Record WHY it ended even on failure. Precedence:
		//   1. a cancel reason (interrupted / locklost) — set at cancel time;
		//   2. otherwise CLASSIFY the error: an input-overflow (handled by
		//      compaction elsewhere) vs. a transient transport/5xx failure worth
		//      retrying ("retryable") vs. a permanent failure ("error").
		reason := r.takeCancelReason(tenant, session)
		if reason == "" {
			reason = classifyTurnError(err)
		}
		_ = r.deps.DB.SetTurnEnd(tenant, session, reason)
		return false
	}
	// B4: record why the turn ended (readable from the list row without polling).
	reason := res.Reason
	if reason == "" {
		reason = "stop"
	}
	// A cancelled-but-successful return (e.g. the model finished right as the
	// interrupt landed) still carries the cancel reason.
	if cr := r.takeCancelReason(tenant, session); cr != "" {
		reason = cr
	}
	_ = r.deps.DB.SetTurnEnd(tenant, session, reason)
	return true
}

// classifyTurnError maps a turn failure onto a precise reason:
//
//	"overflow"  — the INPUT exceeded the context window (compaction territory);
//	"retryable" — a transient transport/5xx/429 failure worth retrying;
//	"error"     — a permanent failure (bad request, auth, unknown model, ...).
func classifyTurnError(err error) string {
	switch {
	case llm.IsContextOverflowError(err):
		return "overflow"
	case llm.IsRetryable(err):
		return "retryable"
	default:
		return "error"
	}
}

// Interrupt aborts a session's in-flight turn on THIS replica (a cross-replica
// interrupt arrives via the mailbox as an `interrupt` item).
func (r *Runtime) Interrupt(tenant, session string) bool {
	r.mu.Lock()
	_, ok := r.active[tenant+"\x00"+session]
	r.mu.Unlock()
	if ok {
		r.markCancel(tenant, session, "interrupted")
	}
	return ok
}

// IsActive reports whether a turn is currently running on THIS replica.
func (r *Runtime) IsActive(tenant, session string) bool {
	r.mu.Lock()
	_, ok := r.active[tenant+"\x00"+session]
	r.mu.Unlock()
	return ok
}

func (r *Runtime) setActive(tenant, session string, cancel context.CancelFunc) {
	r.mu.Lock()
	r.active[tenant+"\x00"+session] = cancel
	r.mu.Unlock()
}

func (r *Runtime) clearActive(tenant, session string) {
	r.mu.Lock()
	delete(r.active, tenant+"\x00"+session)
	r.mu.Unlock()
}

// foldUserMessage persists a user prompt (role=user) under its logical id,
// idempotently, and bumps the chain counter.
func (r *Runtime) foldUserMessage(tenant, session string, it store.Mailbox) error {
	var p WakePayload
	if err := json.Unmarshal([]byte(it.Payload), &p); err != nil {
		return fmt.Errorf("agent: bad trigger payload: %w", err)
	}
	id := p.MessageID
	if id == "" {
		id = it.ID
	}
	tip, err := r.deps.DB.SessionTip(tenant, session)
	if err != nil {
		return err
	}
	created, err := r.deps.DB.InsertMessage(tenant, id, "user", strOrNil(tip), "user")
	if err != nil {
		return err
	}
	if !created {
		return nil // already folded (idempotent)
	}
	if err := r.deps.DB.InsertPart(tenant, id, "text", 0, map[string]any{"text": p.Text}); err != nil {
		return err
	}
	seq := 0
	for _, a := range p.Attachments {
		seq++
		_ = r.deps.DB.InsertPart(tenant, id, "file", seq, map[string]any{
			"code": a.Code, "name": a.Name, "mime": a.Mime, "size": a.Size,
		})
	}
	if _, err := r.deps.DB.AppendMessageTip(tenant, session, id); err != nil {
		return err
	}
	return emitBg(r.emitterFor(tenant, session), "message-added", map[string]any{
		"message_id": id, "prev_id": tip, "role": "user", "streaming": false, "source": "user",
	})
}

// foldEvent persists a context-only event as an `event` message.
func (r *Runtime) foldEvent(tenant, session string, it store.Mailbox) error {
	tip, err := r.deps.DB.SessionTip(tenant, session)
	if err != nil {
		return err
	}
	created, err := r.deps.DB.InsertMessage(tenant, it.ID, "event", strOrNil(tip), it.Source)
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	if err := r.deps.DB.InsertPart(tenant, it.ID, "text", 0, map[string]any{"text": it.Payload}); err != nil {
		return err
	}
	_, err = r.deps.DB.AppendMessageTip(tenant, session, it.ID)
	return err
}

// runTurn loads the session + history and drives one turn through the turn loop.
func (r *Runtime) runTurn(ctx context.Context, tenant, session string) (Result, error) {
	s, ok, err := r.deps.DB.GetSession(tenant, session)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, fmt.Errorf("agent: session not found: %s", session)
	}
	if s.Model == "" {
		return Result{}, fmt.Errorf("agent: no model configured for session %s", session)
	}
	client := r.deps.Client
	if client == nil {
		if r.deps.Resolver == nil {
			return Result{}, errors.New("agent: no client or resolver")
		}
		client, err = r.deps.Resolver.Resolve(ctx, tenant, s.Model)
		if err != nil {
			return Result{}, err
		}
	}
	tip, err := r.deps.DB.SessionTip(tenant, session)
	if err != nil {
		return Result{}, err
	}
	msgs, err := r.deps.DB.Chain(tenant, tip, 0, "")
	if err != nil {
		return Result{}, err
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	parts, err := r.deps.DB.PartsByMessages(tenant, ids)
	if err != nil {
		return Result{}, err
	}
	history := rebuildHistory(msgs, parts)

	// Resolve the turn plan: preset system prompt (localized) + <env> block,
	// the tool list (whitelist + denylist), and the step budget.
	plan, err := Plan(ctx, r.deps, PlanInput{
		Tenant: tenant, SessionPreset: s.Preset, SessionLocale: s.Locale,
		SessionSystem: s.SystemPrompt, SessionMax: s.MaxTurns,
		DeniedTools: r.deps.DeniedTools, Now: r.deps.Now(),
	})
	if err != nil {
		return Result{}, err
	}

	sink := &persistSink{rt: r, tenant: tenant, session: session, prevID: tip, emit: r.emitterFor(tenant, session)}
	// Merge the session's reasoning variant into the request body.
	var extra map[string]any
	if body, ok := ResolveVariant(s.Variant); ok {
		extra = body
	}
	res, err := RunTurn(ctx, Deps{
		Client:   client,
		Retry:    r.deps.Retry,
		Tools:    r.deps.Tools,
		Emit:     sink,
		MaxTurns: plan.MaxTurns,
		// Relay a tool's in-flight progress onto the session's durable event
		// stream. The frames reach clients (and replay on reconnect) but never
		// enter the model's context — see relayProgress in turn.go.
		ToolProgress:       r.deps.ToolProgress,
		ToolTimeout:        r.deps.ToolTimeout,
		SystemTokens:       EstimateTokens(plan.System),
		ContextLimit:       r.contextLimit(tenant, s.Model),
		CompactAtFraction:  r.deps.CompactAtFraction,
		ForceCompactTokens: r.deps.ForceCompactTokens,
		Compact: func(ctx context.Context, reason string) ([]llm.Message, string, bool, error) {
			ok, cerr := r.CompactSession(context.WithoutCancel(ctx), tenant, session, reason)
			if cerr != nil || !ok {
				return nil, "", false, cerr
			}
			newTip, terr := r.deps.DB.SessionTip(tenant, session)
			if terr != nil {
				return nil, "", false, terr
			}
			msgs, terr := r.deps.DB.Chain(tenant, newTip, 0, "")
			if terr != nil {
				return nil, "", false, terr
			}
			ids := make([]string, len(msgs))
			for i, m := range msgs {
				ids[i] = m.ID
			}
			parts, terr := r.deps.DB.PartsByMessages(tenant, ids)
			if terr != nil {
				return nil, "", false, terr
			}
			// Re-anchor persistence to the NEW tip: the running turn must
			// continue on the compacted chain, not branch off the old one.
			sink.prevID = newTip
			return rebuildHistory(msgs, parts), newTip, true, nil
		},
	}, Input{
		Tenant: tenant, Session: session, RunID: newID(),
		Model: ModelOf(s.Model), System: plan.System,
		PrevID: tip, History: history,
		Tools: TurnPlanToLLMTools(plan.Tools, plan.Locale),
		Extra: extra,
	})
	if err != nil {
		return res, err
	}
	if res.InputTokens != 0 || res.OutputTokens != 0 {
		_ = r.deps.DB.AddUsage(tenant, session, res.InputTokens, res.OutputTokens)
	}
	return res, nil
}

// contextLimit is the model's context window: an explicit override, else the
// provider row's model metadata, else 0 (proactive compaction disabled).
func (r *Runtime) contextLimit(tenant, ref string) int {
	if r.deps.ContextLimit > 0 {
		return r.deps.ContextLimit
	}
	if r.deps.DB == nil {
		return 0
	}
	if v, ok := modelContextLimit(r.deps.DB, tenant, ref); ok {
		return v
	}
	return 0
}

func firstNonZero(vals ...int) int {
	for _, v := range vals {
		if v > 0 {
			return v
		}
	}
	return 0
}

// persistSink tees the turn loop's events to the emitter AND persists the
// step's parts + chain position, closing the seam between the turn loop (which
// only decides WHAT to emit) and the store.
type persistSink struct {
	rt      *Runtime
	tenant  string
	session string
	prevID  string
	emit    Emitter
	// Grouped by PART id, not one entry per delta: a step's streamed text is one
	// part per id, not one part per frame (see deltaParts).
	texts    map[string]*deltaParts
	reason   map[string]*deltaParts
	tools    map[string][]toolPart
	results  map[string][]toolResultPart
	finalIDs map[string]bool
}

func (s *persistSink) Emit(ctx context.Context, ev Event) error {
	if s.texts == nil {
		s.texts = map[string]*deltaParts{}
		s.reason = map[string]*deltaParts{}
		s.tools = map[string][]toolPart{}
		s.results = map[string][]toolResultPart{}
		s.finalIDs = map[string]bool{}
	}
	mid, _ := ev.Params["message_id"].(string)
	switch ev.Name {
	case "text-delta":
		s.textPart(mid).add(asString(ev.Params["id"]), asString(ev.Params["text"]))
	case "reasoning-delta":
		s.reasonPart(mid).add(asString(ev.Params["id"]), asString(ev.Params["text"]))
	case "tool-call":
		s.tools[mid] = append(s.tools[mid], toolPart{
			ID: asString(ev.Params["toolCallId"]), Name: asString(ev.Params["toolName"]),
			Input: ev.Params["input"],
		})
	case "tool-result":
		s.results[mid] = append(s.results[mid], toolResultPart{
			ToolUseID: asString(ev.Params["toolCallId"]),
			Content:   asString(ev.Params["formatted"]),
		})
		// Persist NOW, not only at message-added.
		//
		// A tool result is emitted AFTER the step's message-added, so storing parts
		// solely on message-added meant `results` was always empty at write time:
		// the database kept 23 tool calls and zero tool_results, the console showed
		// an empty Content block, and history rebuilt from the DB had the same hole.
		//
		// persistStep is idempotent (it replaces the whole part set), so calling it
		// again here simply records what has arrived so far.
		if mid != "" {
			if err := s.persistStep(mid); err != nil {
				return err
			}
		}
	case "message-added":
		if streaming, _ := ev.Params["streaming"].(bool); !streaming && mid != "" && !s.finalIDs[mid] {
			s.finalIDs[mid] = true
			if err := s.persistStep(mid); err != nil {
				return err
			}
		}
	}
	if s.emit != nil {
		return s.emit.Emit(ctx, ev)
	}
	return nil
}

// textPart returns the accumulator for one step's text, creating it on first use.
func (s *persistSink) textPart(mid string) *deltaParts {
	if s.texts[mid] == nil {
		s.texts[mid] = &deltaParts{}
	}
	return s.texts[mid]
}

// reasonPart is textPart for reasoning.
func (s *persistSink) reasonPart(mid string) *deltaParts {
	if s.reason[mid] == nil {
		s.reason[mid] = &deltaParts{}
	}
	return s.reason[mid]
}

// persistStep writes the assistant message + its parts under the SAME id the
// stream used, then advances the chain tip.
func (s *persistSink) persistStep(mid string) error {
	_, err := s.rt.deps.DB.InsertMessage(s.tenant, mid, "assistant", strOrNil(s.prevID), "")
	if err != nil {
		return err
	}
	// Rewrite this step's parts EVERY time, not only when the message row is new.
	//
	// The guard used to be `if created`: the row is created when the step's first
	// event arrives, and the tool RESULT arrives later, at which point `created` is
	// false for every subsequent call — so tool_result parts were never written.
	// The console then showed a tool card with an empty Content area while the
	// result sat in the event stream, and history rebuilt from the DB had the same
	// hole.
	//
	// Replacing is safe because the sinks hold the whole step: the parts are derived
	// from them, so a rewrite is idempotent and always complete.
	s.rt.deps.DB.ReplaceParts(s.tenant, mid, s.partsOf(mid))
	if _, err := s.rt.deps.DB.AppendMessageTip(s.tenant, s.session, mid); err != nil {
		return err
	}
	_ = s.rt.deps.DB.SetSessionPreview(s.tenant, s.session, preview(strings.Join(s.textPart(mid).Texts(), "")))
	s.prevID = mid
	return nil
}

// partsOf renders the accumulated step into the ordered parts to store.
//
// Data is the part's JSON document (the same shape InsertPart used to marshal),
// so a row written here is indistinguishable from one written inline.
func (s *persistSink) partsOf(mid string) []store.Part {
	out := make([]store.Part, 0, 8)
	add := func(typ string, data any) {
		raw, err := json.Marshal(data)
		if err != nil {
			return // an unencodable part is dropped rather than corrupting the row
		}
		out = append(out, store.Part{Type: typ, Data: string(raw)})
	}
	for _, r := range s.reasonPart(mid).Texts() {
		add("reasoning", map[string]any{"text": r})
	}
	for _, t := range s.textPart(mid).Texts() {
		add("text", map[string]any{"text": t})
	}
	for _, tc := range s.tools[mid] {
		add("tool", map[string]any{"id": tc.ID, "name": tc.Name, "input": tc.Input})
	}
	for _, tr := range s.results[mid] {
		add("tool_result", map[string]any{"tool_use_id": tr.ToolUseID, "content": tr.Content})
	}
	return out
}

// preview renders a one-line list preview from message text.
func preview(text string) string {
	t := strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	r := []rune(t)
	if len(r) > 120 {
		return string(r[:120]) + "…"
	}
	return t
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// emitBg emits a background event (folding happens outside a run).
func emitBg(e Emitter, name string, params map[string]any) error {
	if e == nil {
		return nil
	}
	return e.Emit(context.Background(), Event{Name: name, Params: params})
}
