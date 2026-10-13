package agent

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/easydev-org/easyagent/store"
)

// IdleWatch re-triggers a session that stopped mid-task after a tool call: every
// sweep it walks each tenant's sessions and, for a session that is IDLE, whose
// LATEST message is an assistant step ending on a tool_result (the model ran a
// tool then produced no text), and which has a model configured, publishes a
// `trigger` (source `system:idlewatch`) telling the model to continue or wrap up.
//
// Safety: a session whose tip does NOT advance across consecutive nudges is
// backed off (the nudge is not helping — the provider is rate-limiting or the
// turn keeps failing). The counter resets as soon as the tip changes.
type IdleWatch struct {
	rt        *Runtime
	interval  time.Duration
	tenant    string
	maxNoProg int

	mu     sync.Mutex
	nudge  map[string]nudgeState
	cancel context.CancelFunc
}

type nudgeState struct {
	tip        string
	noProgress int
}

// NewIdleWatch builds the watchdog.
func NewIdleWatch(rt *Runtime, tenant string, interval time.Duration) *IdleWatch {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &IdleWatch{rt: rt, tenant: tenant, interval: interval, maxNoProg: 3, nudge: map[string]nudgeState{}}
}

// Start begins the sweep loop in the background.
func (w *IdleWatch) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	go func() {
		t := time.NewTicker(w.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.Sweep(ctx)
			}
		}
	}()
}

// Stop halts the loop.
func (w *IdleWatch) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// Sweep runs one pass (exported for tests).
func (w *IdleWatch) Sweep(ctx context.Context) {
	sessions, err := w.rt.deps.DB.ListSessions(w.tenant)
	if err != nil {
		return
	}
	for _, s := range sessions {
		if w.rt.IsActive(w.tenant, s.Name) || s.Model == "" {
			continue
		}
		tip := ""
		if s.TipID != nil {
			tip = *s.TipID
		}
		if tip == "" || !w.stuckOnToolResult(tip) {
			continue
		}
		if w.backedOff(s.Name, tip) {
			continue
		}
		_, _ = w.rt.deps.DB.Enqueue(w.tenant, s.Name, "trigger", WakePayload{
			Type: "trigger", MessageID: newUUID(),
			Text: "Continue the task. If it is complete, reply with a short summary.",
		}, "system:idlewatch")
		_ = w.rt.Wake(ctx, w.tenant, s.Name)
	}
}

// stuckOnToolResult reports whether the tip is an assistant step whose last
// meaningful part is a tool_result (no text after the tool ran).
func (w *IdleWatch) stuckOnToolResult(tip string) bool {
	m, ok, err := w.rt.deps.DB.GetMessage(w.tenant, tip)
	if err != nil || !ok || m.Role != "assistant" {
		return false
	}
	parts, err := w.rt.deps.DB.PartsByMessages(w.tenant, []string{tip})
	if err != nil {
		return false
	}
	var sawToolResult, sawText bool
	for _, p := range parts {
		switch p.Type {
		case "tool_result":
			sawToolResult = true
		case "text":
			var d textPart
			if json.Unmarshal([]byte(p.Data), &d) == nil && d.Text != "" {
				sawText = true
			}
		}
	}
	return sawToolResult && !sawText
}

// backedOff reports whether the session should be skipped this sweep (its tip
// has not advanced across too many nudges).
func (w *IdleWatch) backedOff(session, tip string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.nudge[session]
	if st.tip == tip {
		st.noProgress++
	} else {
		st = nudgeState{tip: tip, noProgress: 1}
	}
	w.nudge[session] = st
	return st.noProgress > w.maxNoProg
}

var _ = store.Session{}
