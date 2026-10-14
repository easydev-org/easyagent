package agent

import (
	"context"
	"sync"
	"time"
)

// resumableReasons are the turn-end reasons that warrant an AUTOMATIC resume —
// and only those. Deliberately NARROW:
//
//	length    — the model was truncated (output hit max_tokens) mid-thought;
//	             it should continue.
//	retryable — a transient failure (transport / 408/409/429/5xx); retrying the
//	             turn may succeed.
//
// NOT resumed (intentionally):
//
//	stop       — finished naturally;
//	max-steps  — the step budget is spent (the last step already forced a wrap-up);
//	interrupted— the USER asked to stop;
//	locklost   — another replica owns the run now (resuming would double-drive);
//	overflow   — handled by compaction, not by re-running the turn;
//	error      — a permanent failure (bad request / auth / unknown model) — a
//	             blind retry would loop.
var resumableReasons = map[string]bool{"length": true, "retryable": true}

// ResumeWatch is the reason-driven replacement for the old structural
// IdleWatch: every sweep it walks each tenant's sessions and, for an IDLE
// session whose last turn ended with a RESUMABLE reason, publishes a `trigger`
// (source `system:resume`) telling the model to continue.
//
// It supersedes the "tip ends on a tool_result" heuristic, which was a coarse
// proxy for "the turn did not finish": that proxy MISSED a truncated `length`
// turn (its tip carries text) and MISFIRED on an `interrupted`/`locklost` turn
// parked on a tool result. The recorded reason is exact.
type ResumeWatch struct {
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

// NewResumeWatch builds the watchdog.
func NewResumeWatch(rt *Runtime, tenant string, interval time.Duration) *ResumeWatch {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ResumeWatch{rt: rt, tenant: tenant, interval: interval, maxNoProg: 3, nudge: map[string]nudgeState{}}
}

// Start begins the sweep loop in the background.
func (w *ResumeWatch) Start(ctx context.Context) {
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
func (w *ResumeWatch) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// Sweep runs one pass (exported for tests).
func (w *ResumeWatch) Sweep(ctx context.Context) {
	sessions, err := w.rt.deps.DB.ListSessions(w.tenant)
	if err != nil {
		return
	}
	for _, s := range sessions {
		if w.rt.IsActive(w.tenant, s.Name) || s.Model == "" {
			continue
		}
		if !resumableReasons[s.LastTurnReason] {
			continue
		}
		tip := ""
		if s.TipID != nil {
			tip = *s.TipID
		}
		// Back off when the tip does not advance across consecutive resumes (the
		// retry is not helping — e.g. the provider is still failing).
		if w.backedOff(s.Name, tip) {
			continue
		}
		_, _ = w.rt.deps.DB.Enqueue(w.tenant, s.Name, "trigger", WakePayload{
			Type: "trigger", MessageID: newUUID(),
			Text: resumeText(s.LastTurnReason),
		}, "system:resume")
		_ = w.rt.Wake(ctx, w.tenant, s.Name)
	}
}

// resumeText is the nudge for a resumable reason.
func resumeText(reason string) string {
	if reason == "length" {
		return "Your previous reply was cut off by the output-length limit. Continue exactly where you left off, then finish."
	}
	return "The previous attempt failed with a transient error. Continue the task; if it is complete, reply with a short summary."
}

// backedOff reports whether the session should be skipped this sweep (its tip
// has not advanced across too many resumes).
func (w *ResumeWatch) backedOff(session, tip string) bool {
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
