package agent

import (
	"context"
	"time"
)

// RecoverOnBoot re-wakes every session with a PENDING wake-type mailbox item.
// The lease guarantees exactly one replica processes each; the others no-op.
// Run once at boot (a crash left messages queued).
func (r *Runtime) RecoverOnBoot(ctx context.Context) {
	rows, err := r.deps.DB.PendingSessions(WakeTypes)
	if err != nil {
		return
	}
	for _, row := range rows {
		_ = r.Wake(ctx, row.Tenant, row.SessionName)
	}
}

// Backstop periodically re-wakes pending sessions, catching a wake that was
// dropped (a transient NATS/PG fault) or a crash between the enqueue and the
// claim. It is idempotent (the lease serializes).
type Backstop struct {
	rt       *Runtime
	interval time.Duration
	cancel   context.CancelFunc
}

// NewBackstop builds the periodic recovery loop.
func NewBackstop(rt *Runtime, interval time.Duration) *Backstop {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &Backstop{rt: rt, interval: interval}
}

// Start runs the loop in the background.
func (b *Backstop) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	go func() {
		t := time.NewTicker(b.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.rt.RecoverOnBoot(ctx)
			}
		}
	}()
}

// Stop halts the loop.
func (b *Backstop) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
}
