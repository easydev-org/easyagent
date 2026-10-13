package agent

import "context"

// Lease is the per-session run lock: it guarantees at most ONE writer drives a
// session's turn at a time (across replicas). The production implementation is
// backed by a NATS KV bucket; a process-local implementation is used for
// single-replica runs and tests.
type Lease interface {
	// Claim tries to take the lock. ok=false means another holder owns it.
	Claim(ctx context.Context, tenant, session string) (ok bool, err error)
	// Renew extends the held lock. held=false means the lock was LOST (another
	// instance owns it) -> abort the run. A transient error is reported as err
	// with held=true (do NOT treat as lost).
	Renew(ctx context.Context, tenant, session string) (held bool, err error)
	// Release drops the lock (only if still held).
	Release(ctx context.Context, tenant, session string) error
}

// MemoryLease is a process-local Lease (single replica / tests).
type MemoryLease struct {
	held map[string]bool
}

// NewMemoryLease builds a process-local lease.
func NewMemoryLease() *MemoryLease { return &MemoryLease{held: map[string]bool{}} }

func leaseKey(tenant, session string) string { return tenant + "\x00" + session }

func (l *MemoryLease) Claim(_ context.Context, tenant, session string) (bool, error) {
	k := leaseKey(tenant, session)
	if l.held[k] {
		return false, nil
	}
	l.held[k] = true
	return true, nil
}

func (l *MemoryLease) Renew(_ context.Context, tenant, session string) (bool, error) {
	return l.held[leaseKey(tenant, session)], nil
}

func (l *MemoryLease) Release(_ context.Context, tenant, session string) error {
	delete(l.held, leaseKey(tenant, session))
	return nil
}

// LeaseReader reports whether a session's run lease is currently held (by any
// replica). Optional: a Lease may also implement it so the server can report a
// cross-replica busy/idle status.
type LeaseReader interface {
	IsHeld(ctx context.Context, tenant, session string) (bool, error)
}

// IsHeld implements LeaseReader (process-local).
func (l *MemoryLease) IsHeld(_ context.Context, tenant, session string) (bool, error) {
	return l.held[leaseKey(tenant, session)], nil
}

// Heartbeat is a per-instance liveness signal (one key per live agent replica,
// short TTL). It lets other replicas tell a crashed holder from a live one
// immediately, instead of waiting for the lease TTL to expire.
type Heartbeat interface {
	// Start begins beating (idempotent).
	Start(ctx context.Context)
	// Stop halts beating.
	Stop()
}
