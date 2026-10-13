package bus

import (
	"context"
	"log/slog"
	"time"
)

// PresenceBucket is the extension-liveness KV bucket (mirrors the protocol).
const PresenceBucket = "abc-presence"

// presenceInitTTLMs is the per-key TTL used to pre-create the presence bucket.
// A NATS KV bucket's TTL is fixed at CREATION (per-key TTLs must match it), so
// this MUST equal the protocol's `extension.PresenceTTL` (15s) — otherwise the
// bucket's TTL would diverge and offline extensions would linger. It is also
// >= 100ms: NATS rejects a KV max-age below that, so a 1ms value silently
// fails and the bucket is never created.
const presenceInitTTLMs = 15_000

// EnsurePresenceBucket pre-creates the extension presence KV bucket. It works
// around an abc-protocol-go bug: `Agent.ensurePresence` calls `KvWatch` on this
// bucket, and when the bucket does not yet exist `KvWatch` errors; the library
// then reassigns `a.presenceOnce` INSIDE `sync.Once.Do`, which panics
// (`sync: unlock of unlocked mutex`). Creating the bucket first makes the watch
// succeed. The bucket carries no TTL (extensions set a per-key TTL); the init
// key is written with a short-but-legal TTL and deleted immediately.
func (b *Bus) EnsurePresenceBucket(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := b.transport.KVPut(ctx, PresenceBucket, "agent-go/init", "{}", presenceInitTTLMs); err != nil {
		slog.Warn("presence bucket init failed", "err", err)
		return err
	}
	_ = b.transport.KVDelete(ctx, PresenceBucket, "agent-go/init")
	return nil
}
