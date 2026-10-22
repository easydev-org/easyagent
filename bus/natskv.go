package bus

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// KV bucket names (mirror the TS agent).
const (
	PresetsBucket = "easybus-presets"
	// ConfigBucket MUST be the bucket extensions watch.
	//
	// Extensions subscribe to protocol.ConfigKVBucket ("cfg") and parse
	// `t.<tenant>.<extId>.<name>`. This used to be "easyagent-config", a separate
	// bucket that no extension ever read: the console saved a value, the agent
	// stored it where nothing was looking, and every extension kept reporting the
	// knob unset — most visibly after each restart, when it had no in-memory copy
	// to fall back on.
	//
	// It is defined in terms of the SDK constant so the two cannot drift again.
	ConfigBucket = protocol.ConfigKVBucket
)

// NATSKV is a JetStream-KV implementation of agent.KV: presets + tenant config
// persisted across replicas and restarts. Errors are best-effort (logged): the
// interface has no error return, matching the in-memory KV.
type NATSKV struct {
	b    *Bus
	name string
}

// NewNATSKV builds a KV over a bucket.
func NewNATSKV(b *Bus, bucket string) *NATSKV { return &NATSKV{b: b, name: bucket} }

// Get returns (value, true) when the key exists.
func (k *NATSKV) Get(key string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	v, err := k.b.transport.KVGet(ctx, k.name, key)
	if err != nil {
		// A missing key surfaces as an error on some clients; treat as absent.
		return "", false
	}
	if v == "" {
		return "", false
	}
	return v, true
}

// Put upserts a value.
func (k *NATSKV) Put(key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := k.b.transport.KVPut(ctx, k.name, key, value, 0); err != nil {
		slog.Warn("kv put failed", "bucket", k.name, "key", key, "err", err)
	}
}

// Delete removes a key (a no-op when absent).
func (k *NATSKV) Delete(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := k.b.transport.KVDelete(ctx, k.name, key); err != nil {
		slog.Warn("kv delete failed", "bucket", k.name, "key", key, "err", err)
	}
}

// Keys lists every key with the given prefix, sorted. JetStream KV has no native
// listing, so it reads the bucket's snapshot via a watch (bounded by a short
// deadline) and collects the keys.
func (k *NATSKV) Keys(prefix string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, stop, err := k.b.transport.KvWatch(ctx, k.name, ">")
	if err != nil {
		slog.Warn("kv keys watch failed", "bucket", k.name, "err", err)
		return nil
	}
	defer stop()
	seen := map[string]bool{}
	var out []string
	for {
		select {
		case <-ctx.Done():
			sort.Strings(out)
			return out
		case ev, ok := <-ch:
			if !ok {
				sort.Strings(out)
				return out
			}
			if !ev.Deleted && strings.HasPrefix(ev.Key, prefix) && !seen[ev.Key] {
				seen[ev.Key] = true
				out = append(out, ev.Key)
			}
			if ev.Done {
				sort.Strings(out)
				return out
			}
		}
	}
}

var _ = bus.KvEvent{}
