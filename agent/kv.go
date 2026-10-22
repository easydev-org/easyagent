package agent

import (
	"encoding/json"
	"sort"
	"sync"
)

// KV is a small key/value store for presets + tenant config. The production
// implementation is NATS JetStream KV (see the bus package); MemoryKV is the
// single-replica / test implementation.
type KV interface {
	Get(key string) (string, bool)
	Put(key string, value string)
	Delete(key string)
	// Keys returns every key with the given prefix, sorted.
	Keys(prefix string) []string
}

// MemoryKV is an in-process KV.
type MemoryKV struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewMemoryKV builds an in-process KV.
func NewMemoryKV() *MemoryKV { return &MemoryKV{m: map[string]string{}} }

func (k *MemoryKV) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.m[key]
	return v, ok
}

func (k *MemoryKV) Put(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = value
}

func (k *MemoryKV) Delete(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, key)
}

func (k *MemoryKV) Keys(prefix string) []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	var out []string
	for key := range k.m {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// PresetRow is a preset as stored/served. A user preset may SHADOW a system one
// with the same id (see the two keyspaces below).
type PresetRow struct {
	ID               string            `json:"id"`
	SystemPrompt     string            `json:"system_prompt"`
	SystemPromptI18N map[string]string `json:"system_prompt_i18n,omitempty"`
	Tools            []string          `json:"tools"`
	MaxTurns         int               `json:"max_turns"`
	IsSystem         bool              `json:"is_system"`
	IsUserOverride   bool              `json:"is_user_override,omitempty"`
}

// Presets lives in TWO keyspaces per tenant so a user preset can shadow a
// system one:
//
//	system (immutable, deployment-seeded): t.<tenant>.<id>
//	user   (editable, overrides on read):  t.<tenant>.u.<id>
//
// Get/List read BOTH and prefer the user row. Upsert ALWAYS writes the USER
// keyspace. Delete removes ONLY the user row (deleting an override = "restore
// the system version").
type Presets struct {
	kv KV
}

// NewPresets builds the presets service over a KV.
func NewPresets(kv KV) *Presets { return &Presets{kv: kv} }

func sysKey(tenant, id string) string  { return "t." + tenant + "." + id }
func userKey(tenant, id string) string { return "t." + tenant + ".u." + id }

// Get returns the effective preset (user override wins), or false when absent.
func (p *Presets) Get(tenant, id string) (PresetRow, bool) {
	if raw, ok := p.kv.Get(userKey(tenant, id)); ok {
		if r, err := decodePreset(raw); err == nil {
			r.IsUserOverride = true
			r.IsSystem = false
			return r, true
		}
	}
	if raw, ok := p.kv.Get(sysKey(tenant, id)); ok {
		if r, err := decodePreset(raw); err == nil {
			r.IsSystem = true
			return r, true
		}
	}
	return PresetRow{}, false
}

// List returns every effective preset for the tenant, id-sorted.
func (p *Presets) List(tenant string) []PresetRow {
	ids := map[string]bool{}
	for _, k := range p.kv.Keys("t." + tenant + ".") {
		ids[presetIDOf(tenant, k)] = true
	}
	out := make([]PresetRow, 0, len(ids))
	for id := range ids {
		if id == "" || id == "u" {
			continue
		}
		if r, ok := p.Get(tenant, id); ok {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// presetIDOf extracts the id from a tenant key (handles the `u.` prefix).
func presetIDOf(tenant, key string) string {
	rest := key[len("t."+tenant+"."):]
	if len(rest) > 2 && rest[:2] == "u." {
		return rest[2:]
	}
	return rest
}

// Upsert writes the USER keyspace (a user preset may reuse a system id to
// override it).
func (p *Presets) Upsert(tenant string, r PresetRow) {
	r.IsSystem = false
	r.IsUserOverride = true
	raw, _ := json.Marshal(r)
	p.kv.Put(userKey(tenant, r.ID), string(raw))
}

// Delete removes ONLY the user row (restoring a system version if one exists).
func (p *Presets) Delete(tenant, id string) { p.kv.Delete(userKey(tenant, id)) }

// SeedSystem writes the SYSTEM keyspace (a deployment refresh never clobbers an
// override).
func (p *Presets) SeedSystem(tenant string, r PresetRow) {
	r.IsSystem = true
	r.IsUserOverride = false
	raw, _ := json.Marshal(r)
	p.kv.Put(sysKey(tenant, r.ID), string(raw))
}

func decodePreset(raw string) (PresetRow, error) {
	var r PresetRow
	err := json.Unmarshal([]byte(raw), &r)
	return r, err
}

// Config is a tenant-scoped string config store (e.g. `default_model`,
// `locale`).
type Config struct {
	kv KV
	mu sync.Mutex
	// namespaces maps "<extID>.<knob>" -> the namespace that knob was DECLARED
	// in, so a read addresses the same key a write used. Filled from the
	// extensions' manifests; an unregistered knob reads as extension-level.
	namespaces map[string]string
}

// NewConfig builds the config service over a KV.
func NewConfig(kv KV) *Config { return &Config{kv: kv} }

// SetKnobNamespaces records the declared namespace per "<extID>.<knob>".
func (c *Config) SetKnobNamespaces(m map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.namespaces = m
}

// knobNamespace is the declared namespace for one knob ("" = extension level).
func (c *Config) knobNamespace(extID, name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.namespaces[extID+"."+name]
}

// ConfigExt reads a knob in its owning extension's scope.
//
// There is deliberately NO fallback to the tenant-wide key: a value stored by a
// different extension must never satisfy this one, which is the entire point of
// the namespaced key.
func (c *Config) ConfigExt(tenant, extID, name string) (string, bool) {
	return c.GetExt(tenant, extID, c.knobNamespace(extID, name), name)
}

// cfgKey is the storage address of one config value.
//
// The key is (tenant, extId, namespace, name) — NOT just (tenant, name).
//
// WHY the extension is in the key: the old flat `cfg.<tenant>.<name>` was a
// single global namespace, so two extensions that declared a knob with the same
// name silently shared one stored value. Setting `sandboxes` for git made the
// sandbox extension report it as configured too. Namespacing by extension makes
// that impossible.
//
// A knob with an empty extId keeps the LEGACY address (`cfg.<tenant>.<name>`):
// that is the tenant-wide tier (locale, default_model, ...) which no extension
// owns, and it is also what the migration reads.
//
// A knob with an empty namespace is EXTENSION-level: shared by every namespace
// of that extension, which is what an endpoint/token knob wants.
// cfgKey is the storage address of one config value.
//
// The prefix is "t." and NOT a bucket-specific word, because this KV bucket is
// watched directly by every extension: the SDK subscribes to `t.*.<extId>.>` and
// parses `t.<tenant>.<extId>.<name>`. Writing `cfg.<tenant>...` meant the pattern
// never matched, so an extension NEVER received a config snapshot — it only got
// the one-off push sent at save time, and lost everything on restart. That is why
// a freshly restarted extension reported "not configured" for knobs the console
// showed as set.
func cfgKey(tenant, extID, namespace, name string) string {
	switch {
	case extID == "":
		return "t." + tenant + "." + name
	case namespace == "":
		return "t." + tenant + "." + extID + "." + name
	default:
		return "t." + tenant + "." + extID + "." + namespace + "." + name
	}
}

// legacyCfgKey is the address this table used before the prefix matched the
// extension's watch pattern. Reads fall back to it so values saved by an older
// build are not silently lost; the next write moves them to the new key.
func legacyCfgKey(tenant, extID, namespace, name string) string {
	switch {
	case extID == "":
		return "cfg." + tenant + "." + name
	case namespace == "":
		return "cfg." + tenant + "." + extID + "." + name
	default:
		return "cfg." + tenant + "." + extID + "." + namespace + "." + name
	}
}

// Get returns (value, true) or ("", false) for a TENANT-wide knob (no
// extension). Extension knobs use GetExt.
func (c *Config) Get(tenant, key string) (string, bool) {
	if v, ok := c.kv.Get(cfgKey(tenant, "", "", key)); ok {
		return v, true
	}
	return c.kv.Get(legacyCfgKey(tenant, "", "", key))
}

// Set writes a tenant-wide config value.
func (c *Config) Set(tenant, key, value string) {
	c.kv.Put(cfgKey(tenant, "", "", key), value)
}

// GetExt returns an extension-scoped knob's value.
func (c *Config) GetExt(tenant, extID, namespace, key string) (string, bool) {
	if v, ok := c.kv.Get(cfgKey(tenant, extID, namespace, key)); ok {
		return v, true
	}
	return c.kv.Get(legacyCfgKey(tenant, extID, namespace, key))
}

// SetExt writes an extension-scoped knob's value.
func (c *Config) SetExt(tenant, extID, namespace, key, value string) {
	c.kv.Put(cfgKey(tenant, extID, namespace, key), value)
}

// KeysWithPrefix exposes the underlying keys (the one-time namespace migration
// needs to enumerate what exists).
func (c *Config) KeysWithPrefix(prefix string) []string { return c.kv.Keys(prefix) }
