// Package bus is the NATS/JetStream wiring over abc-protocol-go: the KV-backed
// session lease, the mailbox consumer, and the extension tool executor.
//
// It implements the agent package's `Lease` and `ToolExecutor` seams so the
// runtime is transport-agnostic (tests use the in-memory lease + a stub tool
// executor).
package bus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	agentpkg "github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
	easybus "github.com/easydev-org/easybus-sdk"
	abcagent "github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/events"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/transport/nats"
)

// LeaseTTL is the session run-lease TTL (matches the protocol constant).
const LeaseTTL = 30 * time.Second

// Bus wraps the abc NATS transport + agent role.
type Bus struct {
	transport *nats.Bus
	agent     *abcagent.Agent
	url       string

	mu   sync.Mutex
	held map[string]int64 // session key -> KV revision
}

// Connect dials NATS and builds the agent-side role.
func Connect(url string) (*Bus, error) {
	t, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &Bus{transport: t, agent: abcagent.New(t), url: url, held: map[string]int64{}}, nil
}

// Close shuts the transport down.
func (b *Bus) Close() error { return b.transport.Close() }

// URL is the broker URL this bus is connected to.
func (b *Bus) URL() string { return b.url }

// Agent exposes the long-lived agent role (manifest cache + config authority).
func (b *Bus) Agent() *abcagent.Agent { return b.agent }

// Transport exposes the raw bus (advanced callers).
func (b *Bus) Transport() bus.Bus { return b.transport }

// ---- Lease (implements agentpkg.Lease) ----

// Claim takes the session run lease via KV create (fails when a live lease
// exists). The returned revision is used to renew.
func (b *Bus) Claim(ctx context.Context, tenant, session string) (bool, error) {
	rev, err := b.transport.KVCreate(ctx, abcagent.LeaseBucket,
		protocol.TenantKVKey(tenant, protocol.SessionToken(session)),
		"running", LeaseTTL.Milliseconds())
	if err != nil {
		return false, err
	}
	if rev == 0 {
		return false, nil // another holder owns it
	}
	b.mu.Lock()
	b.held[tenant+"\x00"+session] = rev
	b.mu.Unlock()
	return true, nil
}

// Renew extends the lease via CAS. held=false means the lease was LOST.
func (b *Bus) Renew(ctx context.Context, tenant, session string) (bool, error) {
	b.mu.Lock()
	rev := b.held[tenant+"\x00"+session]
	b.mu.Unlock()
	next, err := b.transport.KVCas(ctx, abcagent.LeaseBucket,
		protocol.TenantKVKey(tenant, protocol.SessionToken(session)),
		"running", rev)
	if err != nil {
		// Transient error: NOT a loss (the caller retries next tick).
		return true, err
	}
	if next == 0 {
		return false, nil // lost
	}
	b.mu.Lock()
	b.held[tenant+"\x00"+session] = next
	b.mu.Unlock()
	return true, nil
}

// Release drops the lease.
func (b *Bus) Release(ctx context.Context, tenant, session string) error {
	b.mu.Lock()
	delete(b.held, tenant+"\x00"+session)
	b.mu.Unlock()
	return b.transport.KVDelete(ctx, abcagent.LeaseBucket,
		protocol.TenantKVKey(tenant, protocol.SessionToken(session)))
}

// IsHeld reports whether a session's run lease is currently held (any replica).
func (b *Bus) IsHeld(ctx context.Context, tenant, session string) (bool, error) {
	v, err := b.transport.KVGet(ctx, abcagent.LeaseBucket,
		protocol.TenantKVKey(tenant, protocol.SessionToken(session)))
	if err != nil {
		return false, err
	}
	return v != "", nil
}

// Heartbeat beats this instance's liveness key in the owner bucket, so other
// replicas can tell a crashed holder from a live one immediately (instead of
// waiting for the lease TTL).
type Heartbeat struct {
	bucket string
	key    string
	ttlMs  int64
	cancel context.CancelFunc
}

// HeartbeatTTL is the owner-heartbeat TTL (mirrors the TS agent).
const HeartbeatTTL = 30 * time.Second

// OwnerBucket is the per-instance heartbeat bucket (one key per live replica).
// Mirrors the TS agent's `easybus-session-owner`.
const OwnerBucket = "easybus-session-owner"

// NewHeartbeat builds a heartbeat keyed by instanceID.
func (b *Bus) NewHeartbeat(instanceID string) *Heartbeat {
	if instanceID == "" {
		instanceID = "agent-" + newCallID()
	}
	return &Heartbeat{
		bucket: OwnerBucket,
		key:    instanceID,
		ttlMs:  HeartbeatTTL.Milliseconds(),
	}
}

// Start begins beating (TTL/3). No-op if already started.
func (h *Heartbeat) Start(ctx context.Context, b *Bus) {
	if h.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	beat := func() {
		// KVCreate refreshes only when absent; KVDelete+KVCreate is racy, so
		// use KVPut which upserts with a TTL.
		_ = b.transport.KVPut(ctx, h.bucket, h.key, "alive", h.ttlMs)
	}
	beat()
	go func() {
		t := time.NewTicker(HeartbeatTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				beat()
			}
		}
	}()
}

// Stop halts beating and removes the key.
func (h *Heartbeat) Stop() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
}

// WatchInterrupts streams the session's mailbox wake types while a turn runs,
// so an `interrupt` published by another replica aborts it.
func (b *Bus) WatchInterrupts(ctx context.Context, tenant, session string) (<-chan string, func(), error) {
	sub, err := b.transport.Subscribe(ctx, protocol.ChMailbox(tenant, session), bus.SubscribeOpts{})
	if err != nil {
		return nil, nil, err
	}
	out := make(chan string, 16)
	go func() {
		defer close(out)
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			if m, ok := env.Payload.AsInterface().(map[string]any); ok {
				if t, _ := m["type"].(string); t != "" {
					select {
					case out <- t:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, func() { _ = sub.Close() }, nil
}

// ---- Tool executor (implements agentpkg.ToolExecutor) ----

// ToolExecutor calls extension tools over the bus. It records the owning
// extension id per tool name (from discovery) so a call routes to
// `tool.call.<extId>.<name>`.
type ToolExecutor struct {
	agent *abcagent.Agent

	mu    sync.RWMutex
	owner map[string]string // bare tool name -> extension id
	defs  map[string]agentpkg.ToolDef
	// extFields maps an extension id -> its declared config surface (from the
	// discovered manifest), so the server can list + edit extension config
	// without re-discovering. Populated by Refresh.
	extFields map[string][]bundled.ConfigField
}

// NewToolExecutor builds a tool executor over the agent role.
func NewToolExecutor(a *abcagent.Agent) *ToolExecutor {
	return &ToolExecutor{agent: a, owner: map[string]string{}, defs: map[string]agentpkg.ToolDef{}, extFields: map[string][]bundled.ConfigField{}}
}

// Refresh discovers extensions and records tool -> extension ownership.
func (e *ToolExecutor) Refresh(ctx context.Context, maxWaitMs int) error {
	manifests, err := e.agent.Discover(ctx, maxWaitMs)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, m := range manifests {
		// Record the extension's declared config surface (for the config page),
		// independent of whether it declares any tool.
		e.extFields[m.Id] = manifestConfigFields(m)
		if m.Tools == nil {
			continue
		}
		for _, t := range m.Tools {
			e.owner[t.Name] = m.Id
			def := agentpkg.ToolDef{Name: t.Name, ExtID: m.Id, Description: t.Description}
			if t.InputSchema != nil {
				def.InputSchema = t.InputSchema.AsMap()
			}
			def.Descriptions = t.Descriptions
			if t.RequiredConfig != nil {
				def.RequiredConfig = t.RequiredConfig
			}
			e.defs[t.Name] = def
		}
	}
	return nil
}

// manifestConfigFields maps a discovered extension's declared config items onto
// the server's ConfigField shape (kind/scope/enum/default preserved).
func manifestConfigFields(m *easybus.ExtensionManifest) []bundled.ConfigField {
	if m == nil || len(m.Config) == 0 {
		return nil
	}
	out := make([]bundled.ConfigField, 0, len(m.Config))
	for _, c := range m.Config {
		kind := "value"
		if c.Kind != nil && *c.Kind != "" {
			kind = *c.Kind
		}
		typ := c.Type
		if typ == "" {
			typ = "string"
		}
		scope := c.Scope
		if scope == "" {
			scope = "global"
		}
		f := bundled.ConfigField{Name: c.Name, Type: typ, Scope: scope, Kind: kind}
		if c.Description != nil {
			f.Description = *c.Description
		}
		if c.Capability != nil {
			f.Capability = *c.Capability
		}
		// A secret's default is never surfaced (nor broadcast).
		if c.Default != nil && kind != bundled.ConfigKindSecret {
			if s, ok := c.Default.AsInterface().(string); ok {
				f.Default = s
			}
		}
		// Carry the SHAPE of a structured knob so the console can render a form.
		// Without this a `list`/`object` knob degrades to a JSON textarea, which is
		// exactly the hand-editing this exists to remove.
		f.Fields = configFieldSpecs(c.Fields)
		if c.ItemType != nil {
			f.ItemType = *c.ItemType
		}
		if c.MinItems != nil {
			f.MinItems = int(*c.MinItems)
		}
		if c.MaxItems != nil {
			f.MaxItems = int(*c.MaxItems)
		}
		out = append(out, f)
	}
	return out
}

// configFieldSpecs maps the wire schema onto the agent's own type.
//
// It recurses the same shallow depth the extension declared: a field may be an
// object, and the console renderer/validator stop there on purpose.
func configFieldSpecs(in []*easybus.ConfigFieldSchema) []bundled.ConfigFieldSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]bundled.ConfigFieldSpec, 0, len(in))
	for _, f := range in {
		spec := bundled.ConfigFieldSpec{
			Name: f.GetName(), Type: f.GetType(), Required: f.GetRequired(),
			EnumValues: f.GetEnumValues(), Descriptions: f.GetDescriptions(),
		}
		if f.Kind != nil {
			spec.Kind = *f.Kind
		}
		if f.Format != nil {
			spec.Format = *f.Format
		}
		if f.Description != nil {
			spec.Description = *f.Description
		}
		if f.Default != nil {
			spec.Default = fmt.Sprintf("%v", f.Default.AsInterface())
		}
		spec.Fields = configFieldSpecs(f.GetFields())
		out = append(out, spec)
	}
	return out
}

// Execute calls one tool and maps the abc ToolResult to the agent's ToolOutcome.
func (e *ToolExecutor) Execute(ctx context.Context, tenant, session, name, args string) (agentpkg.ToolOutcome, error) {
	// The model may call the extension-QUALIFIED `<extId>-<name>` form; resolve
	// it back to the bare name + owning extension.
	extID, bare := e.resolve(name)
	if extID == "" {
		// Fall back to a discovery refresh (a tool may have appeared since boot).
		_ = e.Refresh(ctx, 500)
		extID, bare = e.resolve(name)
		if extID == "" {
			return agentpkg.ToolOutcome{}, &UnknownToolError{Name: name}
		}
	}
	var input map[string]any
	if args != "" {
		_ = json.Unmarshal([]byte(args), &input)
	}
	res, err := e.agent.CallTool(ctx, tenant, session, extID, bare, newCallID(), input)
	if err != nil {
		return agentpkg.ToolOutcome{}, err
	}
	if res.Error != nil {
		return agentpkg.ToolOutcome{}, &ToolError{Message: res.Error.Message}
	}
	return agentpkg.ToolOutcome{Content: res.Content, Metadata: res.Data}, nil
}

// resolve maps a model-emitted name onto (extId, bareName). It accepts both the
// bare name and the qualified `<extId>-<name>` form.
func (e *ToolExecutor) resolve(name string) (extID, bare string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if id, ok := e.owner[name]; ok {
		return id, name
	}
	// Qualified form: split on the LAST '-' and match a known (extId, name).
	for bareName, id := range e.owner {
		if name == id+"-"+bareName {
			return id, bareName
		}
	}
	return "", ""
}

// Tools returns the discovered tool definitions (with description + schema).
func (e *ToolExecutor) Tools() []agentpkg.ToolDef {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]agentpkg.ToolDef, 0, len(e.defs))
	for _, d := range e.defs {
		out = append(out, d)
	}
	return out
}

// ExtensionConfigFields returns an extension's declared config surface (from
// the last Refresh), or nil when the extension is unknown / declares none.
func (e *ToolExecutor) ExtensionConfigFields(extID string) []bundled.ConfigField {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.extFields[extID]
}

// ExtensionIDs returns the discovered extension ids (sorted for stability).
func (e *ToolExecutor) ExtensionIDs() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.extFields))
	for id := range e.extFields {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Lookup returns the definition of a bare tool name.
func (e *ToolExecutor) Lookup(name string) (agentpkg.ToolDef, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	d, ok := e.defs[name]
	return d, ok
}

// OwnerOf returns the extension owning a tool (bare name).
func (e *ToolExecutor) OwnerOf(name string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	id, ok := e.owner[name]
	return id, ok
}

// ToolRegistry adapts the bus's discovered tools to the agent.ToolRegistry
// seam, so the turn loop can offer them to the model.
type ToolRegistry struct {
	exec *ToolExecutor
}

// NewToolRegistry builds a registry over a tool executor.
func NewToolRegistry(exec *ToolExecutor) *ToolRegistry { return &ToolRegistry{exec: exec} }

// Exec returns the underlying executor (for ListTools/config surfaces).
func (r *ToolRegistry) Exec() *ToolExecutor { return r.exec }

// Tools returns the discovered tools as agent.ToolDefs.
func (r *ToolRegistry) Tools() []agentpkg.ToolDef {
	return r.exec.Tools()
}

// UnknownToolError is returned when no discovered extension owns the tool.
type UnknownToolError struct{ Name string }

func (e *UnknownToolError) Error() string { return "unknown tool: " + e.Name }

// Unwrap marks this as agent.ErrUnknownTool so a composite executor can fall
// through to the next source.
func (e *UnknownToolError) Unwrap() error { return agentpkg.ErrUnknownTool }

// ToolError is a tool that ran but reported an error.
type ToolError struct{ Message string }

func (e *ToolError) Error() string { return e.Message }

// ---- Mailbox consumer ----

// MailboxMessage is a consumed mailbox envelope (transport-agnostic).
type MailboxMessage struct {
	ID          string
	Tenant      string
	SessionName string
	Type        string
	Payload     any
	Source      string
}

// ConsumeMailbox subscribes to the durable mailbox stream and invokes handler
// for each message (exactly-once per replica via the durable consumer + queue
// group). Returns a stop function.
func (b *Bus) ConsumeMailbox(ctx context.Context, handler func(MailboxMessage) error) (func(), error) {
	return b.agent.ConsumeMailbox(ctx, func(m abcagent.MailboxMessageResolved) error {
		return handler(MailboxMessage{
			ID: m.ID, Tenant: m.Tenant, SessionName: m.SessionName,
			Type: m.Type, Payload: m.Payload, Source: m.Source,
		})
	}, 1000)
}

// PublishMailbox publishes a durable mailbox message (used by the Prompt route
// and subsession hand-off tools).
func (b *Bus) PublishMailbox(ctx context.Context, tenant, session, typ string, payload any, source string) error {
	return b.agent.PublishMailbox(ctx, tenant, session, typ, payload, source)
}

// PublishLifecycle broadcasts a session lifecycle event (`created` / `forked` /
// `renamed` / `deleted`) on `easybus.<tenant>.session.lifecycle.<kind>`, so external
// services (repo/workspace extensions) can react. Best-effort: consumers must
// tolerate a missed event and converge via their own reconciliation.
func (b *Bus) PublishLifecycle(ctx context.Context, tenant, kind, session string, payload map[string]any) error {
	var k = events.Deleted
	switch kind {
	case "created":
		k = events.Created
	case "forked":
		k = events.Forked
	case "renamed":
		k = events.Renamed
	}
	// Durable (inbox) publish, matching the TS agent: consumers may replay.
	return events.LifecyclePublish(ctx, b.transport, tenant, k, session, payload, events.LifecyclePublishOpts{Inbox: true})
}

// ---- Session events (durable, cross-replica) ----

// PublishSessionEvent publishes one session event frame onto the durable
// session-events stream (`easybus.<tenant>.session.events.<token>`), so ANY replica
// (and any reconnecting watcher) receives it.
func (b *Bus) PublishSessionEvent(ctx context.Context, tenant, session, id string, payload any) error {
	return b.transport.InboxPublish(ctx, protocol.ChSessionEvents(tenant, session), payload,
		bus.InboxPublishOpts{ID: id, SessionName: session, Tenant: tenant})
}

// SubscribeSessionEvents subscribes to a session's durable event stream. If
// sinceSeq > 0 it first replays retained frames at/after that sequence, then
// streams live (deduping by id). Returns a channel + a cancel func.
func (b *Bus) SubscribeSessionEvents(ctx context.Context, tenant, session string, sinceSeq uint64) (<-chan []byte, func(), error) {
	subject := protocol.ChSessionEvents(tenant, session)
	out := make(chan []byte, 128)
	sub, err := b.transport.Subscribe(ctx, subject, bus.SubscribeOpts{})
	if err != nil {
		return nil, nil, err
	}
	// Replay the retained window (best-effort).
	seen := map[string]bool{}
	if sinceSeq > 0 {
		if envs, rerr := b.transport.Replay(ctx, subject); rerr == nil {
			for _, e := range envs {
				id := derefStr(e.Id)
				raw, err := json.Marshal(e.Payload)
				if err != nil {
					continue
				}
				if id != "" && seen[id] {
					continue
				}
				if id != "" {
					seen[id] = true
				}
				select {
				case out <- raw:
				default:
				}
			}
		}
	}
	go func() {
		defer close(out)
		for {
			env, ok := sub.Next(ctx)
			if !ok {
				return
			}
			id := derefStr(env.Id)
			if id != "" && seen[id] {
				continue
			}
			if id != "" {
				seen[id] = true
			}
			raw, err := json.Marshal(env.Payload)
			if err != nil {
				continue
			}
			select {
			case out <- raw:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, func() { _ = sub.Close() }, nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func newCallID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "call_" + hex.EncodeToString(b[:])
}
