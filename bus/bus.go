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
	"github.com/easydev-org/easyagent/store"
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
	// db is the mailbox queue PublishMailbox appends to before signalling.
	//
	// An interface, not *store.DB: this package must not depend on the storage layer, and a test
	// needs to supply its own. nil means "no local queue" — a bare bus used only for transport.
	db MailboxQueue

	mu   sync.Mutex
	held map[string]int64 // session key -> KV revision
}

// MailboxQueue is the durable queue a mailbox message is written to.
//
// Declared here (rather than taking *store.DB) so this package stays independent of storage:
// PublishMailbox needs exactly one method, and saying so is more honest than accepting the whole
// database handle.
type MailboxQueue interface {
	Enqueue(tenant, session, typ string, payload any, source string) (string, error)
}

// WithMailboxQueue installs the durable queue PublishMailbox writes to.
//
// Without it PublishMailbox would only SIGNAL — which is the defect this method exists to make
// impossible to configure by accident: a bus with no queue panics on the first publish rather
// than silently delivering nothing.
func (b *Bus) WithMailboxQueue(q MailboxQueue) *Bus {
	b.db = q
	return b
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
	// extNS maps an extension id -> its DECLARED namespaces (in author order).
	extNS map[string][]bundled.Namespace
	// extPerms maps an extension id -> the permissions it can require. A client renders
	// the grant form from this, so it never has to know what any extension means.
	extPerms map[string][]bundled.PermissionDecl
	// extChans maps an extension id -> the channels it publishes to. A client offers a
	// subscription from this instead of asking the user to invent a pattern.
	extChans map[string][]bundled.ChannelDecl
	// grants supplies a session's authorisations. nil = no permission enforcement.
	//
	// Injected rather than held as a *store.DB so this package stays independent of the
	// storage layer, and so a test can hand in a fixed set.
	grants GrantSource
}

// GrantSource supplies a session's authorisations, grouped by extension.
//
// It returns the OPAQUE params text; the executor never looks inside. Whether a grant
// covers a particular call is the extension's decision.
type GrantSource func(ctx context.Context, tenant, session string) (map[string][]store.GrantSnapshot, error)

// NewToolExecutor builds a tool executor over the agent role.
func NewToolExecutor(a *abcagent.Agent) *ToolExecutor {
	return &ToolExecutor{agent: a, owner: map[string]string{}, defs: map[string]agentpkg.ToolDef{}, extFields: map[string][]bundled.ConfigField{}, extNS: map[string][]bundled.Namespace{}, extPerms: map[string][]bundled.PermissionDecl{}, extChans: map[string][]bundled.ChannelDecl{}}
}

// WithGrants installs the authorisation source. Without one, permission-gated tools are
// refused rather than silently allowed: a deployment that has not wired the store must not
// hand out capability it cannot check.
func (e *ToolExecutor) WithGrants(g GrantSource) *ToolExecutor {
	e.grants = g
	return e
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
		// The declared grouping (pvc, build, ...), so a client can lay the surface
		// out in the author's order instead of one flat list.
		e.extNS[m.Id] = manifestNamespaces(m)
		// What this extension can require of a caller. A client renders the grant form
		// from this, which is what keeps the permission UI generic.
		e.extPerms[m.Id] = manifestPermissions(m)
		// What this extension publishes, so a client can suggest a subscription instead of
		// asking the user to invent a pattern.
		e.extChans[m.Id] = manifestChannels(m)
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
			if t.Namespace != nil {
				def.Namespace = *t.Namespace
			}
			// Permission keys the tool needs. Kept as declared: the agent only tests
			// presence, and matching against a call's target happens in the extension.
			def.Requires = t.Requires
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
		// The declared format travels with the knob so the validator can check the
		// value exactly. Without it the agent can only guess from the knob's name.
		if c.Format != nil {
			f.Format = *c.Format
		}
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
		// The namespace is what scopes the stored key, so it has to survive the
		// manifest hop; without it every knob of an extension collapses into one
		// flat namespace again.
		if c.Namespace != nil {
			f.Namespace = *c.Namespace
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
func (e *ToolExecutor) Execute(ctx context.Context, tenant, session, callID, name, args string) (agentpkg.ToolOutcome, error) {
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
	// Forward the MODEL's call id, not a fresh one: the turn subscribes to
	// progress under the id it was given, so an invented id sends every frame into
	// the void. A caller with no id (an internal call, a probe) still gets one
	// generated, so the contract stays usable outside a turn.
	if callID == "" {
		callID = newCallID()
	}

	// Permission gate. Shallow on purpose: it answers "does this session hold ANY grant for
	// each key the tool requires", never "is this call covered". Coverage needs the target,
	// which only the extension can resolve from the arguments.
	//
	// Refusing here is an optimisation with a better message — the extension re-checks and
	// is the real boundary. Erring towards allowing is deliberate: a false positive costs
	// one refused call, while a false negative would block work the user authorised.
	grants := e.sessionGrants(ctx, tenant, session, extID)
	if needs := e.requiresOf(name); len(needs) > 0 {
		if missing, ok := store.HasAnyCapability(grants, needs); !ok {
			return agentpkg.ToolOutcome{}, &PermissionDeniedError{
				Tool: name, ExtID: extID, Capability: missing,
			}
		}
	}

	res, err := e.agent.CallToolWithGrants(ctx, tenant, session, extID, bare, callID, input, grantsToWire(grants))
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

// ExtensionPermissions returns an extension's declared permissions.
func (e *ToolExecutor) ExtensionPermissions(extID string) []bundled.PermissionDecl {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.extPerms[extID]
}

// HasTools reports whether an extension declared at least one tool.
//
// Used to decide whether an extension with no config knobs and no permissions is still worth
// listing: one that has tools is, and hiding it would hide its tools too.
func (e *ToolExecutor) HasTools(extID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for name, owner := range e.owner {
		_ = name
		if owner == extID {
			return true
		}
	}
	return false
}

// AllPermissions returns every extension's declared permissions, keyed by extension id.
//
// The whole set at once: a client showing "what can I grant this session" needs all of it,
// and asking per extension would be one round trip per extension for a panel that shows
// them together.
func (e *ToolExecutor) AllPermissions() map[string][]bundled.PermissionDecl {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string][]bundled.PermissionDecl, len(e.extPerms))
	for k, v := range e.extPerms {
		if len(v) == 0 {
			continue
		}
		out[k] = v
	}
	return out
}

// AllChannels returns every extension's published channels, keyed by extension id.
func (e *ToolExecutor) AllChannels() map[string][]bundled.ChannelDecl {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string][]bundled.ChannelDecl, len(e.extChans))
	for k, v := range e.extChans {
		if len(v) == 0 {
			continue
		}
		out[k] = v
	}
	return out
}

// manifestChannels maps a discovered manifest's channel declarations.
func manifestChannels(m *easybus.ExtensionManifest) []bundled.ChannelDecl {
	if m == nil || len(m.Channels) == 0 {
		return nil
	}
	out := make([]bundled.ChannelDecl, 0, len(m.Channels))
	for _, c := range m.Channels {
		topics := make([]bundled.ChannelTopicDecl, 0, len(c.Topics))
		for _, t := range c.Topics {
			topics = append(topics, bundled.ChannelTopicDecl{
				Pattern: t.Pattern, Description: t.Description, Descriptions: t.Descriptions,
			})
		}
		out = append(out, bundled.ChannelDecl{
			Name: c.Name, Description: c.Description,
			Descriptions: c.Descriptions, Topics: topics,
		})
	}
	return out
}

// manifestPermissions maps a discovered manifest's permission declarations.
func manifestPermissions(m *easybus.ExtensionManifest) []bundled.PermissionDecl {
	if m == nil || len(m.Permissions) == 0 {
		return nil
	}
	out := make([]bundled.PermissionDecl, 0, len(m.Permissions))
	for _, p := range m.Permissions {
		out = append(out, bundled.PermissionDecl{
			Key:          p.Key,
			Label:        p.Label,
			Descriptions: p.Descriptions,
			Risk:         p.Risk,
			ParamsFields: permFieldSpecs(p.ParamsFields),
		})
	}
	return out
}

// permFieldSpecs converts declared params to the SAME shape config knobs use, so the
// console renders one form component for both and there is no second schema dialect.
func permFieldSpecs(in []*easybus.ConfigFieldSchema) []bundled.ConfigField {
	if len(in) == 0 {
		return nil
	}
	out := make([]bundled.ConfigField, 0, len(in))
	for _, f := range in {
		out = append(out, bundled.ConfigField{
			Name:        f.Name,
			Type:        f.Type,
			Kind:        derefStr(f.Kind),
			Format:      derefStr(f.Format),
			Description: derefStr(f.Description),
			Fields:      permNestedSpecs(f.Fields),
		})
	}
	return out
}

// permNestedSpecs maps a nested field list onto the config-knob sub-field shape.
//
// A second conversion exists because the two shapes differ in one place: a config knob's
// Fields are ConfigFieldSpec (with defaults and enums) while the wire publishes
// ConfigFieldSchema. Keeping them separate here is cheaper than unifying types across the
// manifest and the tool registry for one nested level.
func permNestedSpecs(in []*easybus.ConfigFieldSchema) []bundled.ConfigFieldSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]bundled.ConfigFieldSpec, 0, len(in))
	for _, f := range in {
		out = append(out, bundled.ConfigFieldSpec{
			Name:        f.Name,
			Type:        f.Type,
			Kind:        derefStr(f.Kind),
			Format:      derefStr(f.Format),
			Description: derefStr(f.Description),
			Fields:      permNestedSpecs(f.Fields),
		})
	}
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

// PublishMailbox delivers one mailbox message to a session. THE single entry point.
//
// A mailbox message has to exist in TWO places, and they are not interchangeable:
//
//   - the DATABASE is the execution queue. `Wake` drains it, and a drained row is what a turn
//     actually reads. Anything not in the table is, to the runtime, a message that never arrived.
//   - NATS is the WAKE SIGNAL. The database alone would sit there unread until something nudged
//     a runtime, and the nudge has to reach whichever replica ends up holding the lease.
//
// They drifted apart once, with exactly that consequence: the channel distributor called the
// NATS-only path, so a channel event reached the mailbox stream, a consumer acked it, `Wake` ran,
// found an empty table, and returned. Routing said `matches=2` and no session ever woke.
//
// So both happen here, in this order: enqueue first (durable), then signal. If the signal is lost
// the row is still there and the boot backstop drains it; the reverse order would signal a wake
// for a message that is not yet readable.
func (b *Bus) PublishMailbox(ctx context.Context, tenant, session, typ string, payload any, source string) error {
	if b.db == nil {
		// Refusing is deliberate. Falling through to signal-only would look like success from
		// every angle except the session waking up, which is exactly how this took a day to find.
		return fmt.Errorf("publish mailbox: no queue configured (call WithMailboxQueue)")
	}
	if _, err := b.db.Enqueue(tenant, session, typ, payload, source); err != nil {
		return fmt.Errorf("enqueue mailbox: %w", err)
	}
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

// PushExtensionConfig delivers one config value to a RUNNING extension.
//
// WHY this is required at all: an extension does not read the agent's config
// store. It subscribes to easybus.<tenant>.config.<extId> and keeps its own
// in-memory copy, which the SDK only refreshes when the agent sends a ConfigSet.
// Writing the store alone therefore updates what the CONSOLE shows and nothing
// else — the extension keeps answering "not configured" for a knob that is
// plainly set.
//
// It is a REQUEST, not a publish: the SDK's serveConfig ignores a ConfigSet with
// no ReplyTo, and the reply carries the extension's accept/reject verdict (a
// OnConfigChange hook may refuse a value).
func (b *Bus) PushExtensionConfig(ctx context.Context, tenant, extID, name string, value any, session string) error {
	ack := true
	set := easybus.ConfigSet{
		Name:  name,
		Value: easybus.Value(value),
		Scope: easybus.ConfigSetScopeGlobal,
		// Ask for the accept/reject reply.
		//
		// The SDK only publishes to ReplyTo when Ack is set, so without it a
		// Request waits for a reply that never comes and the caller times out —
		// even though the extension applied the value correctly. That is a nasty
		// shape: the config works and every save reports failure.
		Ack: &ack,
	}
	if tenant != "" {
		t := tenant
		set.Tenant = &t
	}
	if session != "" {
		set.Scope = easybus.ConfigSetScopeSession
		s := session
		set.SessionName = &s
	}
	// Pointer, not value: ConfigSet embeds protoimpl.MessageState, which holds a
	// sync.Mutex. Passing it by value copies a lock (`go vet` flags it, correctly —
	// copying a mutex is undefined behaviour waiting for a timing to go wrong).
	_, err := b.transport.Request(ctx, protocol.ChConfig(tenant, extID), &set, bus.RequestOpts{Tenant: tenant})
	return err
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

// ExtensionNamespaces returns an extension's DECLARED namespaces, in the order
// the author listed them.
//
// The order matters: it is what a client uses to lay the groups out, and an
// author's ordering (services, build, storage, ...) reflects how the extension
// is actually used.
func (e *ToolExecutor) ExtensionNamespaces(extID string) []bundled.Namespace {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.extNS[extID]
}

// manifestNamespaces maps the wire declaration onto the agent's own type.
func manifestNamespaces(m *easybus.ExtensionManifest) []bundled.Namespace {
	if m == nil || len(m.Namespaces) == 0 {
		return nil
	}
	out := make([]bundled.Namespace, 0, len(m.Namespaces))
	for _, n := range m.Namespaces {
		ns := bundled.Namespace{Name: n.GetName(), Descriptions: n.GetDescriptions()}
		if n.Description != nil {
			ns.Description = *n.Description
		}
		out = append(out, ns)
	}
	return out
}
