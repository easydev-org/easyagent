// Package server is the Connect RPC surface: the `agent.v1.AgentService` and
// `agent.v1.AdminService` implementations over connect-go v2, backed by the
// store + the session runtime.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/blob"
	"github.com/easydev-org/easyagent/bundled"
	"github.com/easydev-org/easyagent/bus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/store"
)

// Config is the server's configuration.
type Config struct {
	// AuthMode is "required" (verify tenant bearer tokens) or "none" (pin every
	// request to DefaultTenant; dev only).
	AuthMode string
	// AdminToken is the static admin bearer for AdminService.
	AdminToken string
	// AdminUsername is the platform admin's login name (paired with AdminToken).
	AdminUsername string
	// DefaultTenant is the tenant used when AuthMode="none".
	DefaultTenant string
}

// Server implements both Connect services.
type Server struct {
	db  *store.DB
	rt  *agent.Runtime
	hub *EventHub
	cfg Config

	presets *agent.Presets
	config  *agent.Config

	// runtimeBus is the NATS bus, when wired (nil in in-memory mode).
	runtimeBus *bus.Bus
	// tools is the discovered tool surface (nil in in-memory mode).
	tools *bus.ToolExecutor
	// bundledTools is the in-process bundled extension tool surface.
	bundledTools ToolSource
	// bundledFields exposes a tool's declared config fields.
	bundledFields ConfigFieldSource
	// stream is the durable session-event source (nil = in-process hub only).
	stream StreamSource
	// lifecycle is the lifecycle broadcaster (nil = not wired).
	lifecycle LifecyclePublisher
	// seedTenant applies deployment-pinned defaults to a fresh tenant.
	seedTenant func(tenant string)

	// blobs holds file bytes (an in-memory backend; a durable blob backend is a
	// later layer).
	blobMu sync.RWMutex
	blobs  map[string][]byte
	// blobStore is the durable S3 backend (nil = the in-memory map above).
	blobStore *blob.Store

	// nowFn is an injectable clock (tests).
	nowFn func() time.Time
}

// Deps are the server's collaborators.
type Deps struct {
	DB       *store.DB
	Runtime  *agent.Runtime
	Hub      *EventHub
	Config   Config
	Presets  *agent.Presets
	ConfigKV *agent.Config
	Bus      *bus.Bus
}

// New builds the server.
func New(d Deps) *Server {
	if d.Hub == nil {
		d.Hub = NewEventHub()
	}
	return &Server{
		db: d.DB, rt: d.Runtime, hub: d.Hub, cfg: d.Config,
		presets: d.Presets, config: d.ConfigKV, runtimeBus: d.Bus,
		blobs: map[string][]byte{},
	}
}

// Hub exposes the event hub (the runtime's emitter target).
func (s *Server) Hub() *EventHub { return s.hub }

// SetTools installs the discovered tool surface (for ListTools/ToolConfig).
func (s *Server) SetTools(t *bus.ToolExecutor) { s.tools = t }

// ToolSource is a read-only tool surface (the bundled extension).
type ToolSource interface {
	Tools() []agent.ToolDef
}

// SetBundledTools installs the in-process bundled tool surface (for ListTools).
func (s *Server) SetBundledTools(t ToolSource) { s.bundledTools = t }

// ConfigFieldSource exposes a tool's declared config fields (for ListTools) and
// the extension's WHOLE config surface (for the extension-config page).
type ConfigFieldSource interface {
	ConfigFieldsFor(toolName string) []bundled.ConfigField
	ConfigFields() []bundled.ConfigField
}

// SetBundledConfigFields installs the bundled config-field source.
func (s *Server) SetBundledConfigFields(src ConfigFieldSource) { s.bundledFields = src }

// SetBlob installs the durable S3 blob backend for file bytes.
func (s *Server) SetBlob(b *blob.Store) { s.blobStore = b }

// SetStreamSource installs the durable session-event source (for WatchSession
// since_seq resume). When nil, WatchSession falls back to the in-process hub.
func (s *Server) SetStreamSource(src StreamSource) { s.stream = src }

// SetLifecycle installs the lifecycle publisher (created/forked/renamed/deleted).
func (s *Server) SetLifecycle(p LifecyclePublisher) { s.lifecycle = p }

// SetTenantSeeder installs the per-tenant default seeder (for CreateTenant).
func (s *Server) SetTenantSeeder(fn func(tenant string)) { s.seedTenant = fn }

// LifecyclePublisher broadcasts a session lifecycle event (best-effort).
type LifecyclePublisher interface {
	PublishLifecycle(ctx context.Context, tenant, kind, session string, payload map[string]any) error
}

// lifecycleFire fires a lifecycle event when wired (best-effort).
func (s *Server) lifecycleFire(ctx context.Context, tenant, kind, session string, payload map[string]any) {
	if s.lifecycle == nil {
		return
	}
	_ = s.lifecycle.PublishLifecycle(context.WithoutCancel(ctx), tenant, kind, session, payload)
}

// StreamSource replays + streams a session's durable events from a sequence.
type StreamSource interface {
	SubscribeSessionEvents(ctx context.Context, tenant, session string, sinceSeq uint64) (<-chan []byte, func(), error)
}

// publish emits one session event on the hub (lifecycle handlers use it for
// chain-changed / session-changed notifications).
func (s *Server) publish(tenant, session, name string, params map[string]any) {
	s.hub.Publish(tenant, session, agent.Event{Name: name, Params: params})
}

// Handlers registers both services on a connect server and returns it.
func (s *Server) Register(server *connect.Server) {
	agentv1connect.RegisterAgentServiceHandler(server, s)
	agentv1connect.RegisterAdminServiceHandler(server, s)
}

// ---- auth ----

type tenantKey struct{}

// scopeKey carries a delegated-admin scope: "*" for the static platform admin,
// the subtree root id for a delegated admin, and "" for a plain tenant.
type scopeKey struct{}

// tenantOf returns the tenant resolved by the auth interceptor.
func tenantOf(ctx context.Context) string {
	if t, ok := ctx.Value(tenantKey{}).(string); ok {
		return t
	}
	return s_defaultTenant
}

// scopeOf returns the caller's admin scope, or "" when the caller is not an
// admin (a tenant, or auth=none).
func scopeOf(ctx context.Context) string {
	if s, ok := ctx.Value(scopeKey{}).(string); ok {
		return s
	}
	return ""
}

// s_defaultTenant is set at construction so the free function can read it.
var s_defaultTenant = "default"

// SetDefaultTenant sets the fallback tenant for auth=none.
func SetDefaultTenant(t string) {
	if t != "" {
		s_defaultTenant = t
	}
}

// AuthInterceptor verifies the tenant bearer token (or the admin token) and
// pins the resolved tenant onto the context. It is a connect ServerInterceptor.
func (s *Server) AuthInterceptor() connect.ServerInterceptor {
	SetDefaultTenant(s.cfg.DefaultTenant)
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			// Login is exempt: the (username, token) pair IS the credential.
			if spec.Procedure == "/agent.v1.AdminService/Login" {
				return next(ctx, spec, stream)
			}
			auth := inboundAuthorization(ctx)
			if s.cfg.AuthMode == "none" {
				return next(context.WithValue(ctx, tenantKey{}, s.cfg.DefaultTenant), spec, stream)
			}
			if auth == "" {
				return connect.NewError(connect.CodeUnauthenticated, "a bearer token is required")
			}
			// Admin token unlocks the AdminService (and nothing on AgentService
			// requires admin).
			if s.cfg.AdminToken != "" && constantTimeEqual(auth, s.cfg.AdminToken) {
				// Static platform admin: unscoped ("*" = every subtree).
				ctx = context.WithValue(ctx, tenantKey{}, "")
				ctx = context.WithValue(ctx, scopeKey{}, "*")
				return next(ctx, spec, stream)
			}
			// Delegated admin token: scoped to its subtree root.
			if scope, atok, ok, err := s.db.AdminTokenBySHA(sha256Hex(auth)); err != nil {
				return connect.NewError(connect.CodeInternal, err.Error())
			} else if ok {
				_ = s.db.TouchAdminToken(atok.TokenID)
				ctx = context.WithValue(ctx, tenantKey{}, "")
				ctx = context.WithValue(ctx, scopeKey{}, scope)
				return next(ctx, spec, stream)
			}
			sha := sha256Hex(auth)
			ten, tok, ok, err := s.db.TenantByTokenSHA(sha)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err.Error())
			}
			if !ok || ten.Disabled {
				return connect.NewError(connect.CodeUnauthenticated, "invalid or disabled tenant token")
			}
			_ = s.db.TouchToken(tok.TokenID)
			return next(context.WithValue(ctx, tenantKey{}, ten.ID), spec, stream)
		}
	}
}

// inboundAuthorization reads the Authorization header off the server CallInfo.
func inboundAuthorization(ctx context.Context) string {
	info, ok := connect.CallInfoForServerContext(ctx)
	if !ok {
		return ""
	}
	v := info.RequestHeader().Get("Authorization")
	if v == "" {
		return ""
	}
	if len(v) > 7 && strings.EqualFold(v[:7], "Bearer ") {
		return v[7:]
	}
	return v
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// ---- event hub ----

// EventHub fans session events out to live subscribers (WatchSession / Prompt).
// It is IN-PROCESS: a single replica serves both the turn and the watcher. A
// multi-replica deployment must back this with the durable NATS session-event
// stream (see DEVELOP.md); the interface is kept small so that swap is local.
type EventHub struct {
	mu   sync.Mutex
	subs map[string]map[int64]chan agent.Event
	next int64
	seq  int64
	// backplane, when set, mirrors every published event onto the durable NATS
	// session-events stream so OTHER replicas' watchers receive it.
	backplane Backplane
}

// Backplane is the durable, cross-replica event sink.
type Backplane interface {
	PublishSessionEvent(ctx context.Context, tenant, session, id string, payload any) error
}

// NewEventHub builds an in-process hub.
func NewEventHub() *EventHub {
	return &EventHub{subs: map[string]map[int64]chan agent.Event{}}
}

func hubKey(tenant, session string) string { return tenant + "\x00" + session }

// Subscribe returns a channel of events for one session + a cancel func.
func (h *EventHub) Subscribe(tenant, session string) (<-chan agent.Event, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.next
	h.next++
	ch := make(chan agent.Event, 64)
	k := hubKey(tenant, session)
	if h.subs[k] == nil {
		h.subs[k] = map[int64]chan agent.Event{}
	}
	h.subs[k][id] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if m := h.subs[k]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(h.subs, k)
			}
		}
	}
}

// Publish delivers one event to every subscriber of the session (non-blocking:
// a slow subscriber drops, matching a best-effort live stream).
func (h *EventHub) Publish(tenant, session string, ev agent.Event) {
	if h.backplane != nil {
		frame := Frame{Event: ev.Name, Params: ev.Params, Eid: newUUID(), Seq: h.NextSeq()}
		_ = h.backplane.PublishSessionEvent(context.Background(), tenant, session, frame.Eid, frame)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs[hubKey(tenant, session)] {
		select {
		case ch <- ev:
		default:
		}
	}
	// B3: an event that changes a session's LIST row also nudges the tenant's
	// sessions channel, so WatchSessions re-emits that row live.
	if session != "\x00sessions" && listAffecting[ev.Name] {
		nudge := agent.Event{Name: "session-changed", Params: map[string]any{"session_name": session}}
		for _, ch := range h.subs[hubKey(tenant, "\x00sessions")] {
			select {
			case ch <- nudge:
			default:
			}
		}
	}
}

// listAffecting are the events that change a session's list row (preview, seq,
// status, tip). They trigger a WatchSessions upsert.
var listAffecting = map[string]bool{
	"message-added": true, "turn-complete": true, "status": true,
	"compacted": true, "chain-changed": true,
}

// SetBackplane installs the durable event sink.
func (h *EventHub) SetBackplane(b Backplane) { h.backplane = b }

// NextSeq returns a monotonic stream sequence.
func (h *EventHub) NextSeq() int64 { return atomic.AddInt64(&h.seq, 1) }

// Emitter adapts the hub to agent.Emitter (used by the runtime).
func (h *EventHub) Emitter(tenant, session string) agent.Emitter {
	return agent.EmitterFunc(func(_ context.Context, ev agent.Event) error {
		h.Publish(tenant, session, ev)
		return nil
	})
}

var _ = agentv1.HealthRequest{}
