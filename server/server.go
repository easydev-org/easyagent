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
	"github.com/easydev-org/easyagent/agentbus"
	"github.com/easydev-org/easyagent/blob"
	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/store"
	"github.com/easydev-org/easybus-sdk/mailbox"
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
	runtimeBus *agentbus.Bus
	// mail is the delivery queue. nil in a deployment with no bus, where the mailbox page reports the
	// absence rather than an empty list — "no queue" and "nothing waiting" are different facts.
	mail MailboxQueue
	// tools is the discovered tool surface (nil in in-memory mode).
	tools *agentbus.ToolExecutor
	// bundledTools is the in-process bundled extension tool surface.
	bundledTools ToolSource
	// bundledFields exposes a tool's declared config fields.
	bundledFields ConfigFieldSource
	// stream is the durable session-event source (nil = in-process hub only).
	stream StreamSource
	// lifecycle is the lifecycle broadcaster (nil = not wired).
	lifecycle LifecyclePublisher
	// channels provisions each session's built-in channel subscription (nil = no broker).
	channels ChannelProvisioner
	// subs lists and edits channel subscriptions (nil = channels not configured).
	subs SubscriptionStore
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

/**
 * MailboxQueue is the delivery queue, as this package uses it.
 *
 * TWO METHODS, WHICH IS THE WHOLE OF WHAT AN RPC NEEDS: put a message where the runtime will find it, and
 * page one conversation's messages. Draining belongs to the runtime, and the interface stays narrow so a
 * deployment cannot hand this package something that only half works.
 */
type MailboxQueue interface {
	Enqueue(ctx context.Context, tenant, session, id string, typ mailbox.Type, payload any, source string) (string, error)
	Page(ctx context.Context, tenant, session string) ([]mailbox.Message, error)
}

// Deps are the server's collaborators.
type Deps struct {
	// ---- required ----
	DB      *store.DB
	Runtime *agent.Runtime
	Config  Config
	/**
	 * Mailbox is the delivery queue the prompt path writes to and the mailbox page reads from.
	 *
	 * IT USED TO BE s.db, because the queue was a table. Now that it is a bucket, the server needs the
	 * queue itself — and keeping it as its own field says which of the two a call is about: a message
	 * waiting to be taken, or the transcript it becomes.
	 *
	 * AN INTERFACE, so a test can supply the in-process queue and a deployment the durable one, without
	 * either knowing which it was given.
	 */
	Mailbox MailboxQueue

	// ---- optional, each nil meaning something specific ----
	// Hub is the in-process event fan-out. nil builds one, which is what every deployment wants; a
	// caller supplies one only to observe the stream in a test.
	Hub *EventHub
	// Presets and ConfigKV resolve a tenant's presets and config. nil means no preset system, and a
	// session then runs with its own settings only.
	Presets  *agent.Presets
	ConfigKV *agent.Config
	// Bus is the agent bus. nil is IN-MEMORY MODE: no broker, so no discovery, no channels and no
	// cross-replica events. A prompt still runs; it just cannot reach an extension.
	Bus *agentbus.Bus
	// Tools is the discovered extension tool surface. nil means discovery is not wired.
	Tools *agentbus.ToolExecutor
	// BundledTools and BundledFields are the in-process extension's tool and config surfaces.
	BundledTools  ToolSource
	BundledFields ConfigFieldSource
	// Stream is the durable session-event source. nil falls back to the in-process hub, so a
	// reconnect resumes only within this process.
	Stream StreamSource
	// Lifecycle broadcasts created/forked/renamed/deleted. nil disables the broadcast.
	Lifecycle LifecyclePublisher
	// Channels provisions each session's built-in subscription. nil means no broker, so a session
	// has no default channel — the truthful state, not an empty one.
	Channels ChannelProvisioner
	// Subs lists and edits channel subscriptions. nil means channels are not configured, and the RPCs
	// answer with that rather than an empty list.
	Subs SubscriptionStore
	// SeedTenant applies deployment-pinned defaults to a tenant created at runtime. nil means a new
	// tenant gets no presets, which is what boot-time seeding covers for the known ones.
	SeedTenant func(tenant string)
	// Blob is the durable S3 backend for file bytes. nil uses the in-memory map.
	Blob *blob.Store
}

/**
 * THE DEPENDENCIES ARE IN ONE PLACE, REQUIRED AND OPTIONAL TOGETHER.
 *
 * THEY USED TO BE SPLIT between this struct and fourteen `Set*` methods called from main, and the
 * split was not harmless. A `Set` returns nothing and checks nothing, so a missing call removed a
 * feature with no compile error and no log — `SetChannelProvisioner` sat next to
 * `SetSubscriptions`, controlling two halves of one thing, and forgetting either left sessions
 * half-addressable. Worse, several OPTIONAL fields were also fields here, which made "is this nil?"
 * a question you answered by reading every use site.
 *
 * NOW every collaborator is a field. The required group is checked in New, so a missing one is a
 * panic at startup instead of a nil dereference on the first request that touches it. The optional
 * group is documented in place — nil there is a MEANING (an in-memory deployment, no broker, no
 * durable blob backend), never an oversight.
 */

// New builds the server.
//
// IT PANICS ON A MISSING REQUIRED DEPENDENCY, deliberately. Carrying the nil until the first request
// that needs it turns a wiring mistake into a failed prompt somewhere else, where the error reads as
// a data problem rather than a configuration one.
func New(d Deps) *Server {
	for name, missing := range map[string]bool{
		"DB":      d.DB == nil,
		"Runtime": d.Runtime == nil,
		"Mailbox": d.Mailbox == nil,
	} {
		if missing {
			panic("server.New: " + name + " is required")
		}
	}
	if d.Hub == nil {
		d.Hub = NewEventHub()
	}
	return &Server{
		db: d.DB, rt: d.Runtime, hub: d.Hub, cfg: d.Config,
		presets: d.Presets, config: d.ConfigKV, runtimeBus: d.Bus, mail: d.Mailbox,
		tools: d.Tools, bundledTools: d.BundledTools, bundledFields: d.BundledFields,
		stream: d.Stream, lifecycle: d.Lifecycle,
		channels: d.Channels, subs: d.Subs, seedTenant: d.SeedTenant,
		blobStore: d.Blob,
		blobs:     map[string][]byte{},
	}
}

// Hub exposes the event hub (the runtime's emitter target).
func (s *Server) Hub() *EventHub { return s.hub }

// ToolSource is a read-only tool surface (the bundled extension).
type ToolSource interface {
	Tools() []agent.ToolDef
}

// ConfigFieldSource exposes a tool's declared config fields (for ListTools) and
// the extension's WHOLE config surface (for the extension-config page).
type ConfigFieldSource interface {
	ConfigFieldsFor(toolName string) []bundled.ConfigField
	ConfigFields() []bundled.ConfigField
}

// ChannelProvisioner writes the built-in channel subscription for a session.
//
// An interface rather than a direct call into the bus package so the in-memory server (used
// by tests and single-process runs) can leave it nil: a session then has no default channel,
// which is the truthful state when there is no broker to deliver on.
type ChannelProvisioner interface {
	EnsureDefault(ctx context.Context, tenant, session string) error
}

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
	/**
	 * SubscribeSessionEvents replays retained frames newer than sinceSeq, then streams live.
	 *
	 * sinceAtMillis IS THE PAGE-REFRESH ANCHOR. A refresh has no `seq` in memory (it starts again at
	 * 0), so the client sends the newest MESSAGE id it holds and the server converts that to a
	 * timestamp. Filtering by time needs the frame's own timestamp, which `Replay` does not carry —
	 * so this parameter is honoured here rather than pretending the anchor does not exist. It is
	 * ignored when sinceSeq > 0, matching the proto's stated precedence.
	 */
	SubscribeSessionEvents(ctx context.Context, tenant, session string, sinceSeq uint64, sinceAtMillis int64) (<-chan []byte, func(), error)
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
			// SESSION token first: the credential Login hands back is random and
			// globally unique, so it resolves a caller on its own. Checking it
			// before the label-based tokens is what keeps a tenant's short,
			// guessable label from ever being the thing presented per RPC.
			if st, ten, ok, err := s.db.SessionTokenBySHA(sha256Hex(auth)); err != nil {
				return connect.NewError(connect.CodeInternal, err.Error())
			} else if ok {
				if ten != nil && ten.Disabled {
					return connect.NewError(connect.CodeUnauthenticated, "tenant is disabled")
				}
				s.db.TouchSessionToken(st.TokenID)
				ctx = context.WithValue(ctx, tenantKey{}, st.TenantID)
				ctx = context.WithValue(ctx, scopeKey{}, st.Scope)
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
//
// THE FRAME IS STAMPED HERE, ONCE. `eid` and `seq` identify the event, not the delivery, so they are
// assigned before it goes anywhere and the same values reach the durable backplane and every live
// subscriber. A client resumes from `sinceSeq` and dedupes on `eid`; stamps applied at SEND time —
// which is what this used to do — give each subscriber its own number for one event, and give the
// live path a `seq` the durable log never used, so the anchor a client echoes back resumes from
// somewhere else entirely.
func (h *EventHub) Publish(tenant, session string, ev agent.Event) {
	if !isEphemeralFrame(ev) {
		// EPHEMERAL FRAMES GET NO SEQ AND NO EID, deliberately: they are token-level deltas whose
		// only consumer is a client connected right now, and writing them to a shared counter would
		// make `sinceSeq` skip over numbers nothing was ever stored under.
		ev.Seq = h.NextSeq()
		ev.Eid = newUUID()
		if h.backplane != nil {
			frame := Frame{Event: ev.Name, Params: ev.Params, Eid: ev.Eid, Seq: ev.Seq, At: time.Now().UnixMilli()}
			_ = h.backplane.PublishSessionEvent(context.Background(), tenant, session, frame.Eid, frame)
		}
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

// streamDeltaNames are the per-token frames a turn emits while a model or tool is producing
// output. They exist to make the answer appear gradually.
//
// They are NOT retained, because their only consumer is a client that is connected RIGHT NOW:
//
//   - the finished message supersedes every one of them, and the DATABASE stores it authoritatively
//     (that is what a reconnecting client reads);
//   - replaying them delivers a half-rendered answer that must immediately be discarded, at the
//     cost of the entire stream.
//
// Measured on a real turn before filtering: the stream held 11 frames of which 5 were per-token
// deltas; on a turn that calls tools, `tool-output-available` preliminary frames were 95% of the
// BYTES. The names matter — an earlier version of this filter covered only `tool-output-*` and
// therefore missed `text-delta`, which is the most frequent frame of all.
var streamDeltaNames = map[string]bool{
	"text-delta":       true, // model output, token by token
	"reasoning-delta":  true, // reasoning output, token by token
	"tool-input-delta": true, // tool arguments, token by token
}

// isEphemeralFrame reports whether an event is a STREAMING CHUNK, durable copy unnecessary.
//
// Structural frames (start-step / finish-step) are KEPT: they are few, and they carry the step
// boundaries a replay needs to assemble what it does receive.
func isEphemeralFrame(ev agent.Event) bool {
	if streamDeltaNames[ev.Name] {
		return true
	}
	// Tool output is streamed as a series of preliminary chunks, each superseded by the next; the
	// LAST one is not preliminary and is kept.
	if ev.Name == "tool-output-available" || ev.Name == "tool-output-error" {
		b, _ := ev.Params["preliminary"].(bool)
		return b
	}
	return false
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
