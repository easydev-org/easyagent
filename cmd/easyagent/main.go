// Command easyagent is the standalone agent server: it wires the store, the
// session runtime, and the Connect RPC surface, and serves AgentService +
// AdminService over h1+h2c plus a separate plain-HTTP/1.1 health port.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/easydev-org/easyagent/internal/config"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/blob"
	"github.com/easydev-org/easyagent/bundled"
	"github.com/easydev-org/easyagent/bus"
	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/media"
	"github.com/easydev-org/easyagent/server"
	"github.com/easydev-org/easyagent/store"
	sdkbus "github.com/easydev-org/easybus-sdk/bus"
)

func main() {
	cfgPath := flag.String("config", "", "path to the YAML config file (default: ./easyagent.yaml, then /etc/easyagent/config.yaml)")
	flag.Parse()
	if err := run(*cfgPath); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

// resolveConfigPath applies the default lookup order when --config is unset.
func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	for _, p := range []string{"easyagent.yaml", "/etc/easyagent/config.yaml"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func run(cfgPath string) error {
	cfg, err := config.Load(resolveConfigPath(cfgPath))
	if err != nil {
		return err
	}

	db, err := store.Open(storeBackend(cfg.Database.Backend), cfg.Database.DSN)
	if err != nil {
		return err
	}
	if err := db.Migrate(); err != nil {
		return err
	}

	hub := server.NewEventHub()
	rtDeps := agent.RuntimeDeps{
		DB: db, Resolver: &agent.StoreResolver{DB: db}, Lease: agent.NewMemoryLease(),
		EmitFor: hub.Emitter, MaxTurns: cfg.Agent.DefaultMaxTurns,
		ToolTimeout:        cfg.Agent.ToolTimeout,
		CompactAtFraction:  cfg.Agent.CompactAtFraction,
		ForceCompactTokens: cfg.Agent.ForceCompactTokens,
		DeniedTools:        cfg.Agent.DisabledTools,
		ExtensionAllowlist: extensionAllowlistResolver(db, cfg),
	}
	var runtimeBus *bus.Bus
	var toolExec *bus.ToolExecutor
	// Channels: the local index, the distributor, and the cleanup for the KV watch.
	var subIdx *bus.SubscriptionIndex
	var dist *bus.Distributor
	var channelStop func()
	// Presets + tenant config: JetStream KV when NATS is available (persisted
	// across replicas/restarts), else in-memory.
	var kv agent.KV = agent.NewMemoryKV()
	if cfg.NATS.URL != "" {
		b, err := bus.Connect(cfg.NATS.URL)
		if err != nil {
			return fmt.Errorf("nats: %w", err)
		}
		// The queue PublishMailbox writes to before it signals. Set here so no code path can
		// signal a wake for a message the runtime cannot read: PublishMailbox refuses outright
		// when this is missing.
		runtimeBus = b.WithMailboxQueue(db)
		defer b.Close()
		// Work around an abc-protocol-go bug: pre-create the extension presence
		// bucket so the library's presence watch does not error (and panic).
		_ = b.EnsurePresenceBucket(context.Background())
		kv = bus.NewNATSKV(b, bus.PresetsBucket)
		// The config surface lives in its own bucket (wired below).
	}
	presets := agent.NewPresets(kv)
	tenantConfig := agent.NewConfig(kv)
	if runtimeBus != nil {
		tenantConfig = agent.NewConfig(bus.NewNATSKV(runtimeBus, bus.ConfigBucket))
	}
	// The TURN needs the same presets the console edits.
	//
	// This was missing: the server was constructed with `Presets: presets`, so the
	// Presets page worked and saved rows — while every turn failed with
	// "preset store unavailable", because RuntimeDeps.Presets was never set. A
	// session that references any preset could not run at all.
	// The adapters, not the raw stores: Presets/Config implement the storage
	// operations, while the turn wants the narrow read interfaces.
	rtDeps.Presets = agent.PresetStore{P: presets}
	rtDeps.Config = agent.ConfigStore{C: tenantConfig}
	seedPresets(presets, tenantConfig, db, cfg)
	// The in-process bundled extension (Go rewrite of the TS bundled-extension):
	// it supplies the memory/history/file/web/vision/generation/subsession tools.
	// Bound AFTER the bus exists (below): the bundled tools must publish through the SAME path
	// the channel distributor uses, and the bus is created later in this function. A nil check
	// here would reintroduce the split this variable exists to remove, so the closure refuses
	// loudly instead.
	var mailboxPublish func(tenant, session, typ string, payload any, source string) error
	bundledExt, err := bundled.New(bundledDeps(db, tenantConfig, hub, &mailboxPublish))
	if err != nil {
		return fmt.Errorf("bundled extension: %w", err)
	}
	if runtimeBus != nil {
		b := runtimeBus
		// THE single mailbox path: enqueue (durable) then signal (cross-replica). Wired here
		// because the bus is created after the bundled extension, which needs the publisher.
		mailboxPublish = func(tenant, session, typ string, payload any, source string) error {
			return b.PublishMailbox(context.Background(), tenant, session, typ, payload, source)
		}
		// Mirror every published event onto the durable session-events stream
		// so other replicas' watchers receive it (cross-replica WatchSession).
		hub.SetBackplane(b)
		rtDeps.Lease = b
		exec := bus.NewToolExecutor(b.Agent())
		// Permissions: the executor asks this for a session's grants on every gated call.
		// Injected as a function so the executor stays free of the storage layer.
		exec.WithGrants(func(ctx context.Context, tenant, session string) (map[string][]store.GrantSnapshot, error) {
			return db.GrantsByExt(tenant, session)
		})
		_ = exec.Refresh(context.Background(), 500)
		// Composite: bundled (in-process) + NATS extensions (out-of-process).
		composite := agent.NewCompositeTools(
			[]agent.ToolRegistry{bundledExt, bus.NewToolRegistry(exec)},
			[]agent.ToolExecutor{bundledExt, exec},
		)
		rtDeps.Tools = composite
		rtDeps.Registry = composite
		// Live tool output: the extension publishes in-flight updates on
		// easybus.<tenant>.tool.progress.<callID>; the turn re-emits each frame onto
		// the session's durable event stream so a reconnecting client still sees it.
		rtDeps.ToolProgress = agent.NewToolProgressSource(b.Agent())
		toolExec = exec

		// Channels: one consumer for every channel of every tenant, routed to sessions
		// through an index mirrored from the `subs` KV bucket.
		//
		// The routing is local because a JetStream consumer's filter is fixed at creation:
		// a per-subscription consumer would have to be rebuilt whenever a user changed what
		// they follow. Here the filter is always channel.>, so subscriptions cost nothing at
		// the broker.
		subIdx = bus.NewSubscriptionIndex(b.Transport())
		if stop, err := subIdx.Start(context.Background()); err != nil {
			slog.Error("channel subscriptions unavailable", "err", err)
		} else {
			channelStop = stop
			slog.Info("channel subscriptions watching")
		}
		dist = bus.NewDistributor(subIdx, func(ctx context.Context, tenant, session, typ string, payload any, source string) error {
			return b.PublishMailbox(ctx, tenant, session, typ, payload, source)
		}).WithLogger(slog.Default())
		if _, err := dist.Start(context.Background(), sdkbus.NewChannels(b.Transport()), "agent-channels"); err != nil {
			slog.Error("channel consumer unavailable", "err", err)
		} else {
			slog.Info("channel consumer started", "queue", "agent-channels")
		}
	} else {
		rtDeps.Tools = bundledExt
		rtDeps.Registry = bundledExt
	}
	rt := agent.NewRuntime(rtDeps)

	srv := server.New(server.Deps{
		DB: db, Runtime: rt, Hub: hub,
		Config: server.Config{
			AuthMode: cfg.Auth.Mode, AdminToken: cfg.Auth.AdminToken, AdminUsername: cfg.Auth.AdminUsername, DefaultTenant: cfg.Tenant.Default,
		},
		Presets: presets, ConfigKV: tenantConfig, Bus: runtimeBus,
	})
	server.SetDefaultTenant(cfg.Tenant.Default)
	if toolExec != nil {
		srv.SetTools(toolExec)
	}
	srv.SetBundledTools(bundledExt)
	srv.SetBundledConfigFields(bundledExt)
	// A runtime-created tenant is seeded immediately (boot seeding only covers
	// tenants known at startup) — mirrors the TS seedTenantDefaults.
	srv.SetTenantSeeder(func(tenant string) {
		seedTenantDefaults(presets, tenantConfig, db, cfg, tenant)
	})
	if runtimeBus != nil {
		srv.SetStreamSource(runtimeBus)
		// Every session gets a default channel so anything can reach it by name.
		if subIdx != nil {
			srv.SetChannelProvisioner(bus.NewDefaultChannelProvisioner(runtimeBus.Transport()))
			srv.SetSubscriptions(bus.NewSubscriptionStore(runtimeBus.Transport(), subIdx))
		}
	}
	// Durable file bytes live in S3 when configured (AGENT_BLOB_BACKEND=s3).
	if strings.EqualFold(cfg.Blob.Backend, "s3") {
		store3, err := blob.NewS3(context.Background(), blob.S3Config{
			Bucket:          cfg.Blob.S3.Bucket,
			Region:          cfg.Blob.S3.Region,
			Endpoint:        cfg.Blob.S3.Endpoint,
			AccessKeyID:     cfg.Blob.S3.AccessKey,
			SecretAccessKey: cfg.Blob.S3.SecretKey,
			ForcePathStyle:  cfg.Blob.S3.ForcePathStyle,
			Prefix:          cfg.Blob.S3.Prefix,
		})
		if err != nil {
			return fmt.Errorf("s3 blob: %w", err)
		}
		srv.SetBlob(store3)
		fileBlobGet = func(code string) ([]byte, bool) {
			b, ok, _ := store3.Get(context.Background(), code)
			return b, ok
		}
		fileBlobPut = func(ctx context.Context, code string, data []byte) error {
			return store3.Put(ctx, code, data)
		}
		slog.Info("file bytes backed by S3", "bucket", cfg.Blob.S3.Bucket)
	}

	// Mailbox consumer: each replica joins the durable consumer and processes
	// exactly-once; every delivered message wakes the session.
	if runtimeBus != nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// A2: per-instance liveness heartbeat (owner bucket).
		hb := runtimeBus.NewHeartbeat(replicaID(cfg.NATS.ReplicaID))
		hb.Start(ctx, runtimeBus)
		defer hb.Stop()
		stop, err := runtimeBus.ConsumeMailbox(ctx, func(m bus.MailboxMessage) error {
			return rt.Wake(ctx, m.Tenant, m.SessionName)
		})
		if err != nil {
			return fmt.Errorf("mailbox consumer: %w", err)
		}
		defer stop()
		// File RPCs: let a DB-less / store-less EXTENSION persist bytes through
		// the agent (easybus.<tenant>.file.ingest / file.get). Bytes never ride
		// the message — only the transient object reference does.
		stopFileRPC, err := srv.ServeFileRPC(ctx, runtimeBus.Transport())
		if err != nil {
			return fmt.Errorf("file rpc: %w", err)
		}
		defer stopFileRPC()
		// A5: boot recovery + a periodic backstop for dropped wakes.
		rt.RecoverOnBoot(ctx)
		agent.NewBackstop(rt, 60*time.Second).Start(ctx)
		// Resume watchdog: auto-resume a session whose last turn ended with a
		// RESUMABLE reason (length / retryable) — gated by resume_watch_interval
		// (0 = off).
		if cfg.Agent.ResumeWatchInterval > 0 {
			agent.NewResumeWatch(rt, cfg.Tenant.Default, cfg.Agent.ResumeWatchInterval).Start(ctx)
		}
	}

	// RPC server (h1 + h2c) and the separate plain-HTTP health server.
	mux := http.NewServeMux()
	// Move any value still sitting in the OLD flat config namespace into its
	// per-extension one. Run before serving so no request can observe the
	// half-migrated state; idempotent, so a restart is harmless.
	if n := srv.MigrateLegacyConfig(); n > 0 {
		slog.Info("config migrated to per-extension namespaces", "values", n)
	}
	cs := connect.NewServer(srv.AuthInterceptor())
	srv.Register(cs)
	connecthttp.Mount(mux, cs, connecthttp.WithReadMaxBytes(cfg.Server.MaxBodyBytes))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	rpc := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           cors(cfg.Server.CORSOrigin, mux),
		ReadHeaderTimeout: 15 * time.Second,
		Protocols:         protocols(),
	}
	health := &http.Server{
		Addr:              cfg.Server.HealthAddr,
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	health.Handler = healthMux

	errCh := make(chan error, 2)
	go func() { errCh <- rpc.ListenAndServe() }()
	if cfg.Server.HealthAddr != "" {
		go func() { errCh <- health.ListenAndServe() }()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-stop:
		slog.Info("shutting down", "signal", s.String())
		// Stop the channel KV watch so the shutdown does not leave a goroutine pulling
		// from a closed connection.
		if channelStop != nil {
			channelStop()
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = rpc.Shutdown(ctx)
	_ = health.Shutdown(ctx)
	return nil
}

// protocols enables HTTP/1.1 + cleartext HTTP/2 (h2c) on the RPC listener, so
// both h1 and h2c clients can dial it (mirrors the TS agent).
func protocols() *http.Protocols {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

// cors allows browser clients from the configured origin.
func cors(origin string, h http.Handler) http.Handler {
	if origin == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// seedPresets installs the built-in default preset (system keyspace) + a
// default model config for tenants that have none.
func seedPresets(p *agent.Presets, cfg *agent.Config, db *store.DB, c config.Config) {
	tenants, _ := db.KnownTenants()
	for _, t := range tenants {
		seedTenantDefaults(p, cfg, db, c, t)
	}
}

// seedTenantDefaults applies the deployment-pinned defaults to ONE tenant:
// system preset + providers + extension/tenant config. Create-if-absent (or
// "only when the tenant has none yet"), so it never clobbers a user value.
// Shared by boot seeding and the AdminService's CreateTenant (a runtime-created
// tenant must be immediately usable).
func seedTenantDefaults(p *agent.Presets, cfg *agent.Config, db *store.DB, c config.Config, tenant string) {
	// Host-injected immutable system presets (SYSTEM_PRESETS_FILE). The preset
	// SET is chosen by the tenant's DELEGATION SUBTREE ROOT (scope_id): a
	// subtree carries its own system presets (tool whitelists), falling back to
	// the `default` set. This is per-subadmin preset grouping.
	scope := tenant
	if t, ok, _ := db.GetTenant(tenant); ok && t.ScopeID != "" {
		scope = t.ScopeID
	}
	for _, row := range loadInjectedPresets(c.Seed.SystemPresetsFile, scope) {
		p.SeedSystem(tenant, row)
	}
	if len(p.List(tenant)) == 0 {
		p.SeedSystem(tenant, agent.PresetRow{
			ID: "default", SystemPrompt: "You are a helpful assistant.",
			SystemPromptI18N: map[string]string{
				"en": "You are a helpful assistant.",
				"zh": "你是一个有用的助手。",
			},
			MaxTurns: c.Agent.DefaultMaxTurns, IsSystem: true,
		})
	}
	// Seed providers only into tenants that have NONE yet.
	if len(c.Seed.Providers) > 0 {
		seedProviders(db, []string{tenant}, c.Seed.Providers)
	}
	// Extension config rows, create-if-absent.
	for _, r := range c.Seed.ExtensionConfig {
		if r.Tenant != "*" && r.Tenant != tenant {
			continue
		}
		if _, ok := cfg.Get(tenant, r.Name); !ok {
			cfg.Set(tenant, r.Name, r.Value)
		}
	}
	// Tenant config rows, create-if-absent.
	for _, r := range c.Seed.Config {
		if r.Tenant != "*" && r.Tenant != tenant {
			continue
		}
		if _, ok := cfg.Get(tenant, r.Key); !ok {
			cfg.Set(tenant, r.Key, r.Value)
		}
	}
}

// bundledDeps builds the host-capability adapter the in-process bundled
// extension needs (the Go equivalent of the TS `BundledDeps` injection).
func bundledDeps(db *store.DB, cfg *agent.Config, hub *server.EventHub, mailboxPublish *func(string, string, string, any, string) error) *bundled.StoreDeps {
	d := &bundled.StoreDeps{DB: db}
	d.ConfigFn = func(tenant, key string) (string, bool) { return cfg.Get(tenant, key) }
	d.SessionVarFn = func(tenant, session, name string) (string, bool) {
		if name != "locale" {
			return "", false
		}
		s, ok, _ := db.GetSession(tenant, session)
		if !ok || s.Locale == "" {
			return "", false
		}
		return s.Locale, true
	}
	d.BlobGetFn = func(tenant, code string) (map[string]any, []byte, bool) {
		rec, ok, _ := db.FileByCode(tenant, code)
		if !ok {
			return nil, nil, false
		}
		data, _ := fileBlobGet(rec.Code)
		return map[string]any{
			"code": rec.Code, "name": rec.Name, "mime": rec.Mime,
			"size": rec.Size, "sha256": rec.SHA256,
		}, data, true
	}
	d.BlobPutFn = func(ctx context.Context, tenant, code string, data []byte) error {
		return fileBlobPut(ctx, code, data)
	}
	d.MailFn = func(tenant, session, typ string, payload any, source string) error {
		// THE SAME PATH the channel distributor takes.
		//
		// It used to enqueue and stop, relying on the caller to nudge the runtime — so a message
		// from a bundled tool woke nothing unless the tool happened to do it itself, while a
		// message from a channel did the opposite (signalled, never enqueued). One entry point
		// means there is no second combination to get wrong.
		if mailboxPublish == nil || *mailboxPublish == nil {
			return fmt.Errorf("publish mailbox: bus not wired yet (this is a startup ordering bug)")
		}
		return (*mailboxPublish)(tenant, session, typ, payload, source)
	}
	d.OnIngestFn = func(ctx context.Context, tenant, code, name, mime string, data []byte) {
		facts := media.Probe(data)
		_ = db.UpdateFileMedia(code, facts.Width, facts.Height, facts.DurationMS, nil, nil)
	}
	return d
}

// fileBlobGet / fileBlobPut are the durable blob accessors wired in run(); they
// fall back to an in-memory map when no S3 backend is configured.
var (
	fileBlobGet func(code string) ([]byte, bool)
	fileBlobPut func(ctx context.Context, code string, data []byte) error
)

func seedProviders(db *store.DB, tenants []string, rows []config.SeedProvider) {
	for _, r := range rows {
		targets := tenants
		if r.Tenant != "" && r.Tenant != "*" {
			targets = []string{r.Tenant}
		}
		for _, t := range targets {
			if existing, _ := db.ListProviders(t); len(existing) > 0 {
				continue // never overwrite an existing registry
			}
			models := make([]map[string]any, 0, len(r.Models))
			for _, m := range r.Models {
				mt := m.ModelType
				if mt == "" {
					mt = "text"
				}
				models = append(models, map[string]any{
					"id": m.ID, "name": m.Name, "context_limit": m.ContextLimit, "model_type": mt,
				})
			}
			mb, _ := json.Marshal(models)
			capability := r.Capability
			if capability == "" {
				capability = "text"
			}
			apiType := r.APIType
			if apiType == "" {
				apiType = agent.APITypeV4
			}
			_ = db.UpsertProvider(store.Provider{
				Tenant: t, ProviderID: r.ProviderID, Capability: capability,
				APIType: apiType, BaseURL: r.BaseURL, APIKey: r.APIKey,
				Headers: "null", Models: string(mb),
			})
		}
	}
}

var _ = llm.New

// loadInjectedPresets reads SYSTEM_PRESETS_FILE — the host-injected immutable
// system presets (a deployment ConfigMap), mirroring the TS agent. Two shapes:
//   - a bare ARRAY -> the `default` set (applied to every tenant);
//   - an OBJECT { "default": [...], "groups": { "<scopeId>": [...] } } -> a
//     delegation subtree may carry its own set; unknown scopes use `default`.
//
// Entries accept snake_case OR camelCase. Invalid entries are skipped.
func loadInjectedPresets(file, scopeID string) []agent.PresetRow {
	if file == "" {
		return nil
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		slog.Warn("system_presets_file read failed", "file", file, "err", err.Error())
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		slog.Warn("injected presets JSON parse failed", "err", err.Error())
		return nil
	}
	if arr, ok := v.([]any); ok {
		return parsePresetArray(arr)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if groups, ok := obj["groups"].(map[string]any); ok && scopeID != "" {
		if picked, ok := groups[scopeID]; ok {
			if arr, ok := picked.([]any); ok {
				return parsePresetArray(arr)
			}
		}
	}
	if def, ok := obj["default"].([]any); ok {
		return parsePresetArray(def)
	}
	return nil
}

func parsePresetArray(arr []any) []agent.PresetRow {
	out := make([]agent.PresetRow, 0, len(arr))
	for _, it := range arr {
		v, ok := it.(map[string]any)
		if !ok {
			continue
		}
		id := str(v["id"])
		if id == "" {
			continue
		}
		row := agent.PresetRow{
			ID:           id,
			SystemPrompt: firstStr(v["system_prompt"], v["systemPrompt"]),
			MaxTurns:     int(num(firstAny(v["max_turns"], v["maxTurns"]))),
			IsSystem:     true,
		}
		switch t := firstAny(v["system_prompt_i18n"], v["systemPromptI18n"]).(type) {
		case string:
			_ = json.Unmarshal([]byte(t), &row.SystemPromptI18N)
		case map[string]any:
			m := map[string]string{}
			for k, vv := range t {
				m[k] = str(vv)
			}
			row.SystemPromptI18N = m
		}
		switch t := v["tools"].(type) {
		case string:
			_ = json.Unmarshal([]byte(t), &row.Tools)
		case []any:
			for _, x := range t {
				if s := str(x); s != "" {
					row.Tools = append(row.Tools, s)
				}
			}
		}
		out = append(out, row)
	}
	return out
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func num(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}
func firstAny(vs ...any) any {
	for _, v := range vs {
		if v != nil {
			return v
		}
	}
	return nil
}
func firstStr(vs ...any) string {
	for _, v := range vs {
		if s := str(v); s != "" {
			return s
		}
	}
	return ""
}

// storeBackend maps the config's backend string to store.Backend.
func storeBackend(name string) store.Backend {
	switch name {
	case "postgres":
		return store.Postgres
	case "mysql":
		return store.MySQL
	default:
		return store.SQLite
	}
}

// replicaID resolves this instance's identity among replicas: the configured
// value, else the OS hostname (no env var needed).
func replicaID(configured string) string {
	if configured != "" {
		return configured
	}
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "agent"
}

// extensionAllowlistResolver returns a resolver that maps a tenant to the extIds
// its delegation subtree may use (nil = unrestricted). It reads
// seed.extension_allowlist keyed by the tenant's scope_id (subtree root).
func extensionAllowlistResolver(db *store.DB, c config.Config) func(tenant string) []string {
	if len(c.Seed.ExtensionAllowlist) == 0 {
		return nil
	}
	return func(tenant string) []string {
		scope := tenant
		if t, ok, _ := db.GetTenant(tenant); ok && t.ScopeID != "" {
			scope = t.ScopeID
		}
		if allowed, ok := c.Seed.ExtensionAllowlist[scope]; ok {
			return allowed // a present-but-empty list means "no extensions"
		}
		return nil // no entry = unrestricted
	}
}
