# DEVELOP

Developer notes for `easyagent`. User/deploy docs live in `README.md`.

## What this repo is

A **Go implementation of the `agent.v1` service** (`AgentService` +
`AdminService`) — the same RPC contract the reference agent serves. It is **NOT a port of the TS agent**: internal structure, module
layout, and persistence shape are free. Only the **wire contract** must match.

Read these sibling repos first:

| Repo | What you need from it |
|---|---|
| `abc-agent/agent` (proto/agent/v1) | `proto/agent/v1/agent.proto` — the single source of truth for the contract |
| local buf gen (gen/agent/v1) | Generated Go stubs (`agent/v1` + `agentv1connect`, connect-go **v2**) |
| `easybus-sdk` | The agent-side abc role: `Agent` (discover/call tools/config authority), `bus.Bus`, `transport/nats`, **session lease** |


## Decisions (locked)

1. **Scope = the `agent.v1` interfaces.** Implement the RPCs; do not replicate
   the TS agent's packages, DB schema, or module boundaries.
2. **Persistence = GORM.** Backends: **postgres, mysql, sqlite**.
3. **The sqlite storage layout need NOT match the TS agent's.** There is no
   legacy data to migrate, so all of the TS agent's sqlite upgrade machinery
   (PK rebuilds, legacy-FK rebuilds, `PRAGMA legacy_alter_table`, best-effort
   duplicate-column swallows) is **irrelevant and must not be recreated**.
4. **DB layer uses a two-phase (claim-column) pattern** so it never depends on
   `RETURNING` — see below.

## DB layer (GORM)

### Implemented: `store/`

`store` is the persistence layer: GORM models + repositories. It is a **fresh
initial schema** (`Migrate` = AutoMigrate create-if-absent) — there is NO
migration path from the TS agent's schema.

* `models.go` — `sessions` (composite PK `(tenant, name)`), `messages`,
  `parts`, `mailbox` (with the `consumed_by` CLAIM column), `providers`,
  `tenants`, `tenant_tokens`, `agent_files`.
* `db.go` — `Open(backend, dsn)` over pg/mysql/sqlite; SQLite pins
  `SetMaxOpenConns(1)`.
* `sessions.go` / `messages.go` / `mailbox.go` / `misc.go` — repositories.

Two things the TS agent did in SQL are done in Go here, to stay dialect-neutral:
* the message-chain walk (`Chain` / `DeltaSince` / `IsInChain`) is a Go loop over
  `prev_id` (the TS agent used a recursive CTE; a linked list is linear, so no
  CTE is needed and there is no dialect-specific SQL);
* `AppendMessageTip` / `MarkRead` are read-modify-write transactions
  (`SELECT ... FOR UPDATE` on pg/mysql, a plain tx on the single-writer sqlite)
  instead of `UPDATE ... RETURNING`.

### No `RETURNING` anywhere

`mysql` has no `RETURNING` clause, so the data layer must not rely on it.
`drainAll` / `insertWithId` / `markRead` / `appendMessageTip` in the TS agent
all used `UPDATE ... RETURNING`; reimplement them as follows:

* **Atomic pop / claim** — add a claim column (e.g. `consumed_by` on the
  mailbox table). In one transaction:

  ```sql
  UPDATE mailbox SET consumed_by = <runID>
   WHERE tenant = ? AND session_name = ? AND status = 'pending';
  SELECT ... WHERE consumed_by = <runID>;
  ```

  The run lease (below) already guarantees a single writer per session, so no
  `SKIP LOCKED` is needed. Order the `SELECT` explicitly — `UPDATE` gives no row
  order.
* **"Did my insert win?"** — use GORM's `RowsAffected` (`Create(...).RowsAffected > 0`),
  not `INSERT ... RETURNING`.
* **Read-back after update** (`markRead`, `appendMessageTip`) — do it inside a
  transaction (`SELECT ... FOR UPDATE` on pg/mysql), or use `gorm.Expr` for the
  counter bump and then read the row.

### Dialect differences GORM does NOT erase

GORM erases **syntax** (placeholders, identifier quoting, basic CRUD,
`ON CONFLICT DO NOTHING`); it does **not** erase semantics. Handle these
explicitly and keep them in one place (a `dialect` helper), never scattered:

* **Conflict clause**: pg/sqlite are `ON CONFLICT (<cols>) DO NOTHING`;
  mysql is `ON DUPLICATE KEY UPDATE`, which fires on **any** unique/PK conflict
  — so a `(tenant, sha256)` dedup is not equivalent on mysql (the PK also
  triggers). Prefer an explicit "check-then-insert" or a claim column.
* **Migration leader election**: do not use `pg_advisory_xact_lock` /
  `GET_LOCK`. Use a `schema_migrations` table + a unique constraint (portable
  across all three backends).
* **DDL types**: `BIGSERIAL`/`IDENTITY` vs `AUTO_INCREMENT`; `jsonb` vs `json`;
  `TEXT` vs `VARCHAR(n)`. Let GORM's Migrator emit, but review.
* **Booleans**: pg returns `true/false`, sqlite returns `1/0`. Convert
  explicitly in Go; do not assume the driver scans into `bool`.
* **Time**: sqlite has no time type — store timestamps as `TEXT` (RFC3339) or
  unix seconds; be consistent across backends.

### sqlite specifics

* **Single writer.** Set `sqlDB.SetMaxOpenConns(1)`, or DSN
  `?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)`.
  A multi-connection pool will hit `database is locked` under concurrency.
* **File backend ⇒ single replica.** A file cannot be shared across replicas
  (NFS is unsafe). sqlite deployment implies 1 replica; the lease/heartbeat
  layer degrades to a local lock there but must still be wired correctly.
* Driver: use **`modernc.org/sqlite`** (CGO-free) via `glebarez/sqlite` for
  GORM, matching the org's CGO-free worker images.

### Schema

Design the schema freely (see decision 3). Boot with an idempotent
`CREATE TABLE IF NOT EXISTS` for a fresh DB plus versioned migrations
(goose / atlas / hand-rolled — our choice) for evolution. Multi-tenant rows are
scoped by `tenant`.

## Concurrency & lifecycle

### Implemented: `bus/`

`bus` is the NATS/JetStream wiring over `easybus-sdk`; it implements the
`agent` package's `Lease` and `ToolExecutor` seams:

* **Lease** (`Claim`/`Renew`/`Release`) — KV create / CAS / delete on the
  protocol's lease bucket (`abc-session-state`), TTL 30s. A CAS miss is a LOST
  lease (abort); a transient error is reported as held (retry next tick).
* **ToolExecutor** — discovers extensions, records tool → extension ownership,
  and calls `tool.call.<extId>.<name>` via the agent role; maps the abc
  `ToolResult` to the runtime's `ToolOutcome`. An undiscovered tool triggers one
  refresh before failing.
* **ConsumeMailbox** — the durable mailbox consumer (exactly-once per replica);
  **PublishMailbox** for the Prompt route + subsession hand-off.

The runtime is transport-agnostic: tests inject `MemoryLease` + a stub tool
executor; production wires `bus`. `bus` tests boot an embedded `nats-server`
via `easybus-sdk/natsrun` and skip when the binary is absent
(`ABC_NATS_SERVER_BIN`).

Reuse `easybus-sdk`:

* **Session lease** (`agent/lease.go`): `ClaimSession` / `RenewSession` (CAS) /
  `ReleaseSession`. Renew on TTL/3; treat a transient renew error as
  "still held", only a definite CAS miss as "lost".
* **Mailbox**: durable consumer; a redelivery carries the same envelope id, so
  persistence must be idempotent by that id.
* **Config authority**: one long-lived `Agent` instance owns the manifest cache
  and `ServeConfig`; per-request instances break config writes.

Write yourself:

* The **turn loop** — implemented in `agent/` (see below).
* **Cross-replica interrupt**: publish to the mailbox wake subject; the running
  replica watches it and cancels the run.
* The three **server-streaming** RPCs (`Prompt`, `WatchSession`,
  `WatchSessions`): JetStream ordered consumer, `eid` dedup, resume by stream
  sequence.

## Turn loop (implemented: `agent/`)

`agent.RunTurn(ctx, Deps, Input)` drives one prompt through up to `MaxTurns`
model steps and emits the [`EVENTS.md`](./EVENTS.md) stream:

* per step: mint + announce the step id (`message-added streaming:true`) BEFORE
  any delta → `start-step` → provider deltas → `finish-step` →
  `message-added streaming:false` (persisted under the SAME id);
* tools run **sequentially in model order** (a side-effecting tool must not be
  overtaken — deliberately unlike the AI SDK's concurrent step execution);
* every `tool-call` is paired with a `tool-result` OR `tool-error` for the same
  `toolCallId` (rule 4);
* `retry` (from `llm.RetryingStream`'s callback) reuses the step's
  `message_id` (rule 2);
* the final step injects the wrap-up prompt and disables tools; exhausting the
  budget without a text finish yields `reason: "max-steps"`.

Seams (so the loop is testable without a bus/DB): `Emitter` (the event sink)
and `ToolExecutor` (the extension-bus seam). Both are injected.

## Gateway integration test

`agent/gateway_test.go` runs the REAL turn loop (streaming text + a live tool
call) against the cluster's **ai-multimodal-gateway** OpenAI-compatible
endpoint. It is skipped unless env is set, so `go test ./...` stays hermetic:

```sh
GW_BASE=http://ai-multimodal-gateway.worker.svc.cluster.local/v1 \
GW_KEY=<tal-coding upstream key> \
GW_MODEL=tal-coding/deepseek-v4.1-flash \
go test ./agent -run TestGatewayTurn -v
```

Notes learned from the real wire:
* The gateway's v4 base URL is `<host>/v4/ai`; the client appends
  `/language-model`. Its access control forwards the caller key upstream for
  `key: upstream` models, so `GW_KEY` is the tal-coding key.
* The upstream streams reasoning as a v4 `reasoning-delta` — no provider
  special-casing needed.
* The v4 `finish` part carries the final usage (`inputTokens.total` /
  `outputTokens.total`).

## LLM layer (implemented: `llm/`)

**Self-written, no third-party SDK.** The client speaks the **AI SDK provider
specification v4** directly — the public wire the Vercel AI Gateway, this
platform's `ai-multimodal-gateway` (`/v4/ai/language-model`), and any v4
upstream all serve. There is **no `openai-go` dependency** (go.mod has no
requires); the request body is `LanguageModelV4CallOptions`, the response is a
v4 stream-part SSE stream. Base URL is the v4 mount, e.g.
`http://ai-multimodal-gateway.worker.svc.cluster.local/v4/ai` (the client POSTs
to `<base>/language-model`).

Why v4 rather than the OpenAI `/v1/chat/completions` wire: the platform's
gateway is v4-native, and the v4 **provider-layer** stream parts
(`text-delta`, `reasoning-delta`, `tool-call`, `finish`) are exactly the
normalized events the turn loop needs — so the mapping is direct. The trade:
the v4 prompt must be built in the provider format
(`prompt: [{role, content: [parts]}]`), and direct-to-provider (non-gateway)
endpoints are out of scope by design.

The package owns exactly what the AI SDK's **provider layer** would have given
us:

* `llm.New(ProviderCredentials, httpClient)` — a client bound to one provider
  (base URL, key, extra headers). The caller resolves credentials from the
  provider registry; this package never touches a DB.
* `Client.Stream(ctx, ChatRequest)` — one v4 HTTP/SSE attempt, returning a
  channel of **normalized events** (the client-facing contract):
  `text-delta`, `reasoning-delta`, `tool-call`, `finish-step`, `error`.
  (v4 emits one fully-assembled `tool-call` per call, so no fragment joining.)
* `Client.RetryingStream(ctx, req, RetryPolicy, onRetry)` — a **bounded,
  capped (30s), VISIBLE** retry loop; `onRetry` lets the caller publish a
  durable `retry` event before each sleep.
* `IsRetryable` / `RetryAfter` — classification + `Retry-After` honoring.
  `APIError` carries the status + response headers.
* `IsContextOverflow` — the multi-provider wording table that triggers
  compaction + a single transparent step retry.

### Wire details (v4)

* Headers: `authorization: Bearer <key>`, `ai-language-model-id: <model>`,
  `ai-language-model-streaming: true`,
  `ai-language-model-specification-version: 4`.
* Prompt (body): `{"prompt":[{role,content:[parts]}], "tools":[{type:"function",
  name, description, inputSchema}], "toolChoice":{type,...}, ...}`. A prompt
  `tool-call` part's `input` is the **parsed object**; a `tool-result` part's
  `output` is `{type:"text", value}`.
* Stream parts: `text-start`/`text-delta`/`text-end`, `reasoning-*`,
  `tool-input-*`, `tool-call`, `finish` (with `finishReason:{unified,raw}` +
  nested `usage`), `error`.

### Two-layer division (what this package does NOT do)

`llm/` is the **provider layer**. The following are **AI SDK core** concerns
and live in the turn loop on top of this package, regardless of protocol:

* the step loop (`while step < maxTurns`), mailbox drain at step boundaries;
* **tool execution** and the `tool-result` / `tool-error` /
  `tool-output-denied` events (the provider never sees a tool *result*);
* `start-step` / `finish-step` framing, `abort`, `tool-approval-*`;
* `message_id` / `run_id` / `eid` stamping and persistence.

### Non-obvious decisions

* **Reasoning is a first-class v4 part** (`reasoning-delta`), so no provider
  field sniffing. Reasoning is persisted for display only and is EXCLUDED from
  the rebuilt model context.
* **Provider options / variants** pass through verbatim as extra body fields
  (`ChatRequest.Extra`), e.g. `reasoning`.
* **Tool-call execution scheduling** (sequential vs parallel) is the turn
  loop's choice, NOT part of the wire contract. The AI SDK executes a step's
  tool calls concurrently; prefer sequential-in-model-order here (side-effecting
  tools must not be overtaken).

**Hard contract:** the `Prompt` / `WatchSession` response `event` + `params`
fields are NOT fully modelled in the proto — they are the de-facto contract the
web/Flutter clients depend on. The event names emitted by `llm/` are the
provider-layer subset; the SDK-core events above must match the clients too.
The **frozen** contract (full event catalog, envelope, resume/ordering rules,
and the two AI SDK naming generations) lives in [`EVENTS.md`](./EVENTS.md).
The turn loop must emit exactly those names/shapes.

## Bundled extension

Do NOT rewrite the bundled extension in Go. Run the existing TS
`easybus-sdk/bundled-extension` as a **separate NATS extension process** — the
Go side already has the extension/agent abc roles to talk to it.

## Media & packaging

* Media metadata (width/height/duration/thumbnail) via spawning
  `ffmpeg`/`ffprobe`. `thumbhash` is a JS lib — return it unset (proto field is
  `optional`) until a Go equivalent is chosen.
* Packaging is a **Go static binary** — this is a strict improvement over the
  TS Node SEA, no action needed.

## Contract & ops (highest priority)

### Implemented: `server/` + `cmd/easyagent`

`server` is the Connect RPC surface; `cmd/easyagent` is the standalone binary.

* **Services**: `agent.v1.AgentService` + `agent.v1.AdminService` (generated
  stubs from `agent-sdk-go`, connect-go **v2**). `server.Server` implements both
  and `Register(server)` mounts them.
* **Auth** (`AuthInterceptor`, a `connect.ServerInterceptor`): reads
  `Authorization` off `connect.CallInfoForServerContext`, verifies the tenant
  bearer against `tenant_tokens` (sha256), or the static admin token
  (constant-time). `auth=none` pins every request to the default tenant.
  AdminService requires the admin token (a tenant token is rejected).
* **Events** (`EventHub`): an in-process fan-out to live `WatchSession` /
  `Prompt` subscribers. `Prompt` subscribes BEFORE waking, enqueues a `trigger`
  (the mailbox is the single chain writer), and streams the run's events until
  `turn-complete`. **Multi-replica note**: the hub is per-process; a multi-
  replica deployment must back it with the durable NATS session-event stream
  (the interface is small so the swap is local).
* **Binary** (`cmd/easyagent`): wires store + runtime + server, mounts
  AgentService/AdminService over **h1 + h2c**, and serves a **separate
  plain-HTTP/1.1 health port** (`AGENT_HEALTH_ADDR`, default `:8081`) — the RPC
  surface may be h2c-only, which a kubelet `httpGet` probe cannot speak.
  Env: `PORT`, `DB_BACKEND`/`DATABASE_URL`, `NATS_URL` (when set, the KV lease +
  mailbox consumer + extension tools come from the bus), `AGENT_AUTH_MODE`,
  `AGENT_ADMIN_TOKEN`, `AGENT_DEFAULT_TENANT`, `AGENT_CORS_ORIGIN`,
  `AGENT_DEFAULT_MAX_TURNS`, `AGENT_CONFIG_SEED`.
* **Dockerfile**: multi-stage, `CGO_ENABLED=0` static build → alpine runtime.

"Implement the interfaces" means the existing clients (webui / Flutter / SDKs)
must keep working. Lock these down first:

* **Auth**: tenant bearer tokens stored as sha256 only; static admin token
  (constant-time compare, never logged); provider API keys masked on the RPC
  surface.
* **Health**: a separate plain-HTTP/1.1 liveness port (the RPC surface may be
  h2c-only, and a kubelet `httpGet` probe cannot speak cleartext HTTP/2).
* **Deploy**: image + Helm wiring; CORS for browser clients.

## Remaining work, by priority

1. Contract compatibility (RPC + auth + health + deploy).
2. LLM client + event shapes.
3. Turn loop / watch streams / concurrency.
4. DB layer (two-phase pattern + dialect helper + migrations).
5. Bundled extension (run TS as an out-of-process extension).
6. Media / i18n (deferrable).

### Status: implemented

All of the above have a working implementation in this repo:

| Area | Package | Notes |
|---|---|---|
| Contract + RPC + auth + health + deploy | `server/`, `cmd/easyagent`, `Dockerfile` | AgentService + AdminService, h1+h2c, health port |
| LLM client + event shapes | `llm/` | self-written v4 client; events per `EVENTS.md` |
| Turn loop / watch / concurrency | `agent/`, `bus/` | turn loop, runtime, lease, mailbox, durable event stream |
| DB layer | `store/` | two-phase, no `RETURNING`, pg/mysql/sqlite |
| Bundled extension | — | run the TS extension out-of-process over the bus (not rewritten) |
| i18n / compaction / variants / idlewatch | `agent/` | locale + env block, fold checkpoint, reasoning variants, idlewatch |

### Deliberate gaps (long-term enhancements)

* **models.dev catalog**: the TS agent refreshes a models.dev catalog for
  variant metadata. Here variants are the fixed v4 reasoning levels (no catalog
  fetch); a planned **TS sidecar** owns models.dev + non-v4 protocol adaptation,
  so the Go agent stays v4-only.
* ~~**KV persistence**~~ **DONE**: presets + tenant config use **JetStream KV**
  when `NATS_URL` is set (`bus.NATSKV`, buckets `abc-presets` /
  `easyagent-config`); in-memory otherwise (dev). `Keys()` reads the bucket
  snapshot via a bounded watch (JetStream KV has no native listing).

### Implemented (added)

* **Logging**: standard library `log/slog` (text by default; `AGENT_LOG_FORMAT=json`
  for JSON). `LOG_LEVEL` sets the level. No third-party logger.
* **Blob backend (`blob/`)**: durable file bytes live in **S3-compatible**
  object storage only (no NATS object store) — `AGENT_BLOB_BACKEND=s3` +
  `AGENT_S3_{BUCKET,REGION,ENDPOINT,ACCESS_KEY,SECRET_KEY,FORCE_PATH_STYLE,PREFIX}`.
  Without it, bytes are in-memory (dev/single-replica).
* **Media probe (`media/`)**: pure Go, **no ffmpeg** — mime via
  `gabriel-vasile/mimetype`; image dimensions + a bounded thumbnail via
  `disintegration/imaging`; MP4/M4A duration + dimensions via `abema/go-mp4`.
  Video thumbnails are NOT produced (no pure-Go decoder) — the field stays unset.
* **Lifecycle events**: `bus.PublishLifecycle` broadcasts
  `created`/`forked`/`renamed`/`deleted` on `abc.<tenant>.session.lifecycle.<kind>`
  (durable inbox publish, matching the TS agent) — fired by CreateSession /
  DeleteSession / Rename / Fork. Best-effort: consumers must tolerate a missed
  event and converge via their own reconciliation.
* **Consumer hygiene (verified, no leak)**: every session-scoped live
  subscription is a CORE NATS `SubscribeSync` (`WatchInterrupts`,
  `SubscribeSessionEvents`) — NOT a JetStream consumer; `cancel()/stop()` →
  `Unsubscribe()`. JetStream consumers are used only in the bounded `Replay`
  (`defer Unsubscribe`) and the single durable mailbox consumer per replica.
  `bus/leak_test.go` asserts (via a monitored nats-server's `jsz.consumers`)
  that 30 subscribe/cancel cycles and 5 mailbox consume/stop cycles do not
  accumulate consumers.
* **Presence bucket init (bug fix)**: `bus.EnsurePresenceBucket` pre-creates the
  `abc-presence` KV bucket so easybus-sdk-go's `ensurePresence` watch does not
  error (the library then reassigns its `sync.Once` INSIDE `Do` and panics with
  `sync: unlock of unlocked mutex` — reachable from `ToolExecutor.Refresh`, i.e.
  boot + every tool-call fallback). The init TTL is `15000ms` (= the protocol's
  `extension.PresenceTTL`): a NATS KV bucket's TTL is fixed at creation, so it
  must match the extensions' per-key TTL, and it must be ≥ 100ms (a 1ms value is
  rejected, which is why the earlier workaround silently failed).
* **Seeds**: `AGENT_PROVIDER_SEED` (JSON array of providers, seeded only into
  tenants that have none) + `AGENT_EXT_CONFIG_SEED` (JSON `{tenant,extId,name,
  value}`, create-if-absent). The default preset now carries en/zh prompts.
  * **Runtime tenants are seeded too**: `CreateTenant` (AdminService) runs the
    same per-tenant seeding as boot (`Server.SetTenantSeeder` →
    `seedTenantDefaults`), so a tenant created at runtime is immediately usable
    (providers + `default_model` + ext config), not just at first boot. Mirrors
    the TS `seedTenantDefaults` (#32).
* **Bundled extension (`bundled/`)**: the Go rewrite of the TS
  `@easybus-sdk/bundled-extension`, running **in-process** (not a NATS
  extension server). It implements the `agent.ToolRegistry` + `agent.ToolExecutor`
  seams directly; its tool set + zh/en descriptions come from the SAME
  `manifest.yaml` (embedded via `go:embed`), so it stays in lockstep with the TS
  extension. Host capabilities (the TS `BundledDeps` equivalent) are injected via
  `bundled.Deps` — implemented by `bundled.StoreDeps` over the store + blob +
  provider registry.
  * Tools: `todo-write`, `history-search`, `history-range`, `file-info`,
    `file-read`, `image-read`, `image-generate`, `image-edit`, `video-generate`,
    `tts-generate`, `tts-clone`, `audio-transcribe`, `web-fetch`,
    `brave-search`, `subsession-create`, `mail-send`.
  * **`time-wait` is intentionally NOT included** (decided).
  * `web-fetch` HTML→text/markdown uses `golang.org/x/net/html` (parsed tree),
    not regexes.
  * `history-*` does NOT surface a `change_id` (that came from workspace
    extension metadata; not an agent-core concept — dropped).
  * `agent.NewCompositeTools` merges the bundled (in-process) tools with the
    out-of-process NATS extension tools behind one registry/executor; a call is
    routed to the owning source (`ErrUnknownTool` marks a miss for fall-through).
* **Tool names (aligned with TS)**: the AI/LLM-visible tool name is the tool's
  OWN **sanitized** name (`LLMToolName`, charset `[A-Za-z0-9_-]`, out-of-charset
  → `-`), NOT `extId`-prefixed — extensions self-namespace, so two extensions
  never collide. `ListTools.name` returns the tool's own name (`category` = the
  extension id); the host-facing `QualifiedName` (`<extId>-<name>`) is used only
  by the denylist. Whitelist entries match bare / `<extId>.<name>` /
  `<extId>-<name>`.
* **Generation endpoints (`llm/`)**: `GenerateImage` (`/v4/ai/image-model`),
  `GenerateSpeech` (`/speech-model`), `Transcribe` (`/transcription-model`),
  `GenerateVideo` (`/video-model`, SSE), `Vision` (image → a v4 language-model
  call). These back the bundled generation tools; the agent itself only speaks v4.
* **Proactive compaction (Go-only)**: when the ESTIMATED request context (system
  + messages + tool schemas, via `EstimateTokens`) reaches `AGENT_COMPACT_AT_FRACTION`
  (default 0.9) of the model's window BEFORE a step, the turn loop folds the
  session and retries with the trimmed context — instead of waiting for a
  provider overflow error. The window comes from the provider row's model
  `context_limit` (or `RuntimeDeps.ContextLimit`); no limit → feature off.
  Emits `compacted` with `reason: "threshold"` (vs `"overflow"`).
  **Not in the TS agent** (Go-only).
  * **Absolute force ceiling**: `AGENT_FORCE_COMPACT_TOKENS` (default **900000**)
    triggers compaction once the estimate reaches that many tokens, REGARDLESS
    of the window — the SMALLER of the two thresholds wins. `reason: "force"`.
    (Negative disables the ceiling.)
  * **Continuation after compaction**: the `Compact` callback re-anchors the
    persistence sink to the NEW tip (`sink.prevID = newTip`), so the running
    turn continues on the compacted chain and its steps persist under the
    checkpoint — NOT branching off the pre-compaction tip.
* **Compaction checkpoints are fed back**: `rebuildHistory` renders a
  `compaction` message's `summary` part as a `[compacted earlier context]` user
  block, so a folded session actually reaches the model (previously dropped).
  * **The folded prefix is DROPPED** (fixed): a checkpoint records its
    `tail_from` (the first message of the verbatim tail); `rebuildHistory`
    rebuilds ONLY rows at/after that boundary, preceded by a
    `<conversation-checkpoint>` block (same shape as the TS `spliceContext`).
    Previously the whole chain was rebuilt, so a fold did NOT shrink the context
    (and the threshold would re-fire forever).
  * **Budgets**: ALWAYS a fraction of a window — keep ~20% verbatim / fold ~10%.
    An UNCONFIGURED window is treated as `DefaultContextLimit` (200k), so there
    is ONE rule with no fixed-budget branch (this also lets the absolute `force`
    trigger work for an unknown window). Matches the TS #38.

## Build / verify

### Cross-implementation equivalence harness (Go vs TS)

`scripts/equiv/run.sh` boots the deterministic v4 **mock gateway**
(`llm/v4mock`, also runnable standalone via `cmd/v4mock-gateway`), the **TS
agent** (`.sea/easyagent`, built with Node 26), and the **Go agent**, then
drives BOTH through the identical Connect/JSON scenario (`drive.mjs`) and diffs
the observable behavior (`diff.mjs`). Requires Node 26 + a built TS SEA + a
`nats-server`.

The mock's output is derived ONLY from the request prompt, so both agents get
byte-identical model output. Volatile fields (ids/timestamps/usage) are
normalized; `models.variants` (models.dev, TS-only until a catalog is wired) and
`time-wait` (intentionally absent from Go) are excluded. Current result: all
scenarios **EQUIVALENT**.


```sh
go build ./...
go vet ./...
gofmt -l .          # must be empty
go test ./...
```

Module path is `github.com/easydev-org/easyagent`. Modules come from the private
**artifact** registry (see `easybus-sdk-go/DEVELOP.md`); `proxy.golang.org` is
unreachable in-cluster:

```sh
export GOPROXY=http://artifact.worker.svc.cluster.local/artifacts/go
export GOSUMDB=off
```

The sandbox image ships no Go toolchain; install one via artifact's netcache
(see `agent-proto/DEVELOP.md` for the `netcache` pattern):

```sh
curl -fsSL -o /tmp/go.tgz \
  http://artifact.worker.svc.cluster.local/artifacts/netcache/go.dev/dl/go1.27.1.linux-amd64.tar.gz
tar -C /usr/local -xzf /tmp/go.tgz && export PATH=/usr/local/go/bin:$PATH
```
