# easyagent

The Go implementation of the **`agent.v1`** service (`AgentService` +
`AdminService`) — the orchestration backend for the platform's clients
(web / desktop / mobile / SDKs).

## Stack

- **RPC**: connect-go **v2**, stubs generated from `proto/agent/v1`
  (see local buf gen in `gen/agent/v1`).
- **Transport**: NATS/JetStream via **easybus-sdk**.
- **LLM**: a self-written client speaking the **AI SDK provider spec v4**
  (`/v4/ai/language-model`) — no OpenAI SDK, no AI SDK.
- **Persistence**: GORM over **postgres / mysql / sqlite** (sqlite layout is
  independent of the TS agent's).

See `DEVELOP.md` for the locked decisions, the two-phase DB pattern (no
`RETURNING`, because mysql lacks it), and the remaining work.

## Layout

```
cmd/easyagent/  standalone binary (h1+h2c RPC + separate health port)
server/          Connect RPC: AgentService + AdminService, auth, event hub
agent/           turn loop + session runtime (lease, mailbox drain, history,
                 persist, presets/env, compaction, idlewatch, tool registry)
llm/             self-written AI SDK v4 gateway client (streaming, retry,
                 overflow detection)
bus/             NATS wiring: KV lease, mailbox consumer, extension tools,
                 durable session-event stream
store/           GORM persistence (initial schema; pg/mysql/sqlite)
```

## Run

```sh
# dev (sqlite, no auth, in-process lease)
AGENT_AUTH_MODE=none DB_BACKEND=sqlite DATABASE_URL=/tmp/agent.db \
  go run ./cmd/easyagent

# cluster (postgres + NATS)
DB_BACKEND=pg DATABASE_URL=postgres://... NATS_URL=nats://nats:4222 \
  AGENT_AUTH_MODE=required AGENT_ADMIN_TOKEN=... go run ./cmd/easyagent
```

The RPC surface listens on `PORT` (h1 + h2c); a plain-HTTP/1.1 liveness probe
listens on `AGENT_HEALTH_ADDR` (`/healthz`).

## Verify

```sh
go build ./... && go vet ./... && gofmt -l . && go test ./...
```

The real-gateway integration test is env-gated (see `DEVELOP.md`).

