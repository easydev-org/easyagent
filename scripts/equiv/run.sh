#!/usr/bin/env bash
# Cross-implementation equivalence harness: boot the deterministic v4 mock
# gateway, the TS agent, and the Go agent, drive BOTH with the identical
# scenarios, and diff the observable behavior.
#
# Usage: bash scripts/equiv/run.sh
set -uo pipefail

ROOT="${ROOT:-$HOME/workspace}"
GO_DIR="$ROOT/easyagent"
TS_DIR="$ROOT/ts-agent"
OUT="${OUT:-/tmp/equiv}"
NODE_BIN="$(ls -d /usr/local/lib/nodejs/node-v*/bin 2>/dev/null | head -1)"
export PATH="/usr/local/go/bin:/usr/local/bin:$NODE_BIN:$PATH"
export GOPROXY=http://artifact.worker.svc.cluster.local/artifacts/go GOSUMDB=off
export GOFLAGS=-mod=mod
MOCK_ADDR="127.0.0.1:19000"
NATS_PORT=14222
mkdir -p "$OUT"; rm -f "$OUT"/*.log "$OUT/ts.db" "$OUT/go.db"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.5; }
trap cleanup EXIT

echo "== 0. clean stale procs =="
pkill -f 'easyagent' 2>/dev/null; pkill -f 'v4mock-gateway' 2>/dev/null
pkill -f "nats-server -p $NATS_PORT" 2>/dev/null
sleep 1

echo "== 1. mock v4 gateway =="
( cd "$GO_DIR" && go build -o "$OUT/v4mock-gateway" ./llm/v4mock/cmd/v4mock-gateway )
MOCK_ADDR="$MOCK_ADDR" "$OUT/v4mock-gateway" >"$OUT/mock.log" 2>&1 &
PIDS+=($!)
sleep 1

echo "== 2. nats-server =="
nats-server -p "$NATS_PORT" -js -sd "$OUT/nats" >"$OUT/nats.log" 2>&1 &
PIDS+=($!)
sleep 1

echo "== 3. TS agent =="
PORT=18080 AGENT_HEALTH_PORT=18081 DB_BACKEND=sqlite DATABASE_URL="$OUT/ts.db" \
NATS_URL="nats://127.0.0.1:$NATS_PORT" AGENT_AUTH_MODE=none AGENT_DEFAULT_TENANT=default \
AGENT_PROVIDER_SEED="[{\"tenant\":\"*\",\"providerId\":\"mock\",\"capability\":\"text\",\"apiType\":\"vercel-compatible-gateway\",\"baseUrl\":\"http://$MOCK_ADDR/v4/ai\",\"apiKey\":\"k\",\"models\":[{\"id\":\"mock/echo\",\"name\":\"mock\",\"contextLimit\":100000}]}]" \
AGENT_CONFIG_SEED="[{\"tenant\":\"default\",\"key\":\"default_model\",\"value\":\"mock/mock/echo\"}]" \
"$TS_DIR/.sea/easyagent" >"$OUT/ts.log" 2>&1 &
PIDS+=($!)

echo "== 4. Go agent =="
PORT=:18090 AGENT_HEALTH_ADDR=:18091 DB_BACKEND=sqlite DATABASE_URL="$OUT/go.db" \
NATS_URL="nats://127.0.0.1:$NATS_PORT" AGENT_AUTH_MODE=none AGENT_DEFAULT_TENANT=default \
AGENT_PROVIDER_SEED="[{\"tenant\":\"*\",\"providerId\":\"mock\",\"capability\":\"text\",\"apiType\":\"vercel-compatible-gateway\",\"baseUrl\":\"http://$MOCK_ADDR/v4/ai\",\"apiKey\":\"k\",\"models\":[{\"id\":\"mock/echo\",\"name\":\"mock\",\"contextLimit\":100000}]}]" \
AGENT_CONFIG_SEED="[{\"tenant\":\"default\",\"key\":\"default_model\",\"value\":\"mock/mock/echo\"}]" \
"$GO_DIR/.equiv/easyagent" >"$OUT/go.log" 2>&1 &
PIDS+=($!)
sleep 4

echo "== 4b. wait for readiness =="
wait_ready() {
  local port=$1 name=$2
  for i in $(seq 1 60); do
    if curl -s -m 2 -X POST "http://127.0.0.1:$port/agent.v1.AgentService/Health" \
        -H 'content-type: application/json' -d '{}' 2>/dev/null | grep -q '"ok"'; then
      echo "  $name ready after ${i}s"; return 0
    fi
    sleep 1
  done
  echo "  $name NOT ready after 60s"; return 1
}
wait_ready 18080 TS || true
wait_ready 18090 Go || true

echo "== 5. drive both =="
node "$GO_DIR/scripts/equiv/drive.mjs" 18080 "$OUT/ts.json" || echo "TS drive failed"
node "$GO_DIR/scripts/equiv/drive.mjs" 18090 "$OUT/go.json" || echo "Go drive failed"

echo "== 6. diff =="
node "$GO_DIR/scripts/equiv/diff.mjs" "$OUT/ts.json" "$OUT/go.json"
rc=$?
if [ $rc -ne 0 ]; then
  echo "--- ts.log ---"; tail -20 "$OUT/ts.log"
  echo "--- go.log ---"; tail -20 "$OUT/go.log"
fi
exit $rc
