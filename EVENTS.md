# Event contract (frozen) — `Prompt` / `WatchSession`

The RPCs `Prompt` and `WatchSession` stream `{event, params}` frames whose
`event` names and `params` shapes are **not modelled in `agent.v1`** — they are
the de-facto contract the web / Flutter / desktop clients consume. This file
freezes that contract so the Go turn loop and the TS agent emit the same thing.

Sources of truth used to derive it:

* `abc-protocol/agent` TS: `packages/agent/src/events.ts`,
  `packages/agent/src/session-agent.ts` (the `fullStream` pass-through),
  `packages/agent/src/stream-parts.ts`,
  `packages/server/src/handlers/{messages,watch}.ts`.
* AI SDK `TextStreamPart` (the `fullStream` union) — the passthrough vocabulary.
* AI SDK provider spec v4 (`language-model-v4-stream-part.ts`) — the lower layer.

## Envelope

Every frame published on the session event stream is:

```jsonc
{ "event": "<name>", "params": { ... }, "eid": "<uuid>" }
```

* `eid` — a per-frame unique id, used for dedup across replay + live.
* `params.run_id` — stamped on every event of a live run (turn). Events that
  are NOT part of a run (`message-added` for a user prompt, `chain-changed`)
  carry no `run_id`, and `WatchSession`'s live-run filter MUST exempt them.
* `WatchSession` additionally wraps each frame with the JetStream `seq`
  (`WatchSessionResponse.seq`); resume is by `since_seq` (O(1)
  `by_start_sequence`), with `since_msg` (replay from that message's
  `created_at`) as the page-refresh fallback. `since_seq` wins when both set.

## Event catalog

`Layer` says which layer produces the event — this is what determines whether
`llm/` (the provider layer) emits it or the turn loop (SDK-core layer) does.

| `event` | Layer | `params` (beyond `run_id`) |
|---|---|---|
| `accepted` | RPC (`Prompt` only) | `message_id` — the prompt's logical id, minted by the route |
| `message-added` | SDK-core | `message_id`, `prev_id`, `role`, `streaming` (bool), `source` |
| `status` | SDK-core | `type`: `busy` \| `idle` |
| `start-step` | SDK-core | `warnings` (SDK passthrough; request/response bodies dropped) |
| `text-delta` | provider | `text`, `message_id` |
| `reasoning-delta` | provider | `text`, `message_id` |
| `reasoning-file` | provider | `file: "file:<code>"`, `mediaType`, `code`, `name`, `size`, `message_id` |
| `file` | provider | `file: "file:<code>"`, `mediaType`, `code`, `name`, `size`, `message_id` |
| `tool-input-start` / `tool-input-delta` / `tool-input-end` | provider | SDK passthrough (`id`/`toolName`/`delta`), `message_id` |
| `tool-call` | provider | `toolCallId`, `toolName`, `input`, `message_id` |
| `tool-result` | **SDK-core** | `toolCallId`, `toolName`, `input`, `output`, **plus** `formatted` (= output.content or ""), `data` (= output.metadata), `change_id` (when `metadata.change_id`), `message_id` |
| `tool-error` | **SDK-core** | `toolCallId`, `toolName`, `error`, `message`, `message_id` |
| `tool-output-denied` | **SDK-core** | `toolCallId`, `toolName`, `error: "denied"`, `message_id` |
| `tool-approval-request` / `tool-approval-response` | **SDK-core** | SDK passthrough |
| `finish-step` | SDK-core | `usage`, `finishReason`, `rawFinishReason`, `providerMetadata`, `message_id` |
| `finish` | SDK-core | `finishReason`, `rawFinishReason`, `totalUsage` |
| `abort` | SDK-core | — |
| `error` | SDK-core | `error`, `message`, `message_id` |
| `retry` | SDK-core | `message_id`, `attempt`, `delay_ms`, `reason` |
| `compacted` | SDK-core | `reason`: `manual` \| `overflow` |
| `chain-changed` | SDK-core | `tip_id`, `reason` (no `run_id`) |
| `turn-complete` | SDK-core | `reason` |
| `source` / `custom` / `raw` | provider | SDK passthrough |

Every streamed part also carries `message_id` (the server-minted step id), so a
client groups deltas into the right bubble without guessing.

> **Go implementation scope.** `file` / `reasoning-file` (model-generated media)
> are **intentionally NOT supported** in the Go agent (decided): the `llm` client
> does not surface them and the turn loop does not emit them. The event names
> remain in this catalog for the TS agent's compatibility only.

## What `llm/` emits vs what the turn loop must add

`llm/` (implemented) emits the **provider-layer subset**: `text-delta`,
`reasoning-delta`, `tool-call`, `finish-step`, `error`. The turn loop owns:

* `message-added` (mint + announce the step id BEFORE streaming),
* `start-step` / `finish-step` framing, `finish`, `abort`,
* **`tool-result` / `tool-error` / `tool-output-denied`** (the provider never
  sees a tool *result*),
* `retry` (published from `llm.RetryingStream`'s `onRetry`),
* `status`, `compacted`, `chain-changed`, `turn-complete`,
* `message_id` / `run_id` stamping.

## Naming: two AI SDK generations

The TS agent reads `result.fullStream`, whose part names are the older
`TextStreamPart` vocabulary (`text-delta`, `tool-call`, `tool-result`, …). The
AI SDK **provider** spec v4 renamed some of these (`text-start`/`text-delta`/
`text-end`, `reasoning-start`/`delta`/`end`, `tool-input-*`, `stream-start`,
`response-metadata`). **The client-facing contract is the `fullStream`
generation** — that is what the existing clients parse. Do NOT emit the v4
provider names on the wire; map onto the names above.

## Resuming / ordering rules (do not regress)

1. `status:idle` MUST be durably published BEFORE the run lease is released
   (otherwise a next turn's `busy` can land first and watchers close the live
   turn mid-flight). Use the awaited publish path.
2. `retry` carries the SAME `message_id` as the step it retries, so the client
   CLEARS the partial bubble before the retried attempt's deltas arrive (else
   the two attempts concatenate). `persistStep` writes exactly ONE row.
3. A step's `message_id` is minted and announced (`message-added`,
   `streaming: true`) BEFORE any delta, and `persistStep` writes the SAME id.
4. Tool calls are paired: every `tool-call` must be followed by a
   `tool-result` OR `tool-error` for the same `toolCallId` (a failed/aborted
   tool still emits a result so the provider never sees a dangling call).
