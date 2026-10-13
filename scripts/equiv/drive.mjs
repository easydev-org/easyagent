#!/usr/bin/env node
// Drive one agent (TS or Go) over Connect/JSON with a fixed scenario and dump
// the observable behavior as JSON for comparison. Node (no python in sandbox).
import { writeFileSync } from 'node:fs'

const PORT = process.argv[2]
const OUT = process.argv[3]
const BASE = `http://127.0.0.1:${PORT}`

async function rpc(method, body) {
  try {
    const res = await fetch(`${BASE}/agent.v1.AgentService/${method}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
    const text = await res.text()
    try {
      return JSON.parse(text || '{}')
    } catch {
      return { _raw: text.slice(0, 200) }
    }
  } catch (e) {
    return { _error: String(e) }
  }
}

const out = {}
out.identity = await rpc('GetIdentity', {})
out.create = await rpc('CreateSession', { name: 'eq1' })
out.get = await rpc('GetSession', { id: 'eq1' })
out.watch = await rpc('WatchSession', { id: 'eq1' })
out.prompt = await rpc('Prompt', { id: 'eq1', text: 'hello' })
out.list_after = await rpc('ListSessions', {})
out.messages = await rpc('ListMessages', { id: 'eq1' })
out.models = await rpc('ListModels', { providerId: 'mock' })
out.tools = await rpc('ListTools', {})

// Normalize: drop volatile fields so the diff is about BEHAVIOR, not ids/time.
const VOLATILE = new Set([
  'id', 'createdAt', 'updatedAt', 'lastMessageAt', 'lastTurnAt',
  'eid', 'seq', 'runId', 'message_id', 'prev_id', 'usage',
  'lastInputTokens', 'lastOutputTokens', 'sessionName', 'session_name',
  // models.dev-derived metadata: the Go agent ships fixed v4 reasoning levels
  // until a models.dev catalog is wired; the TS agent fetches it. Compare the
  // rest of the model surface (contextLimit etc.) but not variants.
  'variants',
])
const scrub = x => {
  if (Array.isArray(x)) return x.map(scrub)
  if (x && typeof x === 'object') {
    // Canonicalize key order so the diff is about VALUES, not map iteration.
    const o = {}
    for (const k of Object.keys(x).sort()) if (!VOLATILE.has(k)) o[k] = scrub(x[k])
    return o
  }
  return x
}

// Sort tool lists by a stable key (the two agents order discovery differently).
if (out.tools && Array.isArray(out.tools.tools)) {
  out.tools.tools = out.tools.tools
    // `time-wait` is INTENTIONALLY absent from the Go agent (decided): exclude
    // it from both sides so the diff measures everything else.
    .filter(t => !String(t.description || '').startsWith('Wait for a fixed number'))
    .sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1))
}

writeFileSync(OUT, JSON.stringify(scrub(out), null, 2))
console.log(`wrote ${OUT}`)
