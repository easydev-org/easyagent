#!/usr/bin/env node
// Compare the two agents' observable behavior; report per-scenario equality.
import { readFileSync } from 'node:fs'

const ts = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const go = JSON.parse(readFileSync(process.argv[3], 'utf8'))

let ok = true
const keys = [...new Set([...Object.keys(ts), ...Object.keys(go)])].sort()
for (const k of keys) {
  const a = JSON.stringify(ts[k])
  const b = JSON.stringify(go[k])
  if (a === b) {
    console.log(`  MATCH   ${k}`)
  } else {
    ok = false
    console.log(`  DIFFER  ${k}`)
    console.log(`    TS: ${a?.slice(0, 300)}`)
    console.log(`    Go: ${b?.slice(0, 300)}`)
  }
}
console.log(ok ? 'EQUIVALENT' : 'DIFFERENCES FOUND')
process.exit(ok ? 0 : 1)
