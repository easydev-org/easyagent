package agent

import (
	"encoding/json"

	"github.com/easydev-org/easyagent/llm"
)

// DefaultCompactAtFraction is the proactive-compaction threshold: when the
// estimated request context exceeds this fraction of the model's window BEFORE
// a step, the Go agent compacts first (a Go-only feature; the TS agent only
// reacts to a provider overflow).
const DefaultCompactAtFraction = 0.9

// DefaultForceCompactTokens is the ABSOLUTE proactive-compaction threshold: once
// the estimated request context reaches this many tokens, the Go agent compacts
// regardless of the model window (a hard ceiling, e.g. for very large windows).
const DefaultForceCompactTokens = 900_000

// compactThreshold returns the effective trigger (estimated tokens) and its
// reason: the smaller of the window-fraction threshold and the absolute force
// threshold. 0 = proactive compaction disabled.
func compactThreshold(contextLimit int, frac float64, forceTokens int) (int, string) {
	t, reason := 0, ""
	if contextLimit > 0 {
		t = int(float64(contextLimit) * compactAtFraction(frac))
		reason = "threshold"
	}
	if forceTokens > 0 && (t == 0 || forceTokens < t) {
		t = forceTokens
		reason = "force"
	}
	return t, reason
}

// EstimateMessages estimates the token footprint of a request's messages: text +
// tool-call names/args + a small per-message framing cost.
func EstimateMessages(msgs []llm.Message) int {
	total := 0
	for _, m := range msgs {
		total += EstimateTokens(m.Content) + 4
		for _, tc := range m.ToolCalls {
			total += EstimateTokens(tc.Name) + EstimateTokens(tc.Args) + 4
		}
	}
	return total
}

// EstimateToolSchema estimates the token footprint of the offered tool schemas
// (name + description + JSON-schema parameters).
func EstimateToolSchema(tools []llm.ToolDef) int {
	total := 0
	for _, t := range tools {
		total += EstimateTokens(t.Name) + EstimateTokens(t.Description) + 8
		if t.Parameters != nil {
			if b, err := json.Marshal(t.Parameters); err == nil {
				total += EstimateTokens(string(b)) + 4
			}
		}
	}
	return total
}

// compactAtFraction normalizes the configured threshold (0 → 0.9, clamped to
// (0, 1]).
func compactAtFraction(f float64) float64 {
	if f <= 0 {
		return DefaultCompactAtFraction
	}
	if f > 1 {
		return 1
	}
	return f
}
