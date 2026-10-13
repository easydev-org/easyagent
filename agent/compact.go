package agent

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/easydev-org/easyagent/store"
)

// DefaultContextLimit is the assumed model window (tokens) when a session's
// model has NO configured context window. The compaction budgets are ALWAYS a
// fraction of the (configured or default) window, so there is ONE rule with no
// fallback branch — and the absolute `force` threshold still works without a
// configured window.
const DefaultContextLimit = 200_000

// Compaction budget fractions of the model window.
const (
	compactTailFraction = 2 // 20% kept verbatim
	compactFoldFraction = 1 // 10% folded
)

// CompactSession folds the older part of a session's chain into a deterministic
// Q&A checkpoint (no LLM) and appends it as a `compaction` message carrying a
// `summary` part. The kept tail is verbatim. Returns true when a checkpoint was
// written.
func (r *Runtime) CompactSession(ctx context.Context, tenant, session, reason string) (bool, error) {
	tail, fold := r.compactBudgets(tenant, session)
	return r.compactWithBudgets(ctx, tenant, session, reason, tail, fold)
}

// compactBudgets resolves the tail/fold budgets as a FRACTION of the model's
// context window (20% / 10%), using DefaultContextLimit when the model has no
// configured window — a single rule, no fallback branch. Test overrides
// (compactTailTokens/compactFoldTokens) win when set (>0).
func (r *Runtime) compactBudgets(tenant, session string) (int, int) {
	limit := 0
	if r.deps.DB != nil {
		if s, ok, _ := r.deps.DB.GetSession(tenant, session); ok {
			limit = r.contextLimit(tenant, s.Model)
		}
	}
	if limit <= 0 {
		limit = DefaultContextLimit
	}
	tail := limit * compactTailFraction / 10
	fold := limit * compactFoldFraction / 10
	if r.compactTailTokens > 0 {
		tail = r.compactTailTokens
	}
	if r.compactFoldTokens > 0 {
		fold = r.compactFoldTokens
	}
	return tail, fold
}

// compactWithBudgets is the injectable core (tests use tiny budgets).
func (r *Runtime) compactWithBudgets(ctx context.Context, tenant, session, reason string, tailTokens, foldTokens int) (bool, error) {
	tip, err := r.deps.DB.SessionTip(tenant, session)
	if err != nil {
		return false, err
	}
	msgs, err := r.deps.DB.Chain(tenant, tip, 0, "")
	if err != nil {
		return false, err
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	parts, err := r.deps.DB.PartsByMessages(tenant, ids)
	if err != nil {
		return false, err
	}
	entries, snippets := foldEntries(msgs, parts)
	split := SplitScan(entries, tailTokens, foldTokens)
	if len(split.Folded) == 0 {
		return false, nil // nothing to fold
	}
	summary := FoldQA(split.Folded, snippets)
	if summary == "" {
		return false, nil
	}
	// The checkpoint anchors at the tip and is the new chain tip.
	id := newUUID()
	created, err := r.deps.DB.InsertMessage(tenant, id, CompactionRole, strOrNil(tip), "system:compact")
	if err != nil {
		return false, err
	}
	if !created {
		return false, nil
	}
	// The tail boundary lets the history builder DROP the folded prefix (only
	// rows at/after `tail_from` are rebuilt verbatim, behind the checkpoint).
	tailFrom := ""
	if len(split.Tail) > 0 {
		tailFrom = split.Tail[0].ID
	}
	foldedTokens := 0
	for _, e := range split.Folded {
		foldedTokens += EntryCost(e)
	}
	if err := r.deps.DB.InsertPart(tenant, id, "summary", 0, map[string]any{
		"summary": summary, "reason": reason,
		"tail_from": tailFrom, "folded_count": len(split.Folded), "folded_tokens": foldedTokens,
	}); err != nil {
		return false, err
	}
	if _, err := r.deps.DB.AppendMessageTip(tenant, session, id); err != nil {
		return false, err
	}
	_ = emitBg(r.emitterFor(tenant, session), "compacted", map[string]any{"reason": reason})
	return true, nil
}

// foldEntries builds compaction scan entries from persisted messages + parts.
func foldEntries(msgs []store.Message, parts []store.Part) ([]FoldEntry, map[string][]string) {
	byMsg := map[string][]store.Part{}
	for _, p := range parts {
		byMsg[p.MessageID] = append(byMsg[p.MessageID], p)
	}
	out := make([]FoldEntry, 0, len(msgs))
	snippets := map[string][]string{}
	for _, m := range msgs {
		e := FoldEntry{ID: m.ID, Role: m.Role}
		for _, p := range byMsg[m.ID] {
			switch p.Type {
			case "text":
				var d textPart
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					e.Text += d.Text
				}
			case "tool":
				var d toolPart
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					e.ToolCalls++
					b, _ := json.Marshal(d.Input)
					e.ToolInput += EstimateTokens(string(b))
				}
			case "tool_result":
				var d toolResultPart
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					e.ToolResult += EstimateTokens(d.Content)
					snippets[m.ID] = append(snippets[m.ID], d.Content)
				}
			}
		}
		out = append(out, e)
	}
	return out, snippets
}

// IsCompactionPreview reports whether a session tip is a compaction checkpoint
// (the client substitutes a localized label for the preview).
func IsCompactionPreview(lastPreview string) bool {
	return strings.TrimSpace(lastPreview) == CompactionPreview
}
