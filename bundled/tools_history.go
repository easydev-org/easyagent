package bundled

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

// historyEntry is one message as the history tools surface it.
type historyEntry struct {
	ID      string
	Role    string
	Depth   int
	Tool    string
	Content string
	Time    string
}

// collectHistory walks the FULL chain (no depth cap) and returns entries
// oldest-first plus the full chain length.
func (e *Extension) collectHistory(tenant, session string) []historyEntry {
	tip := e.deps.SessionTip(tenant, session)
	if tip == "" {
		return nil
	}
	chain := e.deps.MessageChain(tenant, tip, 0) // 0 = no cap
	if len(chain) == 0 {
		return nil
	}
	ids := make([]string, len(chain))
	for i, m := range chain {
		ids[i] = m.ID
	}
	parts := e.deps.MessageParts(tenant, ids)
	byMsg := map[string][]MessagePartRow{}
	for _, p := range parts {
		byMsg[p.MessageID] = append(byMsg[p.MessageID], p)
	}
	out := make([]historyEntry, 0, len(chain))
	for _, m := range chain {
		e := historyEntry{ID: m.ID, Role: m.Role, Depth: m.Depth, Time: m.CreatedAt}
		var text string
		for _, p := range byMsg[m.ID] {
			switch p.Type {
			case "text":
				var d struct {
					Text string `json:"text"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					text += d.Text
				}
			case "summary":
				var d struct {
					Summary string `json:"summary"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					text += d.Summary
				}
			case "tool":
				var d struct {
					Name string `json:"name"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil && e.Tool == "" {
					e.Tool = d.Name
				}
			}
		}
		e.Content = text
		out = append(out, e)
	}
	return out
}

func (e *Extension) historySearch(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	_ = locale
	query := strings.ToLower(strArg(args, "query"))
	if query == "" {
		return agent.ToolOutcome{}, &toolError{msg: "history-search: missing \"query\""}
	}
	from, to := strArg(args, "from"), strArg(args, "to")
	offset, limit := 0, 50
	if f, ok := numArg(args, "offset"); ok {
		offset = int(f)
	}
	if f, ok := numArg(args, "limit"); ok {
		limit = int(f)
	}
	maxChars := 100000
	if f, ok := numArg(args, "max_total_chars"); ok {
		maxChars = int(f)
	}

	entries := e.collectHistory(tenant, session)
	// Newest-first matching.
	var matches []historyEntry
	for i := len(entries) - 1; i >= 0; i-- {
		en := entries[i]
		if !inTimeRange(en.Time, from, to) {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(en.Content), query) &&
			!strings.Contains(strings.ToLower(en.Tool), query) {
			continue
		}
		matches = append(matches, en)
	}
	total := len(matches)
	// Page.
	page := matches
	if offset > 0 && offset < len(page) {
		page = page[offset:]
	} else if offset >= len(page) {
		page = nil
	}
	hasMore := false
	if limit > 0 && len(page) > limit {
		page = page[:limit]
		hasMore = true
	}
	rendered, emitted := renderHistory(page, maxChars)
	return agent.ToolOutcome{
		Content: rendered,
		Metadata: map[string]any{
			"total": total, "has_more": hasMore, "next_offset": offset + emitted,
			"entries": entriesToData(page),
		},
	}, nil
}

func (e *Extension) historyRange(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	fromD, toD := 0, 0
	if f, ok := numArg(args, "from"); ok {
		fromD = int(f)
	}
	if f, ok := numArg(args, "to"); ok {
		toD = int(f)
	}
	limit := 200
	if f, ok := numArg(args, "limit"); ok {
		limit = int(f)
	}
	maxChars := 100000
	if f, ok := numArg(args, "max_total_chars"); ok {
		maxChars = int(f)
	}
	entries := e.collectHistory(tenant, session)
	// entries are oldest-first with depth 0 at the tip; window [from, to).
	var window []historyEntry
	for _, en := range entries {
		if en.Depth >= fromD && (toD <= 0 || en.Depth < toD) {
			window = append(window, en)
		}
	}
	hasMore := false
	if limit > 0 && len(window) > limit {
		window = window[:limit]
		hasMore = true
	}
	rendered, _ := renderHistory(window, maxChars)
	return agent.ToolOutcome{
		Content: rendered,
		Metadata: map[string]any{
			"total": len(entries), "has_more": hasMore,
			"entries": entriesToData(window),
		},
	}, nil
}

func entriesToData(entries []historyEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, en := range entries {
		row := map[string]any{
			"id": en.ID, "role": en.Role, "depth": en.Depth, "content": en.Content,
		}
		if en.Tool != "" {
			row["tool"] = en.Tool
		}
		out = append(out, row)
	}
	return out
}

// renderHistory renders entries with a total-char budget; a single entry is
// NEVER truncated (the budget stops the list after emitting one in full).
func renderHistory(entries []historyEntry, maxChars int) (string, int) {
	var b strings.Builder
	used := 0
	emitted := 0
	for _, en := range entries {
		block := fmt.Sprintf("[%d] %s (%s)\n%s\n", en.Depth, en.role(), en.Time, en.Content)
		if maxChars > 0 && used+len(block) > maxChars && emitted > 0 {
			break
		}
		b.WriteString(block)
		used += len(block)
		emitted++
	}
	return strings.TrimSpace(b.String()), emitted
}

func (en historyEntry) role() string {
	if en.Tool != "" {
		return en.Role + ":" + en.Tool
	}
	return en.Role
}

func inTimeRange(t, from, to string) bool {
	if from == "" && to == "" {
		return true
	}
	pt, err := time.Parse("2006-01-02 15:04:05", strings.TrimSpace(t))
	if err != nil {
		pt, err = time.Parse(time.RFC3339, strings.TrimSpace(t))
		if err != nil {
			return true // unparseable timestamp: do not exclude
		}
	}
	if from != "" {
		if pf, e := time.Parse("2006-01-02 15:04:05", from); e == nil && pt.Before(pf) {
			return false
		}
	}
	if to != "" {
		if pt2, e := time.Parse("2006-01-02 15:04:05", to); e == nil && pt.After(pt2) {
			return false
		}
	}
	return true
}

var _ = sort.Strings
