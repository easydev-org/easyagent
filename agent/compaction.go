package agent

import "strings"

// CompactionRole is the message role of a compaction checkpoint.
const CompactionRole = "compaction"

// CompactionPreview is the sentinel last-message preview for a session whose
// tip is a compaction checkpoint (clients substitute a localized label).
const CompactionPreview = "__compacted__"

// FoldEntry is one message as the compaction scan sees it.
type FoldEntry struct {
	ID         string
	Role       string
	Text       string // concatenated text parts
	ToolCalls  int
	ToolInput  int // estimated tokens of tool-call arguments
	ToolResult int // estimated tokens of tool results
}

// EntryCost is the entry's size as it occupies the model's context: text +
// tool-call args + tool results + a small per-call framing cost. Reasoning is
// intentionally NOT represented (it never enters the rebuilt context).
func EntryCost(e FoldEntry) int {
	return EstimateTokens(e.Text) + e.ToolInput + e.ToolResult + e.ToolCalls*4
}

// SplitResult is a split of the chain into a verbatim tail and a folded prefix.
type SplitResult struct {
	Tail   []FoldEntry // oldest-first, within tailTokens
	Folded []FoldEntry // older prefix, oldest-first, within foldTokens
}

// SplitScan splits oldest-first entries (which may include compaction
// checkpoints) into a verbatim tail (~tailTokens, aligned to a user prompt) and
// the older fold region (~foldTokens). Compaction checkpoints are skipped.
func SplitScan(entries []FoldEntry, tailTokens, foldTokens int) SplitResult {
	filtered := make([]FoldEntry, 0, len(entries))
	for _, e := range entries {
		if e.Role != CompactionRole {
			filtered = append(filtered, e)
		}
	}
	if len(filtered) == 0 {
		return SplitResult{}
	}
	// Walk from the NEWEST end, accumulating the tail until the budget is met,
	// aligned so the tail starts at a user prompt.
	start := len(filtered)
	used := 0
	for i := len(filtered) - 1; i >= 0; i-- {
		c := EntryCost(filtered[i])
		if used+c > tailTokens && start != len(filtered) {
			break
		}
		used += c
		start = i
	}
	// Align the tail start to a user message (the fold must not split a turn).
	for start > 0 && filtered[start].Role != "user" {
		start--
	}
	tail := filtered[start:]
	prefix := filtered[:start]

	// The fold region keeps the oldest entries within foldTokens.
	foldStart := len(prefix)
	used = 0
	for i := len(prefix) - 1; i >= 0; i-- {
		c := EntryCost(prefix[i])
		if used+c > foldTokens && foldStart != len(prefix) {
			break
		}
		used += c
		foldStart = i
	}
	return SplitResult{Tail: tail, Folded: prefix[foldStart:]}
}

// FoldQA renders the folded prefix as a deterministic Q&A transcript (no LLM):
// one block per user turn, with the assistant's non-thinking text and a note of
// how many tool calls it made. Tool results are summarised as short snippets.
func FoldQA(folded []FoldEntry, snippets map[string][]string) string {
	var b strings.Builder
	for _, e := range folded {
		switch e.Role {
		case "user", "event":
			if strings.TrimSpace(e.Text) == "" {
				continue
			}
			b.WriteString("User: ")
			b.WriteString(strings.TrimSpace(e.Text))
			b.WriteString("\n")
		case "assistant":
			if e.ToolCalls == 0 && strings.TrimSpace(e.Text) == "" {
				continue
			}
			b.WriteString("Assistant: ")
			if e.ToolCalls > 0 {
				b.WriteString("[" + itoa(e.ToolCalls) + " tool calls] ")
			}
			b.WriteString(strings.TrimSpace(e.Text))
			for _, s := range snippets[e.ID] {
				if s == "" {
					continue
				}
				b.WriteString("\n  tool: ")
				b.WriteString(truncate(s, 200))
			}
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
