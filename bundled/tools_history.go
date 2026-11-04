package bundled

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * THE HISTORY TOOLS: ONE WALKER, THREE ENTRY POINTS.
 *
 * `history-search` finds messages by keyword, `history-range-read` reads a positional window, and
 * `history-read` returns one message in full by id. All three read the same chain through
 * `collectHistory`, so a change to how a message is rendered cannot make one tool disagree with
 * another — which is how `history-search` came to ignore tool output entirely while
 * `history-range-read` was happy to claim it read the transcript.
 */

// The kinds of content a history query can include.
const (
	// includeText is the message's own prose: user prompts, assistant replies, summaries.
	includeText = "text"
	// includeTool matches tool NAMES and their call arguments.
	includeTool = "tool"
	// includeResult matches tool OUTPUT — the part that used to be discarded.
	includeResult = "result"
)

// includeSet is which content kinds a request asked for.
//
// IT IS USED TWICE, FOR TWO DIFFERENT QUESTIONS, and keeping one type is what stops them drifting:
//   - `history-search` asks "what should the KEYWORD match against?";
//   - `history-range-read` asks "what should be RENDERED?".
//
// The second was overlooked: the parameter was parsed and then never consulted, so a caller who
// asked for text-only still got every tool's output in the page — with `preview_chars` hiding it
// only when the log happened to be long enough to be cut.
type includeSet map[string]bool

// has reports whether a kind was requested. An EMPTY set means the default, not "nothing" — a
// caller that names no kinds gets exactly what a caller that names the defaults gets.
func (i includeSet) has(kind string) bool {
	if len(i) == 0 {
		for _, k := range defaultIncludes {
			if k == kind {
				return true
			}
		}
		return false
	}
	return i[kind]
}

func allIncludes() includeSet {
	return includeSet{includeText: true, includeTool: true, includeResult: true}
}

// toSet converts a parsed list into a lookup.
func toSet(list []string) includeSet {
	if len(list) == 0 {
		return nil // the default applies at read time
	}
	out := make(includeSet, len(list))
	for _, k := range list {
		out[k] = true
	}
	return out
}

// defaultIncludes is what a query searches when the caller names nothing.
//
// TEXT ONLY, DELIBERATELY. A single `sandbox-job-start` result can be tens of thousands of
// characters, so including results by default would let one long build drown every conversational
// hit — and a model searching its own history is usually looking for what was SAID, not for a log it
// already has in context. Results are one flag away.
var defaultIncludes = []string{includeText}

// defaultPreviewChars bounds one entry's rendered content.
//
// THE BUDGET IS PER ENTRY, NOT PER PAGE, and that is the difference that makes paging usable: a page
// of fifty entries with one huge result inside it would otherwise be dominated by that entry, and
// `max_total_chars` would cut the page off after it. Truncation is announced (`truncated`,
// `full_chars`), so the model knows there is more and can ask `history-read` for it.
const defaultPreviewChars = 2000

// historyEntry is one message as the history tools surface it.
type historyEntry struct {
	ID   string
	Role string
	// Seq is the message's position in its chain: oldest is 1, fixed when written. It is the only
	// positional handle a caller gets, because the other one (depth) was not stable across appends.
	Seq  int64
	Time string
	// Text is the message's own prose (text + summary parts).
	Text string
	// Tools is every tool call in this message, in order.
	Tools []historyTool
	// Matched records WHICH include kinds produced a hit, so a caller can tell why an entry is
	// in a result set. Empty for a positional read, where nothing was matched.
	Matched []string
}

// historyTool is one tool call and, when it has one, its result.
type historyTool struct {
	ID   string
	Name string
	// Input is the call arguments, JSON-encoded (already a string on the wire).
	Input string
	// Content is the tool's output. Empty when the call produced none.
	Content string
	// State is how it ended: ok | error | timeout | cancelled.
	State string
}

// collectHistory walks a chain and returns its entries OLDEST FIRST.
//
// A CONVERSATION OTHER THAN THIS ONE IS READ THROUGH THE SAME PATH: the caller supplies the session
// name and the chain is walked from ITS tip. There is no separate "read someone else's history"
// mechanism, and that is on purpose — a second reader would be a second answer to what a message is.
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
	byMsg := map[string][]MessagePartRow{}
	for _, p := range e.deps.MessageParts(tenant, ids) {
		byMsg[p.MessageID] = append(byMsg[p.MessageID], p)
	}

	out := make([]historyEntry, 0, len(chain))
	for _, m := range chain {
		en := historyEntry{ID: m.ID, Role: m.Role, Seq: m.Seq, Time: m.CreatedAt}
		// Tools are keyed by their call id so a result attaches to the call it belongs to. A
		// result whose call is in a DIFFERENT message still attaches, because the id is the
		// authority and a provider may split them.
		byCall := map[string]int{}
		for _, p := range byMsg[m.ID] {
			switch p.Type {
			case "text":
				var d struct {
					Text string `json:"text"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					en.Text += d.Text
				}
			case "summary":
				var d struct {
					Summary string `json:"summary"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					en.Text += d.Summary
				}
			case "tool":
				var d struct {
					ID    string `json:"id"`
					Name  string `json:"name"`
					Input any    `json:"input"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					raw, _ := json.Marshal(d.Input)
					byCall[d.ID] = len(en.Tools)
					en.Tools = append(en.Tools, historyTool{ID: d.ID, Name: d.Name, Input: string(raw)})
				}
			case "tool_result":
				var d struct {
					ToolUseID string `json:"tool_use_id"`
					Content   string `json:"content"`
					State     string `json:"state"`
				}
				if json.Unmarshal([]byte(p.Data), &d) == nil {
					if i, ok := byCall[d.ToolUseID]; ok {
						en.Tools[i].Content = d.Content
						en.Tools[i].State = d.State
					} else {
						// A result with no call beside it: keep it rather than drop the output,
						// because the text is the thing a search is looking for.
						en.Tools = append(en.Tools, historyTool{
							ID: d.ToolUseID, Content: d.Content, State: d.State,
						})
					}
				}
			}
		}
		out = append(out, en)
	}
	return out
}

/**
 * historyFilter is what every entry point narrows with.
 *
 * THE ENTRY POINTS DIFFER ONLY IN HOW THEY FILL THIS. `history-search` sets a query and optional
 * time range; `history-range-read` sets a seq window. Both then run the same match, which is what
 * keeps "does this message match?" from having two implementations.
 */
type historyFilter struct {
	// Include names the kinds of content a query is matched against.
	Include []string
	// Query is the case-insensitive keyword. Empty matches everything.
	Query string
	// Session is empty for this conversation, or the name of another one in the same tenant.
	Session string
	// TimeFrom / TimeTo are inclusive bounds, "" for none.
	TimeFrom string
	TimeTo   string
	// SeqFrom / SeqTo bound the window by POSITION IN THE CHAIN (oldest = 1). SeqTo 0 means no upper
	// bound. Position, not "how far back": a window measured from the tip silently moves as messages
	// arrive, so one a caller saved last turn would address different messages this turn.
	SeqFrom int64
	SeqTo   int64
	// LastN selects the newest N messages, which is the one question a "how far back" window answered
	// well. Applied after the seq window so the two compose rather than one silently winning.
	LastN int
}

// includes reports whether a kind is searched, with the default applied.
func (f historyFilter) includes(kind string) bool {
	if len(f.Include) == 0 {
		for _, k := range defaultIncludes {
			if k == kind {
				return true
			}
		}
		return false
	}
	for _, k := range f.Include {
		if k == kind {
			return true
		}
	}
	return false
}

// match reports whether one entry satisfies the filter, and records which kinds hit.
func (f historyFilter) match(en *historyEntry) bool {
	if f.SeqFrom > 0 && en.Seq < f.SeqFrom {
		return false
	}
	if f.SeqTo > 0 && en.Seq >= f.SeqTo {
		return false
	}
	if !inTimeRange(en.Time, f.TimeFrom, f.TimeTo) {
		return false
	}
	if f.Query == "" {
		// A positional read matches on position alone; nothing was "matched" by content.
		return true
	}
	q := strings.ToLower(f.Query)
	en.Matched = nil
	if f.includes(includeText) && strings.Contains(strings.ToLower(en.Text), q) {
		en.Matched = append(en.Matched, includeText)
	}
	if f.includes(includeTool) {
		for _, t := range en.Tools {
			if strings.Contains(strings.ToLower(t.Name), q) || strings.Contains(strings.ToLower(t.Input), q) {
				en.Matched = append(en.Matched, includeTool)
				break
			}
		}
	}
	if f.includes(includeResult) {
		for _, t := range en.Tools {
			if strings.Contains(strings.ToLower(t.Content), q) {
				en.Matched = append(en.Matched, includeResult)
				break
			}
		}
	}
	return len(en.Matched) > 0
}

// applyHistoryFilter runs the filter over entries and returns matches OLDEST FIRST.
//
// NEWEST-FIRST ORDERING IS THE CALLER'S JOB, because a positional read wants the chain's own order
// and a keyword search wants recency. Doing it here would make one of the two wrong.
func applyHistoryFilter(entries []historyEntry, f historyFilter) []historyEntry {
	var out []historyEntry
	for i := range entries {
		en := entries[i] // a copy: matching records into Matched
		if f.match(&en) {
			out = append(out, en)
		}
	}
	return out
}

// renderHistoryWithPreviews renders entries under a total budget, truncating each ENTRY to
// previewChars and announcing it.
//
// A SINGLE ENTRY IS NEVER CUT BY THE TOTAL BUDGET (it is emitted whole, then the list stops), which
// is the behaviour the old tools documented and callers came to rely on. What changed is that the
// PER-ENTRY limit is now separate and always announced: a page of fifty entries where one is a
// 40k-character build log used to be one unusable page.
func renderHistoryWithPreviews(entries []historyEntry, maxChars, previewChars int, inc includeSet) (string, int) {
	var b strings.Builder
	used := 0
	emitted := 0
	for _, en := range entries {
		body, cut := entryBody(en, previewChars, inc)
		/**
		 * THE RENDERED LINE CARRIES THE ID.
		 *
		 * ONE HANDLE, AND IT IS THE SEQ. Both tools address by it: `history-range-read` windows on
		 * it and `history-read` looks a message up by it.
		 *
		 * THE MESSAGE ID IS NOT SHOWN. It is a 36-character UUID, it appears nowhere else in the
		 * model's context, and there are two of them for every pair of turns — pure noise in a
		 * window a reader is scanning. `(session, seq)` is the address a caller can hold; the id
		 * stays inside the tool as the store's key.
		 */
		block := fmt.Sprintf("[seq %d] %s (%s)\n%s\n", en.Seq, en.role(inc), en.Time, body)
		if cut {
			// The marker names the id and the command that returns the rest, so the model does not
			// have to guess either.
			block += fmt.Sprintf("[truncated: %d of %d chars — history-read(seq=%d) for the rest]\n",
				len(body), entryChars(en), en.Seq)
		}
		if maxChars > 0 && used+len(block) > maxChars && emitted > 0 {
			break
		}
		b.WriteString(block)
		used += len(block)
		emitted++
	}
	return strings.TrimSpace(b.String()), emitted
}

// entryChars is the entry's content length before truncation.
func entryChars(en historyEntry) int {
	n := len(en.Text)
	for _, t := range en.Tools {
		n += len(t.Name) + len(t.Input) + len(t.Content)
	}
	return n
}

// entryBody renders an entry's content, truncated to previewChars (0 = no limit).
func entryBody(en historyEntry, previewChars int, inc includeSet) (string, bool) {
	var parts []string
	if inc.has(includeText) && en.Text != "" {
		parts = append(parts, en.Text)
	}
	if inc.has(includeTool) || inc.has(includeResult) {
		for _, t := range en.Tools {
			label := t.Name
			if label == "" {
				label = "tool"
			}
			head := ""
			if inc.has(includeTool) {
				head = fmt.Sprintf("<%s %s>", label, t.Input)
			}
			if inc.has(includeResult) && t.Content != "" {
				state := t.State
				if state == "" {
					state = "ok"
				}
				if head != "" {
					head += "\n"
				}
				head += fmt.Sprintf("<%s result state=%s>\n%s", label, state, t.Content)
			}
			if head != "" {
				parts = append(parts, head)
			}
		}
	}
	body := strings.Join(parts, "\n")
	if previewChars <= 0 || len(body) <= previewChars {
		return body, false
	}
	// Cut on a CODE POINT so a multi-byte character is never split.
	end := 0
	kept := 0
	for _, r := range body {
		if kept == previewChars {
			break
		}
		end += len(string(r))
		kept++
	}
	return body[:end], true
}

/**
 * role labels the entry with the tool names it ran — BUT ONLY WHEN TOOLS WERE ASKED FOR.
 *
 * The label is part of the page's content, so a caller that asked for text only must not get a line
 * reading `assistant:sandbox-job-start` above its prose: that names a tool the caller excluded, and
 * it is the one thing about the entry they said they did not want.
 */
func (en historyEntry) role(inc includeSet) string {
	if !inc.has(includeTool) {
		return en.Role
	}
	names := make([]string, 0, len(en.Tools))
	for _, t := range en.Tools {
		if t.Name != "" {
			names = append(names, t.Name)
		}
	}
	if len(names) > 0 {
		return en.Role + ":" + strings.Join(names, ",")
	}
	return en.Role
}

// entriesToData renders entries for a tool's structured output, honouring the same preview limit.
//
// THE PREVIEW AND THE RENDERED TEXT MUST AGREE. When they did not, a caller reading `data` saw the
// full content while the model saw a truncated one, and neither knew the other existed.
func entriesToData(entries []historyEntry, previewChars int, inc includeSet) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for _, en := range entries {
		body, cut := entryBody(en, previewChars, inc)
		// NO id EITHER. The structured output feeds the same reader as the text, and an id here
		// would be a second address that the tools do not accept — see the note in the renderer.
		//
		// `created_at` IS EMITTED because the console groups a history page by when things were said,
		// and `tool_name` because it labels the row with what ran. Both were missing: the card read
		// fields nobody sent and rendered every entry with an empty time and no tool.
		row := map[string]any{
			"role": en.Role, "seq": en.Seq, "content": body, "created_at": en.Time,
		}
		if cut {
			row["truncated"] = true
			row["full_chars"] = entryChars(en)
		}
		if len(en.Matched) > 0 {
			row["matched"] = en.Matched
		}
		if len(en.Tools) > 0 && inc.has(includeTool) {
			names := make([]string, 0, len(en.Tools))
			for _, t := range en.Tools {
				if t.Name != "" {
					names = append(names, t.Name)
				}
			}
			if len(names) > 0 {
				row["tools"] = names
				// The FIRST tool's name, singular, because a card shows one label per row. The full
				// list stays under `tools` for anything that wants all of them.
				row["tool_name"] = names[0]
			}
		}
		out = append(out, row)
	}
	return out
}

// historyTarget resolves which conversation a query reads.
//
// ANOTHER SESSION IS READ ONLY WHEN NAMED, and the name is checked before the walk: a typo would
// otherwise return "no results", which reads as "there is nothing there" rather than "that
// conversation does not exist".
func historyTarget(args map[string]any, session string) (string, error) {
	named := strings.TrimSpace(strArg(args, "session"))
	if named == "" || named == session {
		return session, nil
	}
	return named, nil
}

// parseIncludes reads the `include` list, validating every name.
//
// AN UNKNOWN NAME IS AN ERROR, not a silent no-op: a caller that writes "results" instead of
// "result" would otherwise get text-only results and believe the search covered tool output.
func parseIncludes(args map[string]any) ([]string, error) {
	raw, ok := args["include"]
	if !ok || raw == nil {
		return nil, nil // the default applies at match time
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, &toolError{msg: "history: \"include\" must be an array of text|tool|result"}
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		s, _ := v.(string)
		switch s {
		case includeText, includeTool, includeResult:
			out = append(out, s)
		default:
			return nil, &toolError{msg: fmt.Sprintf(
				"history: unknown include %q (want text, tool or result)", s)}
		}
	}
	return out, nil
}

// historySearch finds messages by keyword, newest first, optionally in another conversation.
func (e *Extension) historySearch(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	query := strings.TrimSpace(strArg(args, "query"))
	if query == "" {
		return agent.ToolOutcome{}, &toolError{msg: "history-search: missing \"query\""}
	}
	include, err := parseIncludes(args)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	target, err := historyTarget(args, session)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if target != session && !e.deps.SessionExists(tenant, target) {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"history-search: no session named %q in this tenant", target)}
	}

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
	preview := defaultPreviewChars
	if f, ok := numArg(args, "preview_chars"); ok {
		preview = int(f)
	}

	filter := historyFilter{
		Include: include, Query: query, Session: target,
		TimeFrom: strArg(args, "from"), TimeTo: strArg(args, "to"),
	}
	matches := applyHistoryFilter(e.collectHistory(tenant, target), filter)
	// Newest first: a search is asking "when did we talk about this", so recency is the useful order.
	for i, j := 0, len(matches)-1; i < j; i, j = i+1, j-1 {
		matches[i], matches[j] = matches[j], matches[i]
	}

	total := len(matches)
	page := matches
	if offset > 0 {
		if offset < len(page) {
			page = page[offset:]
		} else {
			page = nil
		}
	}
	hasMore := false
	if limit > 0 && len(page) > limit {
		page = page[:limit]
		hasMore = true
	}
	// THE SAME KINDS GOVERN WHAT IS RENDERED, not only what was matched: a search that asked for
	// text-only must not answer with a page full of tool logs.
	inc := toSet(include)
	rendered, emitted := renderHistoryWithPreviews(page, maxChars, preview, inc)
	return agent.ToolOutcome{
		Content: rendered,
		Metadata: map[string]any{
			"session": target, "total": total, "has_more": hasMore, "next_offset": offset + emitted,
			"entries": entriesToData(page, preview, inc),
		},
	}, nil
}

/**
 * historyRangeRead reads a window BY POSITION IN THE CHAIN, oldest first.
 *
 * THE ANCHOR IS `seq`: the message's position, oldest = 1, fixed when it is written. It survives new
 * messages, forks (a child continues from its parent's tip) and undos (the abandoned rows keep their
 * numbers and simply stop being reachable).
 *
 * THERE IS NO DEPTH PARAMETER, AND NO DEPTH ANYWHERE. It counted back from the tip, so it was
 * recomputed from the chain's LENGTH and shifted for every message each time one was appended — a
 * number that looked like an address and was not one.
 *
 * `last` REMAINS, because "the most recent N" is a real question and a seq window cannot ask it
 * without knowing the count first. The two compose: seq narrows to a stretch, last trims it.
 */
func (e *Extension) historyRangeRead(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	include, err := parseIncludes(args)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	target, err := historyTarget(args, session)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if target != session && !e.deps.SessionExists(tenant, target) {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"history-range-read: no session named %q in this tenant", target)}
	}

	var seqFrom, seqTo int64
	if f, ok := numArg(args, "from_seq"); ok {
		seqFrom = int64(f)
	}
	if f, ok := numArg(args, "to_seq"); ok {
		seqTo = int64(f)
	}
	last := 0
	if f, ok := numArg(args, "last"); ok {
		last = int(f)
	}
	limit := 200
	if f, ok := numArg(args, "limit"); ok {
		limit = int(f)
	}
	maxChars := 100000
	if f, ok := numArg(args, "max_total_chars"); ok {
		maxChars = int(f)
	}
	preview := defaultPreviewChars
	if f, ok := numArg(args, "preview_chars"); ok {
		preview = int(f)
	}

	all := e.collectHistory(tenant, target)
	filter := historyFilter{
		Include: include, Session: target, SeqFrom: seqFrom, SeqTo: seqTo,
	}
	window := applyHistoryFilter(all, filter)

	// `last` trims to the NEWEST N of the window, then the window is read oldest-first like any
	// other positional read.
	hasMore := false
	if last > 0 && len(window) > last {
		window = window[len(window)-last:]
		hasMore = true
	}
	if limit > 0 && len(window) > limit {
		window = window[:limit]
		hasMore = true
	}
	// `include` was parsed and then ignored here, so a caller who asked for text-only still got
	// every tool's output — hidden only when it happened to be long enough for `preview_chars` to
	// cut it. It now governs the render.
	inc := toSet(include)
	rendered, _ := renderHistoryWithPreviews(window, maxChars, preview, inc)

	// THE BOUNDS ARE REPORTED so a caller can page without re-deriving them: an empty window still
	// says what the conversation's range is, which distinguishes "you asked past the end" from
	// "this conversation has no messages".
	meta := map[string]any{
		"session": target, "has_more": hasMore, "entries": entriesToData(window, preview, inc),
	}
	if len(all) > 0 {
		meta["seq_min"] = all[0].Seq
		meta["seq_max"] = all[len(all)-1].Seq
	}
	return agent.ToolOutcome{Content: rendered, Metadata: meta}, nil
}

/**
 * historyRead returns ONE message in full by id.
 *
 * THIS IS THE OTHER HALF OF TRUNCATION. `history-search` cuts each entry to a preview so a page
 * stays readable; a model that sees `truncated: true` needs a way to get the rest, and the
 * alternative — asking for a bigger `preview_chars` and re-running the search — re-scans the whole
 * chain to return one message.
 *
 * `part` selects what to return when a message has more than one: "text", "tool" or "result".
 * Omitting it returns everything, which is what a caller who does not yet know what is in the
 * message needs.
 */
func (e *Extension) historyRead(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	seqArg, ok := numArg(args, "seq")
	if !ok {
		return agent.ToolOutcome{}, &toolError{msg: "history-read: missing \"seq\""}
	}
	want := int64(seqArg)
	if want <= 0 {
		return agent.ToolOutcome{}, &toolError{msg: "history-read: \"seq\" must be positive (oldest message is 1)"}
	}
	target, err := historyTarget(args, session)
	if err != nil {
		return agent.ToolOutcome{}, err
	}
	if target != session && !e.deps.SessionExists(tenant, target) {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"history-read: no session named %q in this tenant", target)}
	}
	part := strings.ToLower(strings.TrimSpace(strArg(args, "part")))
	switch part {
	case "", includeText, includeTool, includeResult:
	default:
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"history-read: unknown part %q (want text, tool or result)", part)}
	}

	/**
	 * LOOKED UP BY POSITION, NOT BY ID.
	 *
	 * `(session, seq)` is UNIQUE WITHIN ONE SESSION, which is the only thing a caller can name —
	 * and that is exactly what this tool needs, because the caller just read that seq out of a
	 * listing. Two forked sessions can each have a seq 3 (they share a chain until they diverge),
	 * but they are different conversations, so the session name disambiguates them.
	 *
	 * Out of range is an ERROR rather than an empty answer: "no message" would read as "the
	 * conversation is empty", and the caller needs to know that its seq was wrong.
	 */
	var found *historyEntry
	for _, en := range e.collectHistory(tenant, target) {
		if en.Seq == want {
			e := en
			found = &e
			break
		}
	}
	if found == nil {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"history-read: session %q has no message at seq %d", target, want)}
	}

	// UNTRUNCATED, ALWAYS: this tool exists to return what the search withheld.
	var body string
	switch part {
	case includeText:
		body = found.Text
	case includeTool:
		body = renderTools(found.Tools, false)
	case includeResult:
		body = renderTools(found.Tools, true)
	default:
		body = entryBodyAll(*found)
	}
	return agent.ToolOutcome{
		Content: body,
		Metadata: map[string]any{
			"session": target, "seq": found.Seq, "role": found.Role, "chars": len(body),
		},
	}, nil
}

// entryBodyAll renders an entry with no limit.
func entryBodyAll(en historyEntry) string {
	body, _ := entryBody(en, 0, allIncludes())
	return body
}

// renderTools renders tool calls, and with `resultsOnly` just their output.
func renderTools(tools []historyTool, resultsOnly bool) string {
	var parts []string
	for _, t := range tools {
		label := t.Name
		if label == "" {
			label = "tool"
		}
		switch {
		case resultsOnly && t.Content != "":
			parts = append(parts, fmt.Sprintf("<%s result state=%s>\n%s", label, stateOr(t.State), t.Content))
		case resultsOnly:
			// Asked for results and this call produced none: say so rather than omitting it, or a
			// caller cannot tell "no output" from "not found".
			parts = append(parts, fmt.Sprintf("<%s result: none recorded>", label))
		default:
			head := fmt.Sprintf("<%s %s>", label, t.Input)
			if t.Content != "" {
				head += fmt.Sprintf("\n<%s result state=%s>\n%s", label, stateOr(t.State), t.Content)
			}
			parts = append(parts, head)
		}
	}
	return strings.Join(parts, "\n")
}

func stateOr(s string) string {
	if s == "" {
		return "ok"
	}
	return s
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
