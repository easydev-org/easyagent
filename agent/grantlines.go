package agent

import (
	"encoding/json"
	"sort"
	"strings"
)

// grantLines renders a session's grants for the prompt.
//
// The params are shown VERBATIM as key: value pairs. That is deliberate: "repo:
// agentdev/*" is the extension's vocabulary, and paraphrasing it here would mean this
// package understanding what a repository is — the coupling the whole design avoids. A
// model reads the pair fine; a reader of the prompt sees exactly what was granted.
//
// Sorted and de-duplicated so the prompt is stable: an unstable prompt defeats caching and
// makes two runs of the same session look different for no reason.
func (r *Runtime) grantLines(tenant, session string) []GrantLine {
	if r.deps.DB == nil {
		return nil
	}
	rows, err := r.deps.DB.ListGrants(tenant, session)
	if err != nil || len(rows) == 0 {
		return nil
	}

	seen := make(map[string]bool, len(rows))
	out := make([]GrantLine, 0, len(rows))
	for _, row := range rows {
		params := formatGrantParams(row.Params)
		key := row.ExtID + "\x00" + row.Capability + "\x00" + params
		if seen[key] {
			// Two identical grants read as one line; repeating them would suggest the
			// session has more authority than it does.
			continue
		}
		seen[key] = true
		out = append(out, GrantLine{ExtID: row.ExtID, Capability: row.Capability, Params: params})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ExtID != out[j].ExtID {
			return out[i].ExtID < out[j].ExtID
		}
		if out[i].Capability != out[j].Capability {
			return out[i].Capability < out[j].Capability
		}
		return out[i].Params < out[j].Params
	})
	return out
}

// formatGrantParams renders the opaque params text as "k: v, k2: v2".
//
// Unparseable text is shown raw rather than dropped: a grant the prompt omits would make
// the model behave as if it had less authority than it does.
func formatGrantParams(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil || len(m) == 0 {
		return raw
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+": "+toText(m[k]))
	}
	return strings.Join(parts, ", ")
}

func toText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

/**
 * groupEnv says which group this session belongs to, and what that group is for.
 *
 * A SESSION BELONGS TO AT MOST ONE GROUP THAT IT COORDINATES, but it may be a plain member of several. The
 * coordinator's group is the one that matters for the prompt: that is where the messages addressed to this
 * session arrive, and it is the only group whose purpose shapes what it should do. A member's groups are a
 * lookup away (group-list) and change nothing about how it should answer.
 *
 * NIL, NOT AN EMPTY STRUCT, when the session coordinates nothing: the renderer then omits the section entirely,
 * which is the same rule the session's own intro follows.
 */
func (r *Runtime) groupEnv(tenant, session string) *GroupEnv {
	if r.deps.DB == nil {
		return nil
	}
	coordinated, err := r.deps.DB.CoordinatedGroup(tenant, session)
	if err != nil || coordinated == "" {
		return nil
	}
	g, ok, err := r.deps.DB.GetGroup(tenant, coordinated)
	if err != nil || !ok {
		return nil
	}
	return &GroupEnv{Name: g.Name, Intro: g.Intro, Coordinator: session}
}
