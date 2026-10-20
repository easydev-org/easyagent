package agent

import "strings"

// ToolDef is a tool offered to the model: its own (sanitized) name, a
// description, and a JSON-schema for its arguments.
type ToolDef struct {
	Name        string
	ExtID       string
	Description string
	// Descriptions are locale → text (description is the fallback).
	Descriptions map[string]string
	// InputSchema is a JSON Schema object (may be nil → an empty object schema).
	InputSchema map[string]any
	// RequiredConfig names config knobs whose value must be set for the tool to
	// run; a tool with an unset required knob is HARD-DISABLED (not offered).
	RequiredConfig []string
	// Namespace groups this tool with siblings that do the same kind of job
	// (pvc, build, storage, ...). Organisational: the name is unchanged.
	Namespace string
}

// ToolRegistry supplies the tools a session may call. The production impl
// discovers them over the bus; tests inject a static list.
type ToolRegistry interface {
	Tools() []ToolDef
}

// StaticTools is a fixed tool list.
type StaticTools []ToolDef

// Tools returns the list.
func (s StaticTools) Tools() []ToolDef { return s }

// QualifiedName is the stable tool key offered to the model. A tool's own name
// is unique across extensions; when two extensions expose the SAME bare name,
// the extension-qualified `<extId>-<name>` form disambiguates.
func QualifiedName(t ToolDef) string {
	if t.ExtID == "" {
		return t.Name
	}
	return t.ExtID + "-" + t.Name
}

// SanitizeToolName maps a tool name onto the LLM gateway's accepted charset
// `[A-Za-z0-9_-]` (out-of-charset → '-'), mirroring the TS `sanitizeToolName`.
// The gateway rejects `.` in `tools.N.function.name` (e.g. the bundled
// `model.image` family), which would fail the whole turn.
func SanitizeToolName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// LLMToolName is the AI/LLM-visible tool name: the tool's OWN (sanitized) name,
// NOT `extId`-prefixed — extensions already namespace their tools by their first
// name segment, so two extensions never collide in practice. Mirrors the TS
// `toolQualifiedName`.
func LLMToolName(t ToolDef) string { return SanitizeToolName(t.Name) }

// WhitelistMatch reports whether a tool matches a preset whitelist entry, which
// may be either the bare (sanitized) name or the `<extId>.<name>` /
// `<extId>-<name>` qualified form.
func WhitelistMatch(t ToolDef, whitelist map[string]bool) bool {
	if whitelist[LLMToolName(t)] {
		return true
	}
	if whitelist[t.Name] {
		return true
	}
	if whitelist[QualifiedName(t)] {
		return true
	}
	if t.ExtID != "" {
		if whitelist[t.ExtID+"."+t.Name] {
			return true
		}
	}
	return false
}

// FilterTools applies a preset whitelist (nil/empty = no restriction = all
// tools) and a hard denylist of qualified `<extId>-<name>` ids.
func FilterTools(all []ToolDef, whitelist []string, denied []string) []ToolDef {
	var wl map[string]bool
	if len(whitelist) > 0 {
		wl = map[string]bool{}
		for _, w := range whitelist {
			wl[w] = true
		}
	}
	deny := map[string]bool{}
	for _, d := range denied {
		deny[d] = true
	}
	out := make([]ToolDef, 0, len(all))
	for _, t := range all {
		if deny[QualifiedName(t)] {
			continue
		}
		if wl != nil && !WhitelistMatch(t, wl) {
			continue
		}
		out = append(out, t)
	}
	return out
}
