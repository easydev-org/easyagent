package agent

import (
	"context"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// PresetProvider resolves a preset id to its row (the effective, user-override
// -wins view). Returns false when the id is unknown.
type PresetProvider interface {
	Preset(tenant, id string) (PresetRow, bool)
}

// PresetStore adapts *Presets to PresetProvider.
type PresetStore struct{ P *Presets }

// Preset implements PresetProvider.
func (p PresetStore) Preset(tenant, id string) (PresetRow, bool) { return p.P.Get(tenant, id) }

// ConfigProvider reads a tenant config value (e.g. "locale", "default_model").
type ConfigProvider interface {
	Config(tenant, key string) (string, bool)
}

// ExtConfigProvider additionally reads a knob in the scope of the extension that
// declares it.
//
// WHY this is separate from Config: `required_config` is declared by a TOOL, so
// the value that satisfies it belongs to that tool's extension. With the old flat
// key, `git` was satisfied by a `sandboxes` value that the user had actually set
// for the `sandbox` extension — the config appeared configured but the extension
// it was meant for had nothing.
type ExtConfigProvider interface {
	// ConfigExt returns the value of `name` as declared by extension `extID`,
	// using that knob's namespace ("" = extension level).
	ConfigExt(tenant, extID, name string) (string, bool)
}

// ConfigStore adapts *Config to ConfigProvider.
type ConfigStore struct{ C *Config }

// Config implements ConfigProvider.
func (c ConfigStore) Config(tenant, key string) (string, bool) { return c.C.Get(tenant, key) }

// ConfigExt implements ExtConfigProvider.
//
// WITHOUT this, ConfigStore satisfied only ConfigProvider, so
// FilterBlockedByConfig's type assertion failed and fell back to
// Config(tenant, name) — reading `cfg.<tenant>.<name>`. Every extension config is
// STORED at `cfg.<tenant>.<extID>.<name>`, so the read never matched the write:
// a knob could be saved, pushed to its extension, and still leave every dependent
// tool permanently filtered out of the model's toolbox. The console showed
// "required config" no matter how many times it was filled in.
//
// It forwards to the Config's namespaced read rather than duplicating the key
// construction, so the store and the reader cannot drift apart again.
func (c ConfigStore) ConfigExt(tenant, extID, name string) (string, bool) {
	return c.C.ConfigExt(tenant, extID, name)
}

// TurnPlan is the resolved configuration of one turn: the system prompt (preset
// prompt + <env>), the effective locale, the tool list, and the step budget.
type TurnPlan struct {
	System   string
	Locale   string
	Tools    []ToolDef
	MaxTurns int
}

// PlanInput is one turn's inputs.
type PlanInput struct {
	Tenant        string
	SessionPreset string
	SessionLocale string
	SessionSystem string
	SessionMax    int
	DeniedTools   []string
	// Grants summarises what this session may do, for the model's own benefit.
	//
	// Without it the model discovers its limits by trying and failing, which costs a turn
	// each time and reads to the user as the agent flailing. The summary is rendered by
	// the CALLER (which knows how to read the stored form); this layer only inserts it.
	Grants []GrantLine
	Now    time.Time
}

// GrantLine is one permission, rendered for the prompt.
//
// Text, not a structured value: the params are the extension's vocabulary ("repo:
// agentdev/*"), so the agent shows them verbatim rather than trying to explain them. A
// model that can read "repo: agentdev/*" needs nothing more from this layer.
type GrantLine struct {
	ExtID      string
	Capability string
	// Params is the human-readable form of the stored params, e.g. "repo: agentdev/*".
	Params string
}

// Plan resolves the effective turn configuration, mirroring the TS agent:
//
//   - the preset supplies the system prompt (localized) and the tool whitelist;
//     a session that REFERENCES a preset id that does not exist FAILS CLOSED
//     (err), rather than silently granting every tool;
//   - locale = session → tenant config → "en";
//   - the <env> block (with the language directive) is always appended;
//   - tools = registry filtered by whitelist + the hard denylist.
func Plan(ctx context.Context, deps RuntimeDeps, in PlanInput) (TurnPlan, error) {
	locale := ResolveLocale(in.SessionLocale, configOr(deps.Config, in.Tenant, "locale"), "en")
	system := in.SessionSystem
	var whitelist []string
	maxTurns := in.SessionMax

	if in.SessionPreset != "" {
		if deps.Presets == nil {
			return TurnPlan{}, errPresetUnavailable(in.SessionPreset)
		}
		row, ok := deps.Presets.Preset(in.Tenant, in.SessionPreset)
		if !ok {
			return TurnPlan{}, errPresetMissing(in.SessionPreset)
		}
		if system == "" {
			system = row.SystemPrompt
			if v := PickLocalized(row.SystemPromptI18N, locale); v != "" {
				system = v
			}
		}
		whitelist = row.Tools
		if maxTurns == 0 {
			maxTurns = row.MaxTurns
		}
	}
	if system == "" {
		system = "You are a helpful assistant."
	}
	// The grants ride in the env block so the model plans within them instead of
	// discovering its limits one refusal at a time.
	system = system + "\n\n" + BuildEnvBlockWithGrants(locale, in.Now, in.Grants)
	if maxTurns == 0 {
		maxTurns = firstNonZero(deps.MaxTurns, 8)
	}

	var tools []ToolDef
	if deps.Registry != nil {
		tools = FilterTools(deps.Registry.Tools(), whitelist, in.DeniedTools)
		tools = filterByExtensionAllowlist(tools, deps, in.Tenant)
		tools = FilterBlockedByConfig(tools, deps.Config, in.Tenant)
	}
	return TurnPlan{System: system, Locale: locale, Tools: tools, MaxTurns: maxTurns}, nil
}

// filterByExtensionAllowlist drops tools whose extension server is not allowed
// for the tenant's delegation subtree (P4). A scope with NO allowlist entry is
// unrestricted; a nil resolver is unrestricted. This is the per-subadmin
// "which extension servers may I use" gate.
func filterByExtensionAllowlist(tools []ToolDef, deps RuntimeDeps, tenant string) []ToolDef {
	if deps.ExtensionAllowlist == nil {
		return tools
	}
	allowed := deps.ExtensionAllowlist(tenant) // nil = unrestricted
	if allowed == nil {
		return tools
	}
	ok := make(map[string]bool, len(allowed))
	for _, e := range allowed {
		ok[e] = true
	}
	out := make([]ToolDef, 0, len(tools))
	for _, t := range tools {
		// A tool with no ExtID (a built-in) is always allowed.
		if t.ExtID == "" || ok[t.ExtID] {
			out = append(out, t)
		}
	}
	return out
}

// FilterBlockedByConfig drops tools whose required config is unset: the MODEL must
// never be offered a tool it cannot run.
//
// The console deliberately does NOT use this. ListTools returns the full set and
// lets the client grey out the blocked ones, because a tool that vanishes from the
// console cannot be discovered or configured — the user sees a gap and never learns
// which knob to set.
func FilterBlockedByConfig(tools []ToolDef, cfg ConfigProvider, tenant string) []ToolDef {
	if cfg == nil {
		return tools
	}
	out := make([]ToolDef, 0, len(tools))
	extCfg, _ := cfg.(ExtConfigProvider)
	for _, t := range tools {
		blocked := false
		for _, name := range t.RequiredConfig {
			// Resolve in the OWNING extension's scope when the provider can; fall
			// back to the tenant-wide read for a provider that has no notion of
			// extensions (tests, the memory store).
			var v string
			var ok bool
			if extCfg != nil {
				v, ok = extCfg.ConfigExt(tenant, t.ExtID, name)
			} else {
				v, ok = cfg.Config(tenant, name)
			}
			if !ok || v == "" {
				blocked = true
				break
			}
		}
		if !blocked {
			out = append(out, t)
		}
	}
	return out
}

// TurnPlanToLLMTools maps the planned tools to llm tool definitions.
func TurnPlanToLLMTools(tools []ToolDef, locale string) []llm.ToolDef {
	out := make([]llm.ToolDef, 0, len(tools))
	for _, t := range tools {
		desc := t.Description
		if v := PickLocalized(t.Descriptions, locale); v != "" {
			desc = v
		}
		schema := t.InputSchema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, llm.ToolDef{
			Name: LLMToolName(t), Description: desc, Parameters: schema,
		})
	}
	return out
}

func configOr(c ConfigProvider, tenant, key string) string {
	if c == nil {
		return ""
	}
	v, _ := c.Config(tenant, key)
	return v
}

// presetError is a typed error so callers can map it to a Connect code.
type presetError struct{ msg string }

func (e *presetError) Error() string { return e.msg }

func errPresetMissing(id string) error {
	return &presetError{"preset not found: \"" + id + "\" (this session references a preset that no longer exists)"}
}

func errPresetUnavailable(id string) error {
	return &presetError{"preset store unavailable (session preset \"" + id + "\")"}
}
