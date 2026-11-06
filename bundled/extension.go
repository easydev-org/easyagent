package bundled

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/store"
)

// Handler runs one tool. args is the parsed argument object.
type Handler func(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error)

// Extension is the in-process bundled extension: it declares the manifest tools
// and dispatches them to Go handlers.
type Extension struct {
	deps     Deps
	manifest Manifest
	handlers map[string]Handler
}

// New builds the bundled extension, wiring every tool handler to [deps].
func New(deps Deps) (*Extension, error) {
	m, err := ParseManifest()
	if err != nil {
		return nil, err
	}
	e := &Extension{deps: deps, manifest: m, handlers: map[string]Handler{}}
	e.register()
	return e, nil
}

// Manifest exposes the parsed manifest (tools + config).
func (e *Extension) Manifest() Manifest { return e.manifest }

// Tools returns the tool definitions offered to the model (implements
// agent.ToolRegistry).
func (e *Extension) Tools() []agent.ToolDef { return e.manifest.Tools }

// Lookup returns a tool definition by bare name.
func (e *Extension) Lookup(name string) (agent.ToolDef, bool) {
	for _, t := range e.manifest.Tools {
		if t.Name == name {
			return t, true
		}
	}
	return agent.ToolDef{}, false
}

// ChannelDecl is one channel an extension publishes to.
//
// Topics are documentation: the agent forwards what the extension publishes and never
// parses a topic pattern. They exist so a client can offer "follow this repository's tags"
// instead of asking a user to invent a pattern.
type ChannelDecl struct {
	Name         string
	Description  string
	Descriptions map[string]string
	Topics       []ChannelTopicDecl
}

// ChannelTopicDecl documents one kind of message within a channel.
type ChannelTopicDecl struct {
	Pattern      string
	Description  string
	Descriptions map[string]string
}

// PermissionDecl is one permission an extension can require of a caller.
//
// A permission is a (Key, params) pair: "merge in this repository" and "merge in that one"
// are different authorisations, so a session may hold several grants of the same Key. A
// client renders the grant form from ParamsFields and never interprets them, which is what
// lets one panel serve every extension.
type PermissionDecl struct {
	Key          string
	Label        string
	Descriptions map[string]string
	// Risk is read | write | destructive. Presentation only.
	Risk string
	// ParamsFields types one grant's params, with the SAME shape as a config knob.
	ParamsFields []ConfigField
}

// ConfigField is a declared config knob attached to a tool (TS `configFields`).
type ConfigField struct {
	Name string
	Type string
	// Format narrows a string knob: uri | host | path | email | duration | semver.
	// The value validator uses it when present and falls back to the knob-name
	// convention only when an extension does not declare one.
	Format      string
	Description string
	Scope       string
	Kind        string
	Capability  string
	Default     string
	// EnumValues are the allowed choices when Type is "enum". A client renders a picker from them;
	// without them the knob is a text box and the only way to set it is to type a value from memory.
	EnumValues []string
	// Fields types a STRUCTURED knob (Type "list" or "object"): the keys of the
	// object / of each list item. Empty means "untyped" — the knob is a `json`
	// blob and the console shows a JSON textarea, which is the backward-compatible
	// behaviour for every extension that predates this.
	Fields []ConfigFieldSpec
	// ItemType is the element type when Type == "list".
	ItemType string
	MinItems int
	MaxItems int
	// Namespace scopes this knob's STORED VALUE to one namespace of the owning
	// extension. Empty = extension-level (shared by all its namespaces).
	Namespace string
}

// Namespace is one declared grouping of an extension's tools and config.
//
// A single extension commonly covers several unrelated jobs (an ops extension
// that manages PVCs, builds images and runs jobs). The namespace is the unit a
// client groups by, and for CONFIG it is also part of the storage address.
type Namespace struct {
	Name         string
	Description  string
	Descriptions map[string]string
}

// ConfigFieldSpec is one key of a structured config value.
type ConfigFieldSpec struct {
	Name string
	Type string
	// Kind is "value" (default) or "secret".
	Kind string
	// Format narrows a string: uri | host | path | email | duration | semver.
	Format     string
	EnumValues []string
	// Default is the field's default rendered as text (a form hint; it is not the
	// knob's stored value).
	Default      string
	Required     bool
	Description  string
	Descriptions map[string]string
	Fields       []ConfigFieldSpec
}

// Config knob kinds. `Kind` is an open string on the wire; these are the
// values the agent/server treat specially.
//
//   - ConfigKindValue  — an ordinary knob (default).
//   - ConfigKindModel  — a `provider_id/model_id` reference (a picker).
//   - ConfigKindSecret — a credential: the server NEVER returns its value to a
//     client (see server.GetExtensionConfig), only whether it is set.
const (
	ConfigKindValue  = "value"
	ConfigKindModel  = "model"
	ConfigKindSecret = "secret"
)

// ConfigFieldsFor returns the config knobs a tool requires (in manifest order),
// resolved from the manifest's config declarations.
func (e *Extension) ConfigFieldsFor(toolName string) []ConfigField {
	t, ok := e.Lookup(toolName)
	if !ok {
		return nil
	}
	byName := map[string]ConfigKnob{}
	for _, c := range e.manifest.Config {
		byName[c.Name] = c
	}
	out := make([]ConfigField, 0, len(t.RequiredConfig))
	for _, knob := range t.RequiredConfig {
		c, ok := byName[knob]
		if !ok {
			continue
		}
		typ := c.Kind
		if c.Type != "" {
			typ = c.Type
		} else {
			typ = "string"
		}
		kind := c.Kind
		if kind == "" {
			kind = "value"
		}
		out = append(out, ConfigField{
			Name: knob, Type: typ, Description: c.Description,
			Scope: scopeOrGlobal(c), Kind: kind, Capability: c.Capability, Default: c.Default,
		})
	}
	return out
}

func scopeOrGlobal(c ConfigKnob) string {
	// The manifest declares scope: global for every bundled knob; default to it.
	return "global"
}

// ConfigFields returns the extension's WHOLE declared config surface (every
// knob in the manifest), independent of which tools require them. The
// extension-config page edits these; a tool page only shows the subset it
// requires.
func (e *Extension) ConfigFields() []ConfigField {
	out := make([]ConfigField, 0, len(e.manifest.Config))
	for _, c := range e.manifest.Config {
		typ := c.Type
		if typ == "" {
			typ = "string"
		}
		kind := c.Kind
		if kind == "" {
			kind = "value"
		}
		out = append(out, ConfigField{
			Name: c.Name, Type: typ, Description: c.Description,
			Scope: scopeOrGlobal(c), Kind: kind, Capability: c.Capability, Default: c.Default,
			EnumValues: c.EnumValues,
		})
	}
	return out
}

// Execute runs one tool (implements agent.ToolExecutor). It accepts both the
// bare name and the extension-qualified `<id>-<name>` form.
func (e *Extension) Execute(ctx context.Context, tenant, session, _ /*callID*/, name, args string) (agent.ToolOutcome, error) {
	// callID is unused here: bundled tools run in-process and never report
	// progress over the bus. The parameter exists so one interface serves both
	// kinds of executor.
	// Guard: only tools declared in the manifest are callable.
	if _, ok := e.Lookup(name); !ok {
		if bare, ok2 := e.qualToBare(name); ok2 {
			name = bare
		}
	}
	h, ok := e.handlers[name]
	if !ok {
		return agent.ToolOutcome{}, fmt.Errorf("%w: bundled: %q", agent.ErrUnknownTool, name)
	}
	var parsed map[string]any
	if strings.TrimSpace(args) != "" {
		if err := json.Unmarshal([]byte(args), &parsed); err != nil {
			return agent.ToolOutcome{}, fmt.Errorf("bundled: %s: bad arguments: %w", name, err)
		}
	}
	/**
	 * THE PERMISSION GATE, HERE BECAUSE THIS IS THE BOUNDARY.
	 *
	 * A discovered extension is refused by the agent before the call crosses the bus, and then checks again against
	 * the target only it understands. Bundled runs in this process and the composite calls it DIRECTLY — the agent's
	 * gate is not on its path — so if the check is not here, `requires` in the manifest is a claim nobody verifies.
	 *
	 * IT WAS EXACTLY THAT: `spawn` declared `group.spawn`, the console offered the grant, and every session could
	 * spawn anyway. The declaration looked authoritative, which is worse than not having one.
	 */
	if def, ok := e.Lookup(name); ok {
		if missing, scope, denied := e.checkPermissions(tenant, session, def, parsed); denied {
			return agent.ToolOutcome{}, &PermissionDeniedError{
				Tool: name, Capability: missing, Scope: scope,
			}
		}
	}
	return h(ctx, tenant, session, parsed)
}

/**
 * PermissionDeniedError is a refusal to run a bundled tool for lack of a grant.
 *
 * IT NAMES THE CAPABILITY, NOT THE TOOL'S TARGET, for the same reason the agent's equivalent does: at this point the
 * arguments have not been interpreted, and a message naming the wrong group would send the reader looking at the
 * wrong permission panel.
 */
type PermissionDeniedError struct {
	Tool       string
	Capability string
	// Scope names the object the permission was checked for, when the tool's requirement is scoped. Empty for an
	// unscoped capability.
	Scope string
}

func (e *PermissionDeniedError) Error() string {
	if e.Scope != "" {
		return fmt.Sprintf(
			"permission denied: %q requires the %q permission for %s, which this session does not hold. "+
				"Being the coordinator of that group is not enough: the permission is granted per group.",
			e.Tool, e.Capability, e.Scope)
	}
	return fmt.Sprintf("permission denied: %q requires the %q permission, which this session does not hold "+
		"(grant it to this session, or for this group)", e.Tool, e.Capability)
}

/**
 * checkPermissions answers "may this session run this tool with these arguments".
 *
 * TWO QUESTIONS, and the second is why `requires_scope` exists:
 *
 *	1. does it hold the capability AT ALL — unscoped, or for any object;
 *	2. if the tool names the argument that scopes it, does it hold the capability FOR THAT OBJECT.
 *
 * THE SECOND IS WHAT MAKES A PER-GROUP GRANT MEAN ANYTHING. `group.spawn` narrowed to one group is a smaller
 * authorisation than the bare capability, and a check that ignored the scope would quietly widen it back.
 *
 * NIL GRANTS MEANS NO ENFORCEMENT — a test or a deployment that has not wired the source. That is the honest
 * reading of "nobody told me what this session may do": the alternative, refusing everything, would break every
 * caller that predates the wiring.
 */
func (e *Extension) checkPermissions(tenant, session string, def agent.ToolDef, input map[string]any) (capability, scope string, denied bool) {
	if len(def.Requires) == 0 {
		return "", "", false
	}
	if e.deps == nil {
		return "", "", false
	}
	grants := e.deps.SessionGrants(tenant, session)
	if grants == nil {
		return "", "", false
	}
	if missing, held := store.HasAnyCapability(grants, def.Requires); !held {
		return missing, "", true
	}
	if def.RequiresScope == "" {
		return "", "", false
	}
	object := strings.TrimSpace(strArg(input, def.RequiresScope))
	if object == "" {
		// A CALL WITH NO OBJECT IS LEFT TO THE TOOL, which can refuse a missing argument with a better message than
		// "permission denied" — the latter would send the reader looking for a grant they already hold.
		return "", "", false
	}
	for _, c := range def.Requires {
		if !store.HasCapabilityInScope(grants, c, store.GroupScope(object)) {
			return c, object, true
		}
	}
	return "", "", false
}

func (e *Extension) qualToBare(name string) (string, bool) {
	prefix := e.manifest.ID + "-"
	if strings.HasPrefix(name, prefix) {
		bare := strings.TrimPrefix(name, prefix)
		if _, ok := e.Lookup(bare); ok {
			return bare, true
		}
	}
	return "", false
}

// register wires every handler (mirrors createBundledConfig in the TS src).
func (e *Extension) register() {
	e.handlers["todo-write"] = e.todoWrite
	e.handlers["web-fetch"] = e.webFetch
	e.handlers["web-search"] = e.webSearch
	e.handlers["history-search"] = e.historySearch
	e.handlers["history-range-read"] = e.historyRangeRead
	e.handlers["history-read"] = e.historyRead
	e.handlers["file-info"] = e.fileInfo
	e.handlers["file-read"] = e.fileRead
	e.handlers["image-read"] = e.imageRead
	e.handlers["image-generate"] = e.imageGenerate
	e.handlers["image-edit"] = e.imageEdit
	e.handlers["video-generate"] = e.videoGenerate
	e.handlers["tts-generate"] = e.ttsGenerate
	e.handlers["tts-clone"] = e.ttsClone
	e.handlers["audio-transcribe"] = e.audioTranscribe
	// Group-mediated delegation replaced `subsession-create`/`mail-send`: every session-to-session
	// message now goes through a group, so there is no implicit delivery path left to register.
	e.handlers["group-create"] = e.groupCreate
	e.handlers["spawn"] = e.spawn
	e.handlers["group-send"] = e.groupSend
	// READ-ONLY group tools: a session can see who it is grouped with, never change it.
	e.handlers["group-list"] = e.groupList
	e.handlers["group-members"] = e.groupMembers
	// THE TWO GENERIC JOB TOOLS. Everything else about a job belongs to the extension that runs it;
	// what is generic is "what is running" and "wait for this one", and both are answered from the
	// shared job list rather than from the work itself.
}

// ---- small arg helpers ----

func strArg(args map[string]any, key string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	if v, ok := args[key]; ok && v != nil {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

/**
 * strListArg reads an array-of-strings argument, trimming and dropping empties and duplicates.
 *
 * ORDER IS PRESERVED and duplicates are collapsed: the caller's order is what a reader sees, and a
 * session named twice in one call is almost certainly a mistake rather than an instruction to send the
 * same text twice. Empty strings are dropped for the same reason — an address with nothing in it is a
 * gap in a list, not a recipient.
 *
 * A MISSING KEY AND A WRONG TYPE ARE DIFFERENT: missing returns nil with no error, because "the caller
 * did not mention it" is a case every caller handles; a wrong type is a malformed request and says so.
 */
func strListArg(args map[string]any, key string) ([]string, error) {
	raw, ok := args[key]
	if !ok || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		// A bare string is refused rather than wrapped: accepting both shapes means a caller that meant
		// one recipient can pass a scalar and never learn the field is an array, and the two paths would
		// need to stay in sync forever.
		return nil, fmt.Errorf("%q must be an array of strings", key)
	}
	out := make([]string, 0, len(list))
	seen := make(map[string]bool, len(list))
	for _, v := range list {
		s, _ := v.(string)
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out, nil
}

func numArg(args map[string]any, key string) (float64, bool) {
	switch v := args[key].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}

/**
 * Permissions returns the extension's declared permissions.
 *
 * THE EXECUTOR CANNOT DISCOVER THESE. A remote extension declares them in a manifest the executor
 * fetches; bundled is in this process, so the host has to read them directly and install them. Without
 * this method a `requires` on a bundled tool would name a permission no client could offer.
 */
func (e *Extension) Permissions() []PermissionDecl {
	if e == nil {
		return nil
	}
	out := make([]PermissionDecl, 0, len(e.manifest.Permissions))
	out = append(out, e.manifest.Permissions...)
	return out
}
