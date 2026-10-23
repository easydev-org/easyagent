package bundled

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/easydev-org/easyagent/agent"
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
	return h(ctx, tenant, session, parsed)
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
	e.handlers["brave-search"] = e.braveSearch
	e.handlers["history-search"] = e.historySearch
	e.handlers["history-range"] = e.historyRange
	e.handlers["file-info"] = e.fileInfo
	e.handlers["file-read"] = e.fileRead
	e.handlers["image-read"] = e.imageRead
	e.handlers["image-generate"] = e.imageGenerate
	e.handlers["image-edit"] = e.imageEdit
	e.handlers["video-generate"] = e.videoGenerate
	e.handlers["tts-generate"] = e.ttsGenerate
	e.handlers["tts-clone"] = e.ttsClone
	e.handlers["audio-transcribe"] = e.audioTranscribe
	e.handlers["subsession-create"] = e.subsessionCreate
	e.handlers["mail-send"] = e.mailSend
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
