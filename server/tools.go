package server

import (
	"context"
	"encoding/json"
	"errors"

	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// ListTools returns the discovered tools (with localized description + schema),
// read from the bus tool surface. In-memory mode (no bus) returns an empty list.
func (s *Server) ListTools(ctx context.Context, req *agentv1.ListToolsRequest) (*agentv1.ListToolsResponse, error) {
	if s.tools == nil && s.bundledTools == nil {
		return &agentv1.ListToolsResponse{Tools: nil}, nil
	}
	tenant := tenantOf(ctx)
	locale := agent.ResolveLocale("", configOrEmpty(cfgProvider(s.config), tenant, "locale"), "en")
	var defs []agent.ToolDef
	if s.bundledTools != nil {
		defs = append(defs, s.bundledTools.Tools()...)
	}
	if s.tools != nil {
		defs = append(defs, s.tools.Tools()...)
	}
	// List EVERY tool, including ones whose required config is unset.
	//
	// This used to mirror the turn plan's filter, to stop the console advertising
	// tools the executor would refuse. The cure was worse: a tool that is absent
	// cannot be discovered or configured, so the user saw a gap and had no way to
	// learn WHICH knob it wanted. The console now shows every tool and greys out
	// the runnable-blocked ones — the response already carries `required_config`
	// and `config_fields` (with current values), which is all a client needs.
	//
	// The turn plan keeps the hard filter (agent.FilterBlockedByConfig): the model
	// must never be offered a tool it cannot run.
	out := make([]*agentv1.ToolInfo, 0, len(defs))
	for _, d := range defs {
		// The tool's OWN name (TS `listTools` returns `t.name`, the raw name),
		// NOT extId-prefixed; `category` carries the extension id.
		info := &agentv1.ToolInfo{Name: d.Name, Category: d.ExtID}
		if v := agent.PickLocalized(d.Descriptions, locale); v != "" {
			info.Description = v
		} else {
			info.Description = d.Description
		}
		if d.InputSchema != nil {
			info.Parameters = structFromMap(agent.LocalizeSchema(d.InputSchema, locale))
		}
		info.RequiredConfig = d.RequiredConfig
		if s.bundledFields != nil {
			for _, cf := range s.bundledFields.ConfigFieldsFor(d.Name) {
				info.ConfigFields = append(info.ConfigFields, configFieldToProto(cf))
			}
		}
		out = append(out, info)
	}
	return &agentv1.ListToolsResponse{Tools: out}, nil
}

// configFieldToProto maps a declared config field onto its wire form.
func configFieldToProto(cf bundled.ConfigField) *agentv1.ToolConfigField {
	f := &agentv1.ToolConfigField{
		Name: cf.Name, Type: cf.Type, Description: cf.Description,
		Scope: cf.Scope, Kind: cf.Kind, Capability: cf.Capability,
	}
	// A secret knob's default is NEVER surfaced (it would be broadcast in the
	// manifest); the field declares only its shape.
	if cf.Default != "" && cf.Kind != bundled.ConfigKindSecret {
		f.Default = structpb.NewStringValue(cf.Default)
	}
	// A structured knob ships its SHAPE so the console can render a form (a card
	// per list item with a "+", a picker for an enum field) instead of a JSON
	// textarea. Empty Fields keeps the old behaviour intact.
	f.Fields = configFieldSchemas(cf.Fields)
	f.ItemType = cf.ItemType
	f.MinItems = int32(cf.MinItems)
	f.MaxItems = int32(cf.MaxItems)
	f.Namespace = cf.Namespace
	return f
}

// configFieldSchemas maps a structured knob's shape onto the wire type.
//
// It recurses the same shallow depth the extension declared: a field may be an
// object, and the console renderer stops there on purpose rather than growing an
// unbounded tree.
func configFieldSchemas(in []bundled.ConfigFieldSpec) []*agentv1.ConfigFieldSchema {
	if len(in) == 0 {
		return nil
	}
	out := make([]*agentv1.ConfigFieldSchema, 0, len(in))
	for _, f := range in {
		s := &agentv1.ConfigFieldSchema{
			Name: f.Name, Type: f.Type, Kind: f.Kind, Format: f.Format,
			EnumValues: f.EnumValues, Required: f.Required,
			Description: f.Description, Descriptions: f.Descriptions,
		}
		if f.Default != "" {
			s.Default = structpb.NewStringValue(fmt.Sprintf("%v", f.Default))
		}
		s.Fields = configFieldSchemas(f.Fields)
		out = append(out, s)
	}
	return out
}

// isSecretField reports whether a declared knob is a credential (its value is
// never returned to a client).
func isSecretField(cf bundled.ConfigField) bool {
	return cf.Kind == bundled.ConfigKindSecret
}

// isSecretKnob reports whether `knob` of extension `extID` is a secret. The
// bundled extension is the default when extID is empty/unknown.
func (s *Server) isSecretKnob(extID, knob string) bool {
	var fields []bundled.ConfigField
	if (extID == "" || extID == "bundled") && s.bundledFields != nil {
		fields = s.bundledFields.ConfigFields()
	} else if s.tools != nil {
		fields = s.tools.ExtensionConfigFields(extID)
	}
	for _, cf := range fields {
		if cf.Name == knob {
			return isSecretField(cf)
		}
	}
	return false
}

// ListExtensions returns each discovered extension's WHOLE config surface +
// capabilities + the tools it provides (the extension-config page). Both the
// in-process bundled extension AND every discovered NATS extension are listed.
func (s *Server) ListExtensions(ctx context.Context, _ *agentv1.ListExtensionsRequest) (*agentv1.ListExtensionsResponse, error) {
	out := make([]*agentv1.ExtensionInfo, 0, 2)
	// Re-register each extension's declared knob namespaces before anything reads
	// a value: a read must address the same key a write used, and the namespace is
	// part of that address.
	s.syncKnobNamespaces()
	// The in-process bundled extension (always present).
	if s.bundledTools != nil && s.bundledFields != nil {
		defs := s.bundledTools.Tools()
		info := &agentv1.ExtensionInfo{ExtId: "bundled", Version: "1"}
		if len(defs) > 0 {
			info.Capabilities = []string{"tools"}
		}
		for _, d := range defs {
			info.Tools = append(info.Tools, d.Name)
		}
		for _, cf := range s.bundledFields.ConfigFields() {
			info.ConfigFields = append(info.ConfigFields, configFieldToProto(cf))
		}
		out = append(out, info)
	}
	// Discovered NATS extensions (each with its declared config surface).
	if s.tools != nil {
		for _, extID := range s.tools.ExtensionIDs() {
			fields := s.tools.ExtensionConfigFields(extID)
			if len(fields) == 0 {
				continue
			}
			info := &agentv1.ExtensionInfo{ExtId: extID}
			for _, cf := range fields {
				info.ConfigFields = append(info.ConfigFields, configFieldToProto(cf))
			}
			// Group the surface: the tool names stay as they are, but a client
			// needs to know which group each one belongs to (and the group order),
			// otherwise 44 tools render as one undifferentiated list.
			for _, d := range s.tools.Tools() {
				if d.ExtID != extID {
					continue
				}
				info.Tools = append(info.Tools, d.Name)
				info.ToolsDetailed = append(info.ToolsDetailed, &agentv1.ExtensionToolInfo{
					Name: d.Name, Namespace: d.Namespace, Description: d.Description,
				})
			}
			for _, ns := range s.tools.ExtensionNamespaces(extID) {
				info.Namespaces = append(info.Namespaces, &agentv1.ExtensionNamespace{
					Name: ns.Name, Description: ns.Description, Descriptions: ns.Descriptions,
				})
			}
			out = append(out, info)
		}
	}
	return &agentv1.ListExtensionsResponse{Extensions: out}, nil
}

// GetExtensionConfig returns the configured value for each of an extension's
// declared knobs (a `{knob: value}` map). Empty when unset.
//
// SECRET knobs are withheld: their value never leaves the server. A secret
// knob that currently has a value is reported by name in `secret_set` so the
// client can render "set / not set" without ever seeing the credential.
func (s *Server) GetExtensionConfig(ctx context.Context, req *agentv1.GetExtensionConfigRequest) (*agentv1.GetExtensionConfigResponse, error) {
	s.syncKnobNamespaces()
	tenant := tenantOf(ctx)
	values := map[string]*structpb.Value{}
	var secretSet []string
	// The declared config surface of the requested extension (bundled or a
	// discovered NATS extension).
	var fields []bundled.ConfigField
	if req.ExtId == "" || req.ExtId == "bundled" {
		if s.bundledFields != nil {
			fields = s.bundledFields.ConfigFields()
		}
	} else if s.tools != nil {
		fields = s.tools.ExtensionConfigFields(req.ExtId)
	}
	for _, cf := range fields {
		// Read from the EXTENSION-scoped key: the same knob name under another
		// extension is a different value (see agent.cfgKey).
		v, ok := s.config.GetExt(tenant, req.ExtId, cf.Namespace, cf.Name)
		if isSecretField(cf) {
			// Withhold the value; only disclose that it is set.
			if ok && v != "" {
				secretSet = append(secretSet, cf.Name)
			}
			continue
		}
		if ok {
			values[cf.Name] = structpb.NewStringValue(v)
		}
	}
	return &agentv1.GetExtensionConfigResponse{Values: values, SecretSet: secretSet}, nil
}

// GetToolConfig returns the aggregate `{toolName: {knob: value}}` config surface
// read from the tenant config KV.
func (s *Server) GetToolConfig(ctx context.Context, _ *agentv1.GetToolConfigRequest) (*agentv1.GetToolConfigResponse, error) {
	tenant := tenantOf(ctx)
	values := map[string]*structpb.Value{}
	if s.tools != nil {
		for _, d := range s.tools.Tools() {
			bucket := map[string]any{}
			for _, knob := range d.RequiredConfig {
				// A secret knob's value is withheld (it must never reach a client).
				if s.isSecretKnob(d.ExtID, knob) {
					continue
				}
				if v, ok := s.extKnobValue(tenant, d.ExtID, knob); ok {
					bucket[knob] = v
				}
			}
			if len(bucket) > 0 {
				values[agent.LLMToolName(d)] = structpb.NewStructValue(structFromMap(bucket))
			}
		}
	}
	return &agentv1.GetToolConfigResponse{Config: &agentv1.ToolConfig{Values: values}}, nil
}

// SetToolConfig persists per-tool config values into the tenant config KV.
func (s *Server) SetToolConfig(ctx context.Context, req *agentv1.SetToolConfigRequest) (*agentv1.SetToolConfigResponse, error) {
	tenant := tenantOf(ctx)
	if s.config == nil {
		return nil, connectErr(errors.New("config store unavailable"))
	}
	if req.Config == nil {
		return &agentv1.SetToolConfigResponse{Ok: true}, nil
	}
	for tool, v := range req.Config.Fields {
		m := v.GetStructValue()
		if m == nil {
			continue
		}
		for knob, val := range m.Fields {
			// Route through the EXTENSION-scoped writer: the tool name carries the
			// owning extension, and writing tenant-wide would put the value in the
			// flat namespace every other extension reads from.
			extID := s.extOfTool(tool)
			ns := s.knobNamespace(extID, knob)
			s.config.SetExt(tenant, extID, ns, knob, valueToString(val))
		}
	}
	return &agentv1.SetToolConfigResponse{Ok: true}, nil
}

// extKnobValue reads one extension knob using its declared namespace.
func (s *Server) extKnobValue(tenant, extID, knob string) (string, bool) {
	return s.config.GetExt(tenant, extID, s.knobNamespace(extID, knob), knob)
}

// knobNamespace returns the namespace a knob is declared in ("" = extension
// level).
func (s *Server) knobNamespace(extID, knob string) string {
	for _, cf := range s.knobFields(extID) {
		if cf.Name == knob {
			return cf.Namespace
		}
	}
	return ""
}

// extOfTool maps a (possibly extension-qualified) tool name back to its owning
// extension. The LLM name is `<extId>-<tool>` for discovered extensions and the
// bare name for bundled ones.
//
// It resolves against the LIVE registry instead of a hard-coded list: a new
// extension must not need a code change here, and a stale list would silently
// route its config into the wrong namespace.
func (s *Server) extOfTool(tool string) string {
	if s.tools != nil {
		for _, d := range s.tools.Tools() {
			if agent.LLMToolName(d) == tool || d.Name == tool {
				return d.ExtID
			}
		}
	}
	return "bundled"
}

// SetExtensionConfig sets one extension config value (config authority).
func (s *Server) SetExtensionConfig(ctx context.Context, req *agentv1.SetExtensionConfigRequest) (*agentv1.SetExtensionConfigResponse, error) {
	s.syncKnobNamespaces()
	tenant := tenantOf(ctx)
	if s.config == nil {
		return nil, connectErr(errors.New("config store unavailable"))
	}
	if req.Name == "" {
		return nil, connectErr(errors.New("name is required"))
	}
	// Write to the EXTENSION-scoped key. Storing it tenant-wide (the old
	// behaviour) is what let `git.sandboxes` and `sandbox.sandboxes` share one
	// value: the key simply had no extension in it.
	//
	// The declared knob supplies the NAMESPACE (a structured/ordinary knob may be
	// scoped to one namespace of this extension). An unknown knob has none, which
	// lands it at extension level — still isolated from every other extension.
	ns := ""
	for _, cf := range s.knobFields(req.ExtId) {
		if cf.Name == req.Name {
			ns = cf.Namespace
			break
		}
	}
	extID := req.ExtId
	if extID == "" {
		extID = "bundled"
	}
	s.config.SetExt(tenant, extID, ns, req.Name, valueToString(req.Value))
	return &agentv1.SetExtensionConfigResponse{Ok: true}, nil
}

// knobFields lists an extension's declared config knobs (bundled or discovered).
func (s *Server) knobFields(extID string) []bundled.ConfigField {
	if extID == "" || extID == "bundled" {
		if s.bundledFields == nil {
			return nil
		}
		return s.bundledFields.ConfigFields()
	}
	if s.tools == nil {
		return nil
	}
	return s.tools.ExtensionConfigFields(extID)
}

// syncKnobNamespaces refreshes the (extension, knob) -> namespace map the config
// store uses to address values.
//
// It is derived from the live manifests rather than hard-coded: a manifest may
// move a knob into another namespace, and the address has to follow.
func (s *Server) syncKnobNamespaces() {
	if s.config == nil {
		return
	}
	m := map[string]string{}
	add := func(extID string, fields []bundled.ConfigField) {
		for _, f := range fields {
			m[extID+"."+f.Name] = f.Namespace
		}
	}
	if s.bundledFields != nil {
		add("bundled", s.bundledFields.ConfigFields())
	}
	if s.tools != nil {
		for _, extID := range s.tools.ExtensionIDs() {
			add(extID, s.tools.ExtensionConfigFields(extID))
		}
	}
	s.config.SetKnobNamespaces(m)
}

// MigrateLegacyConfig moves values out of the old flat namespace. Call it once
// at startup, AFTER the extensions have been discovered (the mapping needs to
// know which extensions declare which knob).
func (s *Server) MigrateLegacyConfig() int {
	s.syncKnobNamespaces()
	if s.config == nil {
		return 0
	}
	// knob name -> every extension that declares it. A name declared by two
	// extensions was ONE cell before, so it is copied to both rather than being
	// given to an arbitrary winner.
	declared := map[string][]string{}
	note := func(extID string, fields []bundled.ConfigField) {
		for _, f := range fields {
			declared[f.Name] = append(declared[f.Name], extID)
		}
	}
	if s.bundledFields != nil {
		note("bundled", s.bundledFields.ConfigFields())
	}
	if s.tools != nil {
		for _, extID := range s.tools.ExtensionIDs() {
			note(extID, s.tools.ExtensionConfigFields(extID))
		}
	}
	return s.config.MigrateConfigNamespaces(declared)
}

func configOrEmpty(c agent.ConfigProvider, tenant, key string) string {
	if c == nil {
		return ""
	}
	v, _ := c.Config(tenant, key)
	return v
}

// cfgProvider adapts the server's *agent.Config to agent.ConfigProvider.
func cfgProvider(c *agent.Config) agent.ConfigProvider {
	if c == nil {
		return nil
	}
	return agent.ConfigStore{C: c}
}

func configOrEmptyOK(c agent.ConfigProvider, tenant, key string) (string, bool) {
	if c == nil {
		return "", false
	}
	return c.Config(tenant, key)
}

// valueToString renders a protobuf Value as a config string (JSON for non-scalars).
func valueToString(v *structpb.Value) string {
	if v == nil {
		return ""
	}
	switch k := v.Kind.(type) {
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		b, _ := json.Marshal(k.NumberValue)
		return string(b)
	case *structpb.Value_BoolValue:
		if k.BoolValue {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(v.AsInterface())
		return string(b)
	}
}
