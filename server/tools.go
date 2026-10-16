package server

import (
	"context"
	"encoding/json"
	"errors"

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
				f := &agentv1.ToolConfigField{
					Name: cf.Name, Type: cf.Type, Description: cf.Description,
					Scope: cf.Scope, Kind: cf.Kind, Capability: cf.Capability,
				}
				if cf.Default != "" {
					f.Default = structpb.NewStringValue(cf.Default)
				}
				info.ConfigFields = append(info.ConfigFields, f)
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
	if cf.Default != "" {
		f.Default = structpb.NewStringValue(cf.Default)
	}
	return f
}

// ListExtensions returns each discovered extension's WHOLE config surface +
// capabilities + the tools it provides (the extension-config page). Only the
// in-process bundled extension is currently discoverable here.
func (s *Server) ListExtensions(ctx context.Context, _ *agentv1.ListExtensionsRequest) (*agentv1.ListExtensionsResponse, error) {
	if s.bundledTools == nil || s.bundledFields == nil {
		return &agentv1.ListExtensionsResponse{}, nil
	}
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
	return &agentv1.ListExtensionsResponse{Extensions: []*agentv1.ExtensionInfo{info}}, nil
}

// GetExtensionConfig returns the configured value for each of an extension's
// declared knobs (a `{knob: value}` map). Empty when unset.
func (s *Server) GetExtensionConfig(ctx context.Context, req *agentv1.GetExtensionConfigRequest) (*agentv1.GetExtensionConfigResponse, error) {
	tenant := tenantOf(ctx)
	values := map[string]*structpb.Value{}
	if s.bundledFields != nil && (req.ExtId == "" || req.ExtId == "bundled") {
		for _, cf := range s.bundledFields.ConfigFields() {
			if v, ok := configOrEmptyOK(cfgProvider(s.config), tenant, cf.Name); ok {
				values[cf.Name] = structpb.NewStringValue(v)
			}
		}
	}
	return &agentv1.GetExtensionConfigResponse{Values: values}, nil
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
				if v, ok := configOrEmptyOK(cfgProvider(s.config), tenant, knob); ok {
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
			s.config.Set(tenant, knob, valueToString(val))
		}
		_ = tool
	}
	return &agentv1.SetToolConfigResponse{Ok: true}, nil
}

// SetExtensionConfig sets one extension config value (config authority).
func (s *Server) SetExtensionConfig(ctx context.Context, req *agentv1.SetExtensionConfigRequest) (*agentv1.SetExtensionConfigResponse, error) {
	tenant := tenantOf(ctx)
	if s.config == nil {
		return nil, connectErr(errors.New("config store unavailable"))
	}
	if req.Name == "" {
		return nil, connectErr(errors.New("name is required"))
	}
	s.config.Set(tenant, req.Name, valueToString(req.Value))
	return &agentv1.SetExtensionConfigResponse{Ok: true}, nil
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
