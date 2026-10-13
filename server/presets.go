package server

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// DefaultPreset is the built-in fallback preset id (mirrors the TS agent).
const DefaultPreset = "default"

// ListPresets lists presets (system + user), localizing the prompt when asked.
func (s *Server) ListPresets(ctx context.Context, req *agentv1.ListPresetsRequest) (*agentv1.ListPresetsResponse, error) {
	tenant := tenantOf(ctx)
	rows := s.presets.List(tenant)
	out := make([]*agentv1.Preset, 0, len(rows))
	for _, r := range rows {
		out = append(out, presetToProto(r, req.Locale))
	}
	return &agentv1.ListPresetsResponse{Presets: out}, nil
}

// UpsertPreset writes the USER keyspace (may shadow a system preset).
func (s *Server) UpsertPreset(ctx context.Context, req *agentv1.UpsertPresetRequest) (*agentv1.UpsertPresetResponse, error) {
	p := req.Preset
	if p == nil || p.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "preset.id is required")
	}
	row := agent.PresetRow{
		ID: p.Id, SystemPrompt: p.SystemPrompt, Tools: p.Tools, MaxTurns: int(p.MaxTurns),
	}
	if p.SystemPromptI18N != "" {
		var m map[string]string
		if json.Unmarshal([]byte(p.SystemPromptI18N), &m) == nil {
			row.SystemPromptI18N = m
		}
	}
	s.presets.Upsert(tenantOf(ctx), row)
	return &agentv1.UpsertPresetResponse{Ok: true}, nil
}

// DeletePreset removes ONLY the user row (restoring a system version).
func (s *Server) DeletePreset(ctx context.Context, req *agentv1.DeletePresetRequest) (*agentv1.DeletePresetResponse, error) {
	s.presets.Delete(tenantOf(ctx), req.Id)
	return &agentv1.DeletePresetResponse{Ok: true}, nil
}

// PreviewPreset returns the preset template + its rendered prompt (built-ins
// substituted; session-scoped variables left as literal placeholders).
func (s *Server) PreviewPreset(ctx context.Context, req *agentv1.PreviewPresetRequest) (*agentv1.PreviewPresetResponse, error) {
	tenant := tenantOf(ctx)
	row, ok := s.presets.Get(tenant, req.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "preset not found")
	}
	return &agentv1.PreviewPresetResponse{
		Template: row.SystemPrompt,
		Rendered: agent.RenderTemplate(row.SystemPrompt, s.now()),
	}, nil
}

func presetToProto(r agent.PresetRow, locale string) *agentv1.Preset {
	prompt := r.SystemPrompt
	if locale != "" {
		if v := agent.PickLocalized(r.SystemPromptI18N, locale); v != "" {
			prompt = v
		}
	}
	i18n, _ := json.Marshal(r.SystemPromptI18N)
	return &agentv1.Preset{
		Id: r.ID, SystemPrompt: prompt, SystemPromptI18N: string(i18n),
		Tools: r.Tools, MaxTurns: int32(r.MaxTurns), IsSystem: r.IsSystem,
	}
}
