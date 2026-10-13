package server

import (
	"context"

	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// GetConfig reads a tenant config value.
func (s *Server) GetConfig(ctx context.Context, req *agentv1.GetConfigRequest) (*agentv1.GetConfigResponse, error) {
	v, _ := s.config.Get(tenantOf(ctx), req.Key)
	return &agentv1.GetConfigResponse{Key: req.Key, Value: v}, nil
}

// SetConfig writes a tenant config value.
func (s *Server) SetConfig(ctx context.Context, req *agentv1.SetConfigRequest) (*agentv1.SetConfigResponse, error) {
	s.config.Set(tenantOf(ctx), req.Key, req.Value)
	return &agentv1.SetConfigResponse{Ok: true}, nil
}

// GetAgentConfig returns the agent's own config (static defaults).
func (s *Server) GetAgentConfig(context.Context, *agentv1.GetAgentConfigRequest) (*agentv1.GetAgentConfigResponse, error) {
	return &agentv1.GetAgentConfigResponse{Config: structFromMap(map[string]any{
		"name": "easyagent-go", "api_types": []any{agent.APITypeV4},
	})}, nil
}
