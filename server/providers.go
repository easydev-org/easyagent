package server

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// ListProviders returns the tenant's providers (api keys masked).
func (s *Server) ListProviders(ctx context.Context, _ *agentv1.ListProvidersRequest) (*agentv1.ListProvidersResponse, error) {
	rows, err := s.db.ListProviders(tenantOf(ctx))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.Provider, 0, len(rows))
	for _, r := range rows {
		out = append(out, providerToProto(r, decodeModels(r.Models), decodeHeaders(r.Headers)))
	}
	return &agentv1.ListProvidersResponse{Providers: out}, nil
}

// ListProvidersCatalog returns the accepted api types + their capabilities.
func (s *Server) ListProvidersCatalog(context.Context, *agentv1.ListProvidersCatalogRequest) (*agentv1.ListProvidersCatalogResponse, error) {
	caps := make([]string, len(agent.ModelCapabilities))
	for i, c := range agent.ModelCapabilities {
		caps[i] = string(c)
	}
	return &agentv1.ListProvidersCatalogResponse{
		ApiTypes: map[string]*agentv1.ApiTypeCatalog{
			agent.APITypeV4: {Capabilities: caps},
		},
	}, nil
}

// RegisterProvider upserts a provider row. The api key is stored verbatim; a
// value equal to the currently stored mask round-trips the stored key.
func (s *Server) RegisterProvider(ctx context.Context, req *agentv1.RegisterProviderRequest) (*agentv1.RegisterProviderResponse, error) {
	tenant := tenantOf(ctx)
	p := req.Provider
	if p == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, "provider is required")
	}
	if err := agent.ValidateAPIType(p.ApiType); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	if _, err := agent.ParseCapability(p.Capability); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	apiKey := p.ApiKey
	// A masked value means "unchanged": keep the stored key.
	if existing, ok, _ := s.providerByID(tenant, p.ProviderId); ok && apiKey == agent.MaskSecret(existing.APIKey) {
		apiKey = existing.APIKey
	}
	headers, _ := json.Marshal(p.Headers)
	models, _ := json.Marshal(decodeModelsOut(p.Models))
	row := store.Provider{
		Tenant: tenant, ProviderID: p.ProviderId, Capability: p.Capability,
		APIType: p.ApiType, BaseURL: p.BaseUrl, APIKey: apiKey,
		Headers: string(headers), Models: string(models),
	}
	if err := s.db.UpsertProvider(row); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RegisterProviderResponse{Ok: true}, nil
}

// DeleteProvider removes a tenant-local provider.
func (s *Server) DeleteProvider(ctx context.Context, req *agentv1.DeleteProviderRequest) (*agentv1.DeleteProviderResponse, error) {
	if err := s.db.DeleteProvider(tenantOf(ctx), req.ProviderId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.DeleteProviderResponse{Ok: true}, nil
}

// ListModels returns the models of ONE provider (provider_id required).
func (s *Server) ListModels(ctx context.Context, req *agentv1.ListModelsRequest) (*agentv1.ListModelsResponse, error) {
	if req.ProviderId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "provider_id is required")
	}
	row, ok, err := s.providerByID(tenantOf(ctx), req.ProviderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "provider not found")
	}
	return &agentv1.ListModelsResponse{Models: modelsToInfo(decodeModels(row.Models))}, nil
}

// modelsToInfo maps provider models to the ListModels ModelInfo shape.
func modelsToInfo(ms []*agentv1.ProviderModel) []*agentv1.ModelInfo {
	out := make([]*agentv1.ModelInfo, 0, len(ms))
	for _, m := range ms {
		info := &agentv1.ModelInfo{Id: m.Id, Name: m.Name, ContextLimit: m.ContextLimit}
		// Text models with a context window offer the reasoning variants.
		for _, v := range agent.VariantsFor(m.ModelType == "text" && m.ContextLimit > 0) {
			info.Variants = append(info.Variants, &agentv1.ModelVariant{
				Id: v.ID, Name: v.Name, Description: v.Description,
			})
		}
		out = append(out, info)
	}
	return out
}

// TestProvider probes a provider's model with a trivial completion.
func (s *Server) TestProvider(ctx context.Context, req *agentv1.TestProviderRequest) (*agentv1.TestProviderResponse, error) {
	tenant := tenantOf(ctx)
	baseURL, apiKey := req.BaseUrl, req.ApiKey
	if req.ProviderId != "" {
		if row, ok, _ := s.providerByID(tenant, req.ProviderId); ok {
			if baseURL == "" {
				baseURL = row.BaseURL
			}
			if apiKey == "" || apiKey == agent.MaskSecret(row.APIKey) {
				apiKey = row.APIKey
			}
		}
	}
	if err := agent.ValidateAPIType(req.ApiType); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	res, err := agent.ProbeModel(ctx, baseURL, apiKey, req.Model)
	if err != nil {
		return &agentv1.TestProviderResponse{Ok: false, Result: err.Error()}, nil
	}
	return &agentv1.TestProviderResponse{Ok: true, Result: res}, nil
}

func (s *Server) providerByID(tenant, id string) (store.Provider, bool, error) {
	rows, err := s.db.ListProviders(tenant)
	if err != nil {
		return store.Provider{}, false, err
	}
	for _, r := range rows {
		if r.ProviderID == id {
			return r, true, nil
		}
	}
	return store.Provider{}, false, nil
}

func decodeModels(raw string) []*agentv1.ProviderModel {
	if raw == "" || raw == "null" {
		return nil
	}
	var rows []struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		ContextLimit int64  `json:"context_limit"`
		ModelType    string `json:"model_type"`
	}
	if json.Unmarshal([]byte(raw), &rows) != nil {
		return nil
	}
	out := make([]*agentv1.ProviderModel, 0, len(rows))
	for _, r := range rows {
		out = append(out, &agentv1.ProviderModel{
			Id: r.ID, Name: r.Name, ContextLimit: r.ContextLimit, ModelType: r.ModelType,
		})
	}
	return out
}

func decodeModelsOut(ms []*agentv1.ProviderModel) []map[string]any {
	out := make([]map[string]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, map[string]any{
			"id": m.Id, "name": m.Name, "context_limit": m.ContextLimit, "model_type": m.ModelType,
		})
	}
	return out
}

func decodeHeaders(raw string) map[string]string {
	if raw == "" || raw == "null" {
		return nil
	}
	var m map[string]string
	if json.Unmarshal([]byte(raw), &m) != nil {
		return nil
	}
	return m
}
