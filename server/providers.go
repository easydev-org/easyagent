package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/llm"
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

// TestProvider exercises ONE model of ONE modality.
//
// SOMETHING IS SENT TO THE MODEL, because "the provider row is valid" is not the question anybody is
// asking — the question is whether a call works right now. Which thing is sent depends on the
// modality: a chat completion cannot test a TTS model, so each modality has its own call, and the
// caller says which one it wants.
//
// THE MODEL ARRIVES AS A `provider_id/model_id` REFERENCE, and only the model id is the gateway's. The
// console sends the canonical reference it stores — `gateway-text/tal-coding/deepseek-v4.1-flash` —
// because that is what every other call in the system takes. Handing that straight to the gateway asks
// it for a model named after our own provider id, which it does not have, and the answer is a 404 that
// reads like the model is missing rather than the ref being unparsed.
func (s *Server) TestProvider(ctx context.Context, req *agentv1.TestProviderRequest) (*agentv1.TestProviderResponse, error) {
	tenant := tenantOf(ctx)
	baseURL, apiKey := req.BaseUrl, req.ApiKey
	capability := req.GetCapability()
	if capability == "" {
		capability = "text"
	}
	if req.ProviderId != "" {
		if row, ok, _ := s.providerByID(tenant, req.ProviderId); ok {
			if baseURL == "" {
				baseURL = row.BaseURL
			}
			if apiKey == "" || apiKey == agent.MaskSecret(row.APIKey) {
				apiKey = row.APIKey
			}
			if row.Capability != "" {
				capability = row.Capability
			}
		}
	}
	if err := agent.ValidateAPIType(req.ApiType); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	cap, err := agent.ParseCapability(capability)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	// STRIP THE PROVIDER PREFIX. `ModelOf` returns the bare id when the ref parses and the ref itself
	// when it does not, so a caller that already sent a bare id is unaffected.
	model := agent.ModelOf(req.GetModel())
	if model == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "model is required")
	}
	if baseURL == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "base_url is required")
	}

	c := llm.New(llm.ProviderCredentials{BaseURL: baseURL, APIKey: apiKey}, nil)
	prompt := strings.TrimSpace(req.GetPrompt())

	switch cap {
	case agent.CapText:
		if prompt == "" {
			return invalidTest("the text probe needs a prompt")
		}
		// THE PROMPT IS WHAT THE OPERATOR TYPED, and what comes back is the model's actual answer.
		text, err := agent.ProbeModel(ctx, baseURL, apiKey, model, prompt)
		if err != nil {
			return &agentv1.TestProviderResponse{Ok: false, Result: err.Error()}, nil
		}
		return &agentv1.TestProviderResponse{Ok: true, Result: text}, nil

	case agent.CapImage:
		if prompt == "" {
			return invalidTest("the image probe needs a prompt")
		}
		// n=1: a test produces one image. Asking for several spends quota to prove the same thing.
		files, err := c.GenerateImage(ctx, model, prompt, 1, "", nil)
		return testFiles(files, err)

	case agent.CapVideo:
		if prompt == "" {
			return invalidTest("the video probe needs a prompt")
		}
		files, err := c.GenerateVideo(ctx, model, map[string]any{
			"prompt":   map[string]any{"text": prompt},
			"n":        1,
			"duration": 4,
		})
		return testFiles(files, err)

	case agent.CapSpeech:
		if prompt == "" {
			return invalidTest("the speech probe needs some text to say")
		}
		f, err := c.GenerateSpeech(ctx, model, prompt, req.GetVoice(), "")
		if err != nil {
			return &agentv1.TestProviderResponse{Ok: false, Result: err.Error()}, nil
		}
		return fileResult(f, "speech synthesised"), nil

	case agent.CapTranscription:
		audio, derr := base64.StdEncoding.DecodeString(req.GetAudio())
		if derr != nil || len(audio) == 0 {
			return invalidTest("the transcription probe needs recorded audio")
		}
		mt := req.GetAudioMediaType()
		if mt == "" {
			mt = "audio/webm"
		}
		text, err := c.Transcribe(ctx, model, audio, mt)
		if err != nil {
			return &agentv1.TestProviderResponse{Ok: false, Result: err.Error()}, nil
		}
		return &agentv1.TestProviderResponse{Ok: true, Result: text}, nil
	}

	// embedding / rerank / realtime: the console does not offer them, so there is no probe shape for
	// them either. Saying so is better than a generic failure a caller will retry.
	return &agentv1.TestProviderResponse{
		Ok:     false,
		Result: fmt.Sprintf("there is no test interface for a %q provider", cap),
	}, nil
}

func invalidTest(msg string) (*agentv1.TestProviderResponse, error) {
	return &agentv1.TestProviderResponse{Ok: false, Result: msg}, nil
}

// testFiles turns a generated-file list into the response shape, or the error.
func testFiles(files []llm.GeneratedFile, err error) (*agentv1.TestProviderResponse, error) {
	if err != nil {
		return &agentv1.TestProviderResponse{Ok: false, Result: err.Error()}, nil
	}
	if len(files) == 0 {
		return &agentv1.TestProviderResponse{Ok: false, Result: "the model returned nothing"}, nil
	}
	out := make([]*agentv1.GeneratedFile, 0, len(files))
	for _, f := range files {
		out = append(out, &agentv1.GeneratedFile{MediaType: f.MediaType, Data: f.Data})
	}
	return &agentv1.TestProviderResponse{Ok: true, Files: out}, nil
}

func fileResult(f llm.GeneratedFile, ok string) *agentv1.TestProviderResponse {
	return &agentv1.TestProviderResponse{
		Ok:     true,
		Result: ok,
		Files:  []*agentv1.GeneratedFile{{MediaType: f.MediaType, Data: f.Data}},
	}
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
