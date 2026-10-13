package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// ProviderResolver resolves a canonical `provider_id/model_id` reference to an
// llm client. The registry lives in the store; the llm package never touches a
// DB.
type ProviderResolver interface {
	Resolve(ctx context.Context, tenant, ref string) (*llm.Client, error)
}

// StoreResolver resolves providers from the store's provider rows. Only the
// v4-gateway protocol is supported: every provider's `base_url` is an AI SDK v4
// mount (`/v4/ai`).
type StoreResolver struct {
	DB *store.DB
}

// Resolve parses `provider_id/model_id`, looks up the provider row, and builds
// a client from its base_url + api_key.
func (r *StoreResolver) Resolve(_ context.Context, tenant, ref string) (*llm.Client, error) {
	providerID, _, ok := splitModelRef(ref)
	if !ok {
		return nil, fmt.Errorf("agent: no model selected - set a model as \"provider_id/model_id\" (got %q)", ref)
	}
	rows, err := r.DB.ListProviders(tenant)
	if err != nil {
		return nil, err
	}
	for _, p := range rows {
		if p.ProviderID == providerID {
			// Only a TEXT provider can drive chat turns (semantic grouping).
			if c := p.Capability; c != "" && c != string(CapText) {
				return nil, fmt.Errorf("agent: provider %q serves %q, not text", providerID, c)
			}
			return llm.New(llm.ProviderCredentials{
				ProviderID: p.ProviderID,
				BaseURL:    p.BaseURL,
				APIKey:     p.APIKey,
				Headers:    parseHeaders(p.Headers),
			}, nil), nil
		}
	}
	return nil, fmt.Errorf("agent: provider not found: %s", providerID)
}

// splitModelRef splits `provider/model` at the FIRST slash; both parts must be
// non-empty (a bare model id is never resolved).
func splitModelRef(ref string) (providerID, modelID string, ok bool) {
	for i := 0; i < len(ref); i++ {
		if ref[i] == '/' {
			if i == 0 || i == len(ref)-1 {
				return "", "", false
			}
			return ref[:i], ref[i+1:], true
		}
	}
	return "", "", false
}

// ModelOf returns the bare model id from a `provider/model` reference.
func ModelOf(ref string) string {
	_, m, _ := splitModelRef(ref)
	return m
}

// modelContextLimit reads a model's context window from its provider row's
// `models` metadata (the `context_limit` field).
func modelContextLimit(db *store.DB, tenant, ref string) (int, bool) {
	providerID, modelID, ok := splitModelRef(ref)
	if !ok {
		return 0, false
	}
	rows, err := db.ListProviders(tenant)
	if err != nil {
		return 0, false
	}
	for _, p := range rows {
		if p.ProviderID != providerID {
			continue
		}
		var models []struct {
			ID           string `json:"id"`
			ContextLimit int64  `json:"context_limit"`
		}
		if json.Unmarshal([]byte(p.Models), &models) != nil {
			return 0, false
		}
		for _, m := range models {
			if m.ID == modelID && m.ContextLimit > 0 {
				return int(m.ContextLimit), true
			}
		}
	}
	return 0, false
}

func parseHeaders(raw string) map[string]string {
	if raw == "" || raw == "null" {
		return nil
	}
	var m map[string]string
	if json.Unmarshal([]byte(raw), &m) != nil {
		return nil
	}
	return m
}
