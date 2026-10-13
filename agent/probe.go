package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/easydev-org/easyagent/llm"
)

// ProbeModel sends a trivial completion through a v4 gateway to verify that the
// provider credentials + model are usable. It returns the model's reply text.
func ProbeModel(ctx context.Context, baseURL, apiKey, model string) (string, error) {
	if model == "" {
		return "", errors.New("model is required")
	}
	c := llm.New(llm.ProviderCredentials{BaseURL: baseURL, APIKey: apiKey}, nil)
	ch, err := c.Stream(ctx, llm.ChatRequest{
		Model:    model,
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "Reply with exactly: OK"}},
		Extra:    map[string]any{"maxOutputTokens": 16},
	})
	if err != nil {
		return "", err
	}
	var text string
	for ev := range ch {
		switch ev.Type {
		case llm.EventTextDelta:
			text += ev.Text
		case llm.EventError:
			return "", ev.Err
		}
	}
	return strings.TrimSpace(text), nil
}
