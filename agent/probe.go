package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/easydev-org/easyagent/llm"
)

// ProbeModel sends a trivial completion through a v4 gateway to verify that the
// provider credentials + model are usable. It returns the model's reply text.
// ProbeModel sends ONE prompt to a text model and returns what it said.
//
// THE PROMPT IS THE CALLER'S, NOT A CONSTANT. This used to send "Reply with exactly: OK" because its
// only job was liveness: a provider row that parses and a model that answers prove the same thing, and
// a short reply was cheaper. That made it useless as a TEST — the operator typed a question, the panel
// called this, and every model answered "OK", which reads as the panel ignoring the input.
//
// maxOutputTokens IS GENEROUS ON PURPOSE. It was 16, which truncates any real answer mid-sentence and
// looks like the model stopped working.
func ProbeModel(ctx context.Context, baseURL, apiKey, model, prompt string) (string, error) {
	if model == "" {
		return "", errors.New("model is required")
	}
	if prompt == "" {
		prompt = "Hello"
	}
	c := llm.New(llm.ProviderCredentials{BaseURL: baseURL, APIKey: apiKey}, nil)
	ch, err := c.Stream(ctx, llm.ChatRequest{
		Model:    model,
		Messages: []llm.Message{{Role: llm.RoleUser, Content: prompt}},
		Extra:    map[string]any{"maxOutputTokens": 2048},
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
