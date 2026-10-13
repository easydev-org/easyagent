package agent

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// TestGatewayTurn exercises the whole turn loop against the REAL
// ai-multimodal-gateway (tal-ai/ai-multimodal-gateway) **v4** endpoint
// (`/v4/ai/language-model`, the AI SDK provider-spec v4 wire): streaming text,
// a live tool call, and the emitted event stream.
//
// It is skipped unless GW_BASE and GW_KEY are set, so `go test ./...` stays
// hermetic. Run it with:
//
//	GW_BASE=http://ai-multimodal-gateway.worker.svc.cluster.local/v4/ai \
//	GW_KEY=<tal-coding key> GW_MODEL=tal-coding/deepseek-v4.1-flash \
//	go test ./agent -run TestGatewayTurn -v
func TestGatewayTurn(t *testing.T) {
	base := os.Getenv("GW_BASE")
	key := os.Getenv("GW_KEY")
	if base == "" || key == "" {
		t.Skip("set GW_BASE and GW_KEY to run the gateway integration test")
	}
	model := os.Getenv("GW_MODEL")
	if model == "" {
		model = "tal-coding/deepseek-v4.1-flash"
	}

	client := llm.New(llm.ProviderCredentials{BaseURL: base, APIKey: key}, nil)
	rec := &recorder{}
	tools := &stubTools{out: ToolOutcome{Content: "Beijing: 21°C, clear"}}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	res, err := RunTurn(ctx, Deps{
		Client:   client,
		Retry:    llm.RetryPolicy{MaxRetries: 3, InitialDelay: time.Second},
		Tools:    tools,
		Emit:     rec,
		MaxTurns: 4,
	}, Input{
		Model:   model,
		System:  "You are a concise assistant. Use the get_weather tool when asked about weather.",
		RunID:   "itest-run",
		History: []llm.Message{{Role: llm.RoleUser, Content: "What is the weather in Beijing? Use the tool, then answer in one sentence."}},
		Tools: []llm.ToolDef{{
			Name:        "get_weather",
			Description: "Get the current weather for a city",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"city": map[string]any{"type": "string"},
				},
				"required": []string{"city"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("RunTurn: %v", err)
	}

	got := rec.names()
	t.Logf("model=%s steps=%d reason=%s finish=%s", model, res.Steps, res.Reason, res.FinishReason)
	t.Logf("events=%v", got)
	t.Logf("tool calls=%v", tools.calls)
	t.Logf("text=%q", res.Text)
	t.Logf("usage in=%d out=%d", res.InputTokens, res.OutputTokens)

	// The model must have called the tool and produced a final answer.
	if len(tools.calls) == 0 {
		t.Error("expected the model to call get_weather")
	}
	if !has(got, "tool-call") || !has(got, "tool-result") {
		t.Error("expected tool-call + tool-result events")
	}
	if !has(got, "text-delta") {
		t.Error("expected text-delta events")
	}
	if res.Reason != "stop" {
		t.Errorf("reason = %q, want stop", res.Reason)
	}
	if strings.TrimSpace(res.Text) == "" {
		t.Error("expected non-empty final text")
	}
}

func has(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
