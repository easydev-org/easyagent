package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// captureBody runs one Stream against a server that records the v4 request body.
func captureBody(t *testing.T, req ChatRequest) map[string]any {
	t.Helper()
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		writeV4SSE(w, `{"type":"text-delta","id":"t","delta":"ok"}`, `{"type":"finish","finishReason":{"unified":"stop"}}`)
	}))
	defer srv.Close()
	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	ch, err := c.Stream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	return body
}

func TestBuildV4BodyAssistantToolCallsAndToolResult(t *testing.T) {
	body := captureBody(t, ChatRequest{
		Model: "m",
		Messages: []Message{
			{Role: RoleUser, Content: "hi"},
			{Role: RoleAssistant, ToolCalls: []ToolCall{
				{ID: "call_1", Name: "todo-write", Args: `{"todos":[]}`},
			}},
			{Role: RoleTool, ToolCallID: "call_1", Name: "todo-write", Content: "done"},
		},
	})
	prompt, _ := body["prompt"].([]any)
	if len(prompt) != 3 {
		t.Fatalf("prompt len = %d, want 3", len(prompt))
	}
	asst, _ := prompt[1].(map[string]any)
	if asst["role"] != "assistant" {
		t.Errorf("prompt[1].role = %v", asst["role"])
	}
	parts, _ := asst["content"].([]any)
	if len(parts) != 1 {
		t.Fatalf("assistant parts = %v", asst["content"])
	}
	tc, _ := parts[0].(map[string]any)
	if tc["type"] != "tool-call" || tc["toolName"] != "todo-write" {
		t.Errorf("tool-call part = %v", tc)
	}
	// v4 prompt tool-call.input is the PARSED object, not a string.
	if _, ok := tc["input"].(map[string]any); !ok {
		t.Errorf("tool-call input should be an object, got %T", tc["input"])
	}
	tool, _ := prompt[2].(map[string]any)
	if tool["role"] != "tool" {
		t.Errorf("prompt[2].role = %v", tool["role"])
	}
	tparts, _ := tool["content"].([]any)
	tr, _ := tparts[0].(map[string]any)
	if tr["type"] != "tool-result" || tr["toolCallId"] != "call_1" {
		t.Errorf("tool-result part = %v", tr)
	}
	out, _ := tr["output"].(map[string]any)
	if out["type"] != "text" || out["value"] != "done" {
		t.Errorf("tool-result output = %v", out)
	}
}

func TestBuildV4BodyToolChoiceNamed(t *testing.T) {
	body := captureBody(t, ChatRequest{
		Model:      "m",
		Messages:   []Message{{Role: RoleUser, Content: "hi"}},
		ToolChoice: "todo-write",
	})
	tc, _ := body["toolChoice"].(map[string]any)
	if tc["type"] != "tool" || tc["toolName"] != "todo-write" {
		t.Errorf("toolChoice = %v", body["toolChoice"])
	}
}

func TestBuildV4BodyEmptyModelErrors(t *testing.T) {
	c := New(ProviderCredentials{BaseURL: "http://127.0.0.1:0", APIKey: "k"}, nil)
	if _, err := c.Stream(context.Background(), ChatRequest{}); err == nil {
		t.Error("empty model should error")
	}
}

func TestStreamCancelClosesChannel(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"type\":\"text-delta\",\"id\":\"t\",\"delta\":\"a\"}\n\n"))
		if fl != nil {
			fl.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-block:
		}
	}))
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithCancel(context.Background())
	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	ch, err := c.Stream(ctx, ChatRequest{Model: "m", Messages: []Message{{Role: RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	cancel()
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("channel did not close after cancel")
	}
}
