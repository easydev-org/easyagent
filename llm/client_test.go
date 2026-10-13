package llm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// writeV4SSE streams v4 stream-part JSON frames then a terminating [DONE].
func writeV4SSE(w http.ResponseWriter, parts ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	fl, _ := w.(http.Flusher)
	for _, p := range parts {
		fmt.Fprintf(w, "data: %s\n\n", p)
		if fl != nil {
			fl.Flush()
		}
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	if fl != nil {
		fl.Flush()
	}
}

func TestStreamNormalizesV4Parts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// headers must carry the model id + streaming flag
		if r.Header.Get("ai-language-model-id") != "m" {
			t.Errorf("missing model header: %q", r.Header.Get("ai-language-model-id"))
		}
		if r.Header.Get("ai-language-model-streaming") != "true" {
			t.Errorf("streaming header = %q", r.Header.Get("ai-language-model-streaming"))
		}
		writeV4SSE(w,
			`{"type":"text-start","id":"t1"}`,
			`{"type":"text-delta","id":"t1","delta":"Hel"}`,
			`{"type":"text-delta","id":"t1","delta":"lo"}`,
			`{"type":"reasoning-delta","id":"r1","delta":"why "}`,
			`{"type":"tool-input-start","id":"call_1","toolName":"todo-write"}`,
			`{"type":"tool-input-delta","id":"call_1","delta":"{\"todos\":"}`,
			`{"type":"tool-call","toolCallId":"call_1","toolName":"todo-write","input":"{\"todos\":[]}"}`,
			`{"type":"finish","finishReason":{"unified":"tool-calls","raw":"tool_calls"},"usage":{"inputTokens":{"total":10},"outputTokens":{"total":5}}}`,
		)
	}))
	defer srv.Close()

	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	ch, err := c.Stream(context.Background(), ChatRequest{
		Model: "m", Messages: []Message{{Role: RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var text, reasoning string
	var tool *ToolCall
	var finish string
	var usage *Usage
	for ev := range ch {
		switch ev.Type {
		case EventTextDelta:
			text += ev.Text
		case EventReasoningDelta:
			reasoning += ev.Text
		case EventToolCall:
			tool = ev.ToolCall
		case EventFinishStep:
			finish = ev.FinishReason
			usage = ev.Usage
		case EventError:
			t.Fatalf("unexpected error: %v", ev.Err)
		}
	}
	if text != "Hello" || reasoning != "why " {
		t.Errorf("text=%q reasoning=%q", text, reasoning)
	}
	if tool == nil || tool.ID != "call_1" || tool.Name != "todo-write" || tool.Args != `{"todos":[]}` {
		t.Errorf("tool = %+v", tool)
	}
	if finish != "tool-calls" {
		t.Errorf("finish = %q", finish)
	}
	if usage == nil || usage.InputTokens != 10 || usage.OutputTokens != 5 {
		t.Errorf("usage = %+v", usage)
	}
}

func TestStreamSendsV4Body(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 1024)
		for {
			n, err := r.Body.Read(tmp)
			buf = append(buf, tmp[:n]...)
			if err != nil {
				break
			}
		}
		body = buf
		writeV4SSE(w, `{"type":"text-delta","id":"t","delta":"ok"}`, `{"type":"finish","finishReason":{"unified":"stop"}}`)
	}))
	defer srv.Close()

	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	ch, err := c.Stream(context.Background(), ChatRequest{
		Model: "m", System: "sys",
		Messages: []Message{{Role: RoleUser, Content: "hi"}},
		Tools:    []ToolDef{{Name: "t", Description: "d", Parameters: map[string]any{"type": "object"}}},
		Extra:    map[string]any{"reasoning": "high"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	s := string(body)
	for _, want := range []string{`"prompt"`, `"role":"system"`, `"role":"user"`, `"type":"function"`, `"inputSchema"`, `"reasoning":"high"`} {
		if !contains(s, want) {
			t.Errorf("body missing %s:\n%s", want, s)
		}
	}
	// The v4 prompt is NOT the OpenAI shape.
	if contains(s, `"messages"`) {
		t.Errorf("body should use `prompt`, not `messages`:\n%s", s)
	}
}

func TestRetryingStreamRetriesTransient(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limited","type":"rate_limit_error"}}`)
			return
		}
		writeV4SSE(w, `{"type":"text-delta","id":"t","delta":"ok"}`, `{"type":"finish","finishReason":{"unified":"stop"}}`)
	}))
	defer srv.Close()

	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	var retries int
	ch, err := c.RetryingStream(context.Background(),
		ChatRequest{Model: "m", Messages: []Message{{Role: RoleUser, Content: "hi"}}},
		RetryPolicy{MaxRetries: 3, InitialDelay: time.Millisecond},
		func(attempt int, delay time.Duration, e error) { retries++ },
	)
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var finish bool
	for ev := range ch {
		switch ev.Type {
		case EventTextDelta:
			text += ev.Text
		case EventFinishStep:
			finish = true
		case EventError:
			t.Fatalf("unexpected error: %v", ev.Err)
		}
	}
	if retries != 1 || text != "ok" || !finish {
		t.Errorf("retries=%d text=%q finish=%v", retries, text, finish)
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(nil) {
		t.Error("nil should not be retryable")
	}
	if !IsRetryable(fmt.Errorf("dial tcp: i/o timeout")) {
		t.Error("i/o timeout should be retryable")
	}
	if IsRetryable(&APIError{StatusCode: 401, Message: "invalid api key"}) {
		t.Error("401 should not be retryable")
	}
	if !IsRetryable(&APIError{StatusCode: 429}) {
		t.Error("429 should be retryable")
	}
}

func TestStreamPassesThroughToolInputFraming(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeV4SSE(w,
			`{"type":"tool-input-start","id":"c1","toolName":"echo"}`,
			`{"type":"tool-input-delta","id":"c1","delta":"{\"a\":"}`,
			`{"type":"tool-input-delta","id":"c1","delta":"1}"}`,
			`{"type":"tool-input-end","id":"c1"}`,
			`{"type":"tool-call","toolCallId":"c1","toolName":"echo","input":"{\"a\":1}"}`,
			`{"type":"finish","finishReason":{"unified":"tool-calls"}}`,
		)
	}))
	defer srv.Close()
	c := New(ProviderCredentials{BaseURL: srv.URL, APIKey: "k"}, nil)
	ch, err := c.Stream(context.Background(), ChatRequest{Model: "m", Messages: []Message{{Role: RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	var deltaText string
	var call *ToolCall
	for ev := range ch {
		got = append(got, string(ev.Type))
		switch ev.Type {
		case EventToolInputDelta:
			deltaText += ev.Text
		case EventToolCall:
			call = ev.ToolCall
		}
	}
	want := []string{"tool-input-start", "tool-input-delta", "tool-input-delta", "tool-input-end", "tool-call", "finish-step"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("events = %v, want %v", got, want)
	}
	if deltaText != `{"a":1}` {
		t.Errorf("delta text = %q", deltaText)
	}
	if call == nil || call.ID != "c1" || call.Name != "echo" || call.Args != `{"a":1}` {
		t.Errorf("tool-call = %+v", call)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
