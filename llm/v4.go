package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ProviderCredentials are the per-tenant provider settings resolved from the
// agent's registry. The llm package never touches a DB — the caller supplies
// these.
type ProviderCredentials struct {
	ProviderID string
	BaseURL    string
	APIKey     string
	Headers    map[string]string
}

// Client is a self-contained Vercel AI SDK Gateway (v4) language-model client.
// It speaks the public AI SDK provider specification v4 directly — no OpenAI
// SDK, no AI SDK. The request body is the v4 `LanguageModelV4CallOptions`
// object; the response is a v4 stream-part SSE stream. The Vercel AI Gateway,
// this platform's ai-multimodal-gateway, and any v4 upstream are the same wire
// with a different base URL.
//
// Base URL: the gateway's v4 mount, e.g.
// "http://ai-multimodal-gateway.worker.svc.cluster.local/v4/ai". The client
// POSTs to "<base>/language-model".
type Client struct {
	baseURL string
	apiKey  string
	headers map[string]string
	http    *http.Client
}

// APIError is an error from the gateway (a non-2xx response) or a transport
// failure surfaced with a status. It carries what the retry loop needs.
type APIError struct {
	StatusCode int
	Body       string
	Header     http.Header
	Message    string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("llm: %s (status %d)", e.Message, e.StatusCode)
	}
	return fmt.Sprintf("llm: request failed (status %d)", e.StatusCode)
}

// New builds a client for one provider/gateway.
func New(creds ProviderCredentials, httpClient *http.Client) *Client {
	hc := httpClient
	if hc == nil {
		hc = &http.Client{}
	}
	return &Client{
		baseURL: strings.TrimRight(creds.BaseURL, "/"),
		apiKey:  creds.APIKey,
		headers: creds.Headers,
		http:    hc,
	}
}

// Provider reports the provider id this client is bound to.
func (c *Client) Provider() string { return c.apiKey }

// Stream opens ONE v4 streaming call and returns normalized events. A build
// error (empty model) is returned synchronously; HTTP/stream errors surface as
// an EventError in the channel (so RetryingStream can classify + retry them).
func (c *Client) Stream(ctx context.Context, req ChatRequest) (<-chan Event, error) {
	if req.Model == "" {
		return nil, fmt.Errorf("llm: empty model")
	}
	body, err := buildV4Body(req)
	if err != nil {
		return nil, err
	}
	out := make(chan Event, 16)
	go func() {
		defer close(out)
		resp, err := c.post(ctx, req.Model, body)
		if err != nil {
			send(out, ctx, Event{Type: EventError, Err: err})
			return
		}
		defer resp.Body.Close()
		c.decode(ctx, resp.Body, out)
	}()
	return out, nil
}

func (c *Client) post(ctx context.Context, model string, body map[string]any) (*http.Response, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/language-model", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("ai-language-model-id", model)
	httpReq.Header.Set("ai-language-model-streaming", "true")
	httpReq.Header.Set("ai-language-model-specification-version", "4")
	for k, v := range c.headers {
		httpReq.Header.Set(k, v)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		_ = resp.Body.Close()
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body:       string(raw),
			Message:    extractErrMessage(raw),
		}
	}
	return resp, nil
}

// v4Part is one stream part (only the fields the agent consumes).
type v4Part struct {
	Type         string          `json:"type"`
	ID           string          `json:"id"`
	Delta        string          `json:"delta"`
	ToolCallID   string          `json:"toolCallId"`
	ToolName     string          `json:"toolName"`
	Input        json.RawMessage `json:"input"`
	FinishReason struct {
		Unified string `json:"unified"`
		Raw     string `json:"raw"`
	} `json:"finishReason"`
	Usage struct {
		InputTokens struct {
			Total *int64 `json:"total"`
		} `json:"inputTokens"`
		OutputTokens struct {
			Total *int64 `json:"total"`
		} `json:"outputTokens"`
	} `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// decode reads the SSE stream, normalizes v4 parts into Events, and stops at a
// terminal `finish` / `error` part.
func (c *Client) decode(ctx context.Context, r io.Reader, out chan<- Event) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			return
		}
		var p v4Part
		if err := json.Unmarshal([]byte(data), &p); err != nil {
			continue // tolerate unknown/partial frames
		}
		switch p.Type {
		case "text-delta":
			if p.Delta != "" && !send(out, ctx, Event{Type: EventTextDelta, Text: p.Delta}) {
				return
			}
		case "reasoning-delta":
			if p.Delta != "" && !send(out, ctx, Event{Type: EventReasoningDelta, Text: p.Delta}) {
				return
			}
		case "tool-call":
			tc := &ToolCall{ID: p.ToolCallID, Name: p.ToolName, Args: inputToString(p.Input)}
			if !send(out, ctx, Event{Type: EventToolCall, ToolCall: tc}) {
				return
			}
		case "tool-input-start":
			if !send(out, ctx, Event{Type: EventToolInputStart, ToolCall: &ToolCall{ID: p.ID, Name: p.ToolName}}) {
				return
			}
		case "tool-input-delta":
			if !send(out, ctx, Event{Type: EventToolInputDelta, Text: p.Delta, ToolCall: &ToolCall{ID: p.ID}}) {
				return
			}
		case "tool-input-end":
			if !send(out, ctx, Event{Type: EventToolInputEnd, ToolCall: &ToolCall{ID: p.ID}}) {
				return
			}
		case "finish":
			send(out, ctx, Event{Type: EventFinishStep, FinishReason: p.FinishReason.Unified, Usage: usageOf(p)})
			return
		case "error":
			send(out, ctx, Event{Type: EventError, Err: &APIError{Message: errMessage(p.Error)}})
			return
		}
	}
	if err := sc.Err(); err != nil {
		send(out, ctx, Event{Type: EventError, Err: err})
	}
}

func usageOf(p v4Part) *Usage {
	if p.Usage.InputTokens.Total == nil && p.Usage.OutputTokens.Total == nil {
		return nil
	}
	var u Usage
	if p.Usage.InputTokens.Total != nil {
		u.InputTokens = *p.Usage.InputTokens.Total
	}
	if p.Usage.OutputTokens.Total != nil {
		u.OutputTokens = *p.Usage.OutputTokens.Total
	}
	return &u
}

// inputToString renders a stream tool-call `input`: it is a JSON string in the
// stream spec, but tolerate an inline object.
func inputToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func errMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "stream error"
	}
	var m struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &m) == nil && m.Message != "" {
		return m.Message
	}
	return string(raw)
}

func extractErrMessage(body []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return string(body)
}

// send delivers one event, aborting on ctx cancellation. Returns false when
// the consumer is gone.
func send(ch chan<- Event, ctx context.Context, ev Event) bool {
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}
