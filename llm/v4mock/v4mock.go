// Package v4mock is a deterministic, in-process AI-SDK v4 gateway used for
// tests (and for cross-implementation equivalence checks between the Go and TS
// agents). It speaks the exact wire protocol both clients expect: the
// `<base>/v4/ai/<modality>-model` endpoints, the AI-SDK v4 SSE stream parts, and
// the plain-JSON responses for the generation endpoints.
//
// Determinism: every response is derived ONLY from the request's prompt, so two
// different clients (Go and TS) that send the same conversation receive byte-
// identical model output.
package v4mock

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
)

// Gateway is a mock v4 gateway backed by an httptest server.
type Gateway struct {
	Server *httptest.Server
	// Calls counts requests per modality path (for assertions).
	Calls map[string]int
}

// New starts the mock gateway.
func New() *Gateway {
	g := &Gateway{Calls: map[string]int{}}
	g.Server = httptest.NewServer(HandlerFor(g))
	return g
}

// Handler returns the mock gateway's HTTP handler (mount points under /v4/ai).
func Handler() http.Handler { return HandlerFor(&Gateway{Calls: map[string]int{}}) }

// HandlerFor returns the handler bound to a specific Gateway (call counting).
func HandlerFor(g *Gateway) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v4/ai/language-model", g.language)
	mux.HandleFunc("/v4/ai/image-model", g.image)
	mux.HandleFunc("/v4/ai/speech-model", g.speech)
	mux.HandleFunc("/v4/ai/transcription-model", g.transcription)
	mux.HandleFunc("/v4/ai/video-model", g.video)
	return mux
}

// BaseURL is the gateway root (mount the client at `<BaseURL>/v4/ai`).
func (g *Gateway) BaseURL() string { return g.Server.URL + "/v4/ai" }

// Close shuts the server down.
func (g *Gateway) Close() { g.Server.Close() }

// ---- language model (SSE) ----

func (g *Gateway) language(w http.ResponseWriter, r *http.Request) {
	g.Calls["language-model"]++
	var body struct {
		Prompt []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"prompt"`
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	lastRole, lastText := lastUserText(body.Prompt)
	toolNames := make([]string, 0, len(body.Tools))
	for _, t := range body.Tools {
		toolNames = append(toolNames, t.Name)
	}
	// The AI SDK sets this header explicitly; absent = the generate (non-stream)
	// path (e.g. image-read / vision).
	streaming := r.Header.Get("ai-language-model-streaming") == "true"

	// Decide the model's output deterministically from the conversation.
	var text string
	var toolCall *toolCallOut
	switch {
	case lastRole == "tool":
		text = "tool result received"
	case strings.HasPrefix(lastText, "TOOL "):
		name := strings.TrimSpace(strings.TrimPrefix(lastText, "TOOL "))
		if contains(toolNames, name) {
			toolCall = &toolCallOut{ID: "call_" + name, Name: name, Input: map[string]any{}}
		} else {
			text = "no such tool: " + name
		}
	case strings.HasPrefix(lastText, "REASON "):
		text = "" // reasoning-only step
	default:
		text = "echo: " + lastText
	}

	// Non-streaming call (image-read / vision): return the plain JSON shape.
	if !streaming {
		writeJSON(w, map[string]any{"text": text})
		return
	}

	w.Header().Set("content-type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	writeSSE := func(v any) {
		b, _ := json.Marshal(v)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if fl != nil {
			fl.Flush()
		}
	}
	writeSSE(map[string]any{"type": "stream-start", "warnings": []any{}})
	if strings.HasPrefix(lastText, "REASON ") {
		writeSSE(map[string]any{"type": "reasoning-start", "id": "r1"})
		writeSSE(map[string]any{"type": "reasoning-delta", "id": "r1",
			"delta": strings.TrimSpace(strings.TrimPrefix(lastText, "REASON "))})
		writeSSE(map[string]any{"type": "reasoning-end", "id": "r1"})
	}
	if toolCall != nil {
		writeSSE(map[string]any{"type": "tool-input-start", "id": toolCall.ID, "toolName": toolCall.Name})
		writeSSE(map[string]any{"type": "tool-input-delta", "id": toolCall.ID, "delta": "{}"})
		writeSSE(map[string]any{"type": "tool-input-end", "id": toolCall.ID})
		writeSSE(map[string]any{"type": "tool-call", "toolCallId": toolCall.ID,
			"toolName": toolCall.Name, "input": toolCall.Input})
	} else if text != "" {
		writeSSE(map[string]any{"type": "text-start", "id": "t1"})
		writeSSE(map[string]any{"type": "text-delta", "id": "t1", "delta": text})
		writeSSE(map[string]any{"type": "text-end", "id": "t1"})
	}
	unified := "stop"
	if toolCall != nil {
		unified = "tool-calls"
	}
	writeSSE(map[string]any{
		"type":         "finish",
		"finishReason": map[string]any{"unified": unified},
		"usage": map[string]any{
			"inputTokens":  map[string]any{"total": 10},
			"outputTokens": map[string]any{"total": 5},
		},
	})
	fmt.Fprint(w, "data: [DONE]\n\n")
	if fl != nil {
		fl.Flush()
	}
}

type toolCallOut struct {
	ID    string
	Name  string
	Input map[string]any
}

// lastUserText returns the role + concatenated text of the final prompt message
// (a `tool` role means a tool result; its text is the tool output).
func lastUserText(prompt []struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}) (string, string) {
	for i := len(prompt) - 1; i >= 0; i-- {
		m := prompt[i]
		switch m.Role {
		case "user", "tool":
			var s string
			if json.Unmarshal(m.Content, &s) == nil {
				return m.Role, s
			}
			var parts []struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Value string `json:"value"`
			}
			if json.Unmarshal(m.Content, &parts) == nil {
				var b strings.Builder
				for _, p := range parts {
					if p.Text != "" {
						b.WriteString(p.Text)
					} else if p.Value != "" {
						b.WriteString(p.Value)
					}
				}
				return m.Role, b.String()
			}
		}
	}
	return "", ""
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ---- generation endpoints (JSON) ----

// 1x1 transparent PNG.
const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M8AAAMBAQDJ/pLvAAAAAElFTkSuQmCC"

func (g *Gateway) image(w http.ResponseWriter, r *http.Request) {
	g.Calls["image-model"]++
	writeJSON(w, map[string]any{"images": []string{pngB64}})
}

func (g *Gateway) speech(w http.ResponseWriter, r *http.Request) {
	g.Calls["speech-model"]++
	writeJSON(w, map[string]any{
		"audio":     base64.StdEncoding.EncodeToString([]byte("MOCKAUDIO")),
		"mediaType": "audio/mpeg",
	})
}

func (g *Gateway) transcription(w http.ResponseWriter, r *http.Request) {
	g.Calls["transcription-model"]++
	writeJSON(w, map[string]any{"text": "mock transcript"})
}

func (g *Gateway) video(w http.ResponseWriter, r *http.Request) {
	g.Calls["video-model"]++
	w.Header().Set("content-type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	b, _ := json.Marshal(map[string]any{
		"type": "result",
		"videos": []map[string]any{{
			"type":      "base64",
			"data":      base64.StdEncoding.EncodeToString([]byte("MOCKVIDEO")),
			"mediaType": "video/mp4",
		}},
	})
	fmt.Fprintf(w, "data: %s\n\n", b)
	fmt.Fprint(w, "data: [DONE]\n\n")
	if fl != nil {
		fl.Flush()
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
