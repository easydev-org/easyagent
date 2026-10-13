package llm

import (
	"encoding/json"
	"fmt"
)

// buildV4Body maps a ChatRequest onto the AI SDK v4 LanguageModelV4CallOptions
// object. The prompt is the v4 message/part format (NOT the user-facing shape).
func buildV4Body(req ChatRequest) (map[string]any, error) {
	prompt := make([]any, 0, len(req.Messages)+1)
	if req.System != "" {
		prompt = append(prompt, map[string]any{"role": "system", "content": req.System})
	}
	for _, m := range req.Messages {
		switch m.Role {
		case RoleUser, RoleSystem:
			prompt = append(prompt, map[string]any{
				"role":    "user",
				"content": []any{map[string]any{"type": "text", "text": m.Content}},
			})
		case RoleAssistant:
			parts := make([]any, 0, len(m.ToolCalls)+1)
			if m.Content != "" {
				parts = append(parts, map[string]any{"type": "text", "text": m.Content})
			}
			for _, tc := range m.ToolCalls {
				parts = append(parts, map[string]any{
					"type": "tool-call", "toolCallId": tc.ID,
					"toolName": tc.Name, "input": jsonArg(tc.Args),
				})
			}
			prompt = append(prompt, map[string]any{"role": "assistant", "content": parts})
		case RoleTool:
			prompt = append(prompt, map[string]any{
				"role": "tool",
				"content": []any{map[string]any{
					"type": "tool-result", "toolCallId": m.ToolCallID, "toolName": m.Name,
					"output": map[string]any{"type": "text", "value": m.Content},
				}},
			})
		default:
			return nil, fmt.Errorf("llm: unknown role %q", m.Role)
		}
	}
	body := map[string]any{"prompt": prompt}

	if len(req.Tools) > 0 {
		tools := make([]any, 0, len(req.Tools))
		for _, t := range req.Tools {
			tool := map[string]any{"type": "function", "name": t.Name}
			if t.Description != "" {
				tool["description"] = t.Description
			}
			if t.Parameters != nil {
				tool["inputSchema"] = t.Parameters
			}
			if t.Strict {
				tool["strict"] = true
			}
			tools = append(tools, tool)
		}
		body["tools"] = tools
	}

	switch req.ToolChoice {
	case "", "auto":
		// unset = auto
	case "required", "none":
		body["toolChoice"] = map[string]any{"type": req.ToolChoice}
	default:
		body["toolChoice"] = map[string]any{"type": "tool", "toolName": req.ToolChoice}
	}

	// Extra fields (providerOptions / reasoning variants) pass through verbatim.
	for k, v := range req.Extra {
		body[k] = v
	}
	return body, nil
}

// jsonArg parses a tool-call argument string into an object for the prompt
// (v4 prompt `tool-call.input` is the parsed value, not a string). Falls back
// to the raw string when it is not valid JSON.
func jsonArg(s string) any {
	var v any
	if json.Unmarshal([]byte(s), &v) == nil {
		return v
	}
	return s
}
