// Package llm is the agent's LLM layer.
//
// Scope (decided): ONE wire — the **AI SDK provider specification v4**
// (`/v4/ai/language-model`). The Vercel AI Gateway, this platform's
// ai-multimodal-gateway, and any v4 upstream are the same wire with a
// different base URL. There is no third-party SDK (no OpenAI SDK, no AI SDK);
// see v4.go for the client and prompt.go for the request mapping.
//
// This package owns exactly what the AI SDK's provider layer would have
// provided: the HTTP/SSE transport, a normalized event stream, a bounded +
// visible retry loop, and context-overflow detection. The step loop, tool
// execution, tool-result/tool-error events, abort and approvals live in the
// turn loop on top of this package (they are SDK-core concerns, not provider).
package llm

// Role is a chat message role.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolCall is a fully-assembled tool call emitted by the model. Args is the raw
// JSON argument string exactly as streamed (the caller validates/parses it).
type ToolCall struct {
	ID   string
	Name string
	Args string
}

// ToolDef declares one tool offered to the model.
type ToolDef struct {
	Name        string
	Description string
	// Parameters is a JSON Schema object (map[string]any / json.RawMessage).
	Parameters any
	Strict     bool
}

// Message is one entry of the model conversation.
type Message struct {
	Role    Role
	Content string
	// ToolCalls is set on an assistant message that requested tools.
	ToolCalls []ToolCall
	// ToolCallID/Name are set on a tool result message.
	ToolCallID string
	Name       string
}

// ChatRequest is one model call (one turn-loop step).
type ChatRequest struct {
	Model    string
	System   string
	Messages []Message
	Tools    []ToolDef
	// ToolChoice is "auto" | "required" | "none" | "<tool-name>"; empty = auto.
	ToolChoice string
	// Extra is merged into the request body verbatim (providerOptions/variants
	// and request headers are the caller's concern; body keys land here).
	Extra map[string]any
}

// Usage is the token usage of one model call.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

// EventType is the normalized stream event name (the client-facing contract).
type EventType string

const (
	// EventTextDelta carries assistant text (Event.Text).
	EventTextDelta EventType = "text-delta"
	// EventReasoningDelta carries reasoning/thinking text (Event.Text),
	// normalized from DeepSeek `reasoning_content` / OpenRouter `reasoning`.
	EventReasoningDelta EventType = "reasoning-delta"
	// EventToolCall carries one fully-assembled tool call (Event.ToolCall).
	EventToolCall EventType = "tool-call"
	// EventToolInputStart/Delta/End carry streamed tool-input framing
	// (Event.Text = the delta; Event.ToolCall = {ID,Name} for start/end).
	EventToolInputStart EventType = "tool-input-start"
	EventToolInputDelta EventType = "tool-input-delta"
	EventToolInputEnd   EventType = "tool-input-end"
	// EventFinishStep terminates a step successfully (FinishReason + Usage).
	EventFinishStep EventType = "finish-step"
	// EventError terminates a step with a failure (Event.Err).
	EventError EventType = "error"
)

// Event is one normalized stream event.
type Event struct {
	Type EventType
	Text string
	// ToolCall is set for EventToolCall.
	ToolCall *ToolCall
	// FinishReason + Usage are set for EventFinishStep.
	FinishReason string
	Usage        *Usage
	// Err is set for EventError.
	Err error
}
