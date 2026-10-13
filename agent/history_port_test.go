package agent

import (
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// Ported from the reference's history / append-step tests: rebuilding the
// model conversation from the persisted chain must pair each assistant
// tool-call with a following `tool` result (the AI SDK rejects a dangling
// call), keep text, and exclude display-only reasoning.

func TestRebuildHistoryToolCallResultPairing(t *testing.T) {
	var msgs []store.Message
	var parts []store.Part

	// user prompt
	msgs = append(msgs, store.Message{ID: "u1", Role: "user"})
	parts = append(parts, store.Part{MessageID: "u1", Type: "text", Seq: 0, Data: `{"text":"hi"}`})

	// assistant step: text + one tool call
	msgs = append(msgs, store.Message{ID: "a1", Role: "assistant"})
	parts = append(parts,
		store.Part{MessageID: "a1", Type: "text", Seq: 0, Data: `{"text":"let me check"}`},
		store.Part{MessageID: "a1", Type: "tool", Seq: 1, Data: `{"id":"c1","name":"echo","input":{"x":1}}`},
		store.Part{MessageID: "a1", Type: "tool_result", Seq: 2, Data: `{"tool_use_id":"c1","content":"pong"}`},
		store.Part{MessageID: "a1", Type: "reasoning", Seq: 3, Data: `{"text":"hidden"}`},
	)

	got := rebuildHistory(msgs, parts)
	// Expect: user("hi"), assistant("let me check" + tool call c1), tool(c1, "pong").
	if len(got) != 3 {
		t.Fatalf("history = %d messages, want 3: %+v", len(got), got)
	}
	if got[0].Role != llm.RoleUser || got[0].Content != "hi" {
		t.Fatalf("msg0 = %+v", got[0])
	}
	if got[1].Role != llm.RoleAssistant || got[1].Content != "let me check" {
		t.Fatalf("msg1 = %+v", got[1])
	}
	if len(got[1].ToolCalls) != 1 || got[1].ToolCalls[0].ID != "c1" || got[1].ToolCalls[0].Name != "echo" {
		t.Fatalf("msg1 tool calls = %+v", got[1].ToolCalls)
	}
	if got[2].Role != llm.RoleTool || got[2].ToolCallID != "c1" || got[2].Content != "pong" {
		t.Fatalf("msg2 (tool result) = %+v", got[2])
	}
	// Reasoning must NOT leak into the model conversation.
	for _, m := range got {
		if m.Content == "hidden" {
			t.Fatal("reasoning part leaked into history")
		}
	}
}

// An assistant step with only a tool_result and no text/call is skipped
// (nothing to send), and an assistant step with no parts is dropped.
func TestRebuildHistorySkipsEmptyAssistantSteps(t *testing.T) {
	msgs := []store.Message{
		{ID: "u1", Role: "user"},
		{ID: "a1", Role: "assistant"}, // no parts at all
	}
	parts := []store.Part{{MessageID: "u1", Type: "text", Seq: 0, Data: `{"text":"hi"}`}}
	got := rebuildHistory(msgs, parts)
	if len(got) != 1 || got[0].Role != llm.RoleUser {
		t.Fatalf("history = %+v, want just the user message", got)
	}
}
