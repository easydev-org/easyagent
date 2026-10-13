package v4mock_test

import (
	"context"
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/llm/v4mock"
)

func TestMockLanguageStream(t *testing.T) {
	g := v4mock.New()
	defer g.Close()
	c := llm.New(llm.ProviderCredentials{BaseURL: g.BaseURL(), APIKey: "k"}, nil)

	ch, err := c.Stream(context.Background(), llm.ChatRequest{
		Model:    "mock/echo",
		System:   "You are helpful.",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var finish string
	var in, out int64
	for ev := range ch {
		switch ev.Type {
		case llm.EventTextDelta:
			text += ev.Text
		case llm.EventFinishStep:
			finish = ev.FinishReason
			if ev.Usage != nil {
				in, out = ev.Usage.InputTokens, ev.Usage.OutputTokens
			}
		case llm.EventError:
			t.Fatalf("stream error: %v", ev.Err)
		}
	}
	if text != "echo: hello" {
		t.Errorf("text = %q", text)
	}
	if finish != "stop" {
		t.Errorf("finish = %q", finish)
	}
	if in != 10 || out != 5 {
		t.Errorf("usage = %d/%d", in, out)
	}
}

func TestMockToolCallThenResult(t *testing.T) {
	g := v4mock.New()
	defer g.Close()
	c := llm.New(llm.ProviderCredentials{BaseURL: g.BaseURL(), APIKey: "k"}, nil)
	tools := []llm.ToolDef{{Name: "echo", Description: "echo"}}

	ch, _ := c.Stream(context.Background(), llm.ChatRequest{
		Model: "mock/echo", Tools: tools,
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "TOOL echo"}},
	})
	var call *llm.ToolCall
	var finish string
	for ev := range ch {
		if ev.Type == llm.EventToolCall {
			call = ev.ToolCall
		}
		if ev.Type == llm.EventFinishStep {
			finish = ev.FinishReason
		}
	}
	if call == nil || call.Name != "echo" {
		t.Fatalf("tool call = %+v", call)
	}
	if finish != "tool-calls" {
		t.Errorf("finish = %q", finish)
	}

	// Second step: feed the tool result back; the mock answers deterministically.
	ch2, _ := c.Stream(context.Background(), llm.ChatRequest{
		Model: "mock/echo", Tools: tools,
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: "TOOL echo"},
			{Role: llm.RoleAssistant, ToolCalls: []llm.ToolCall{*call}},
			{Role: llm.RoleTool, ToolCallID: call.ID, Name: call.Name, Content: "ok"},
		},
	})
	var text string
	for ev := range ch2 {
		if ev.Type == llm.EventTextDelta {
			text += ev.Text
		}
	}
	if !strings.Contains(text, "tool result received") {
		t.Errorf("second step text = %q", text)
	}
}

func TestMockGenerationEndpoints(t *testing.T) {
	g := v4mock.New()
	defer g.Close()
	c := llm.New(llm.ProviderCredentials{BaseURL: g.BaseURL(), APIKey: "k"}, nil)
	ctx := context.Background()

	imgs, err := c.GenerateImage(ctx, "m", "a cat", 1, "", nil)
	if err != nil || len(imgs) != 1 || len(imgs[0].Data) == 0 {
		t.Fatalf("image: %v %d", err, len(imgs))
	}
	speech, err := c.GenerateSpeech(ctx, "m", "hi", "", "")
	if err != nil || string(speech.Data) != "MOCKAUDIO" {
		t.Fatalf("speech: %v %q", err, speech.Data)
	}
	text, err := c.Transcribe(ctx, "m", []byte("x"), "audio/wav")
	if err != nil || text != "mock transcript" {
		t.Fatalf("transcribe: %v %q", err, text)
	}
	vids, err := c.GenerateVideo(ctx, "m", map[string]any{"prompt": "clip"})
	if err != nil || len(vids) != 1 || string(vids[0].Data) != "MOCKVIDEO" {
		t.Fatalf("video: %v %d", err, len(vids))
	}
	vision, err := c.Vision(ctx, "m", "describe", []byte{1, 2, 3}, "image/png")
	if err != nil || !strings.HasPrefix(vision, "echo:") {
		t.Fatalf("vision: %v %q", err, vision)
	}
}
