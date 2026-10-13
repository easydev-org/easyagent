package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// recorder captures the emitted event stream.
type recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *recorder) Emit(_ context.Context, ev Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return nil
}

func (r *recorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		out[i] = e.Name
	}
	return out
}

func (r *recorder) first(name string) (Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if e.Name == name {
			return e, true
		}
	}
	return Event{}, false
}

// sseServer replies with the given v4 SSE parts (a fresh body per call).
func sseServer(t *testing.T, bodies ...[]string) *httptest.Server {
	t.Helper()
	var i int
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		var parts []string
		if i < len(bodies) {
			parts = bodies[i]
		}
		i++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, p := range parts {
			fmt.Fprintf(w, "data: %s\n\n", p)
			if fl != nil {
				fl.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

// v4 frame builders.
func textDelta(s string) string {
	return `{"type":"text-delta","id":"t","delta":"` + s + `"}`
}
func reasoningDelta(s string) string {
	return `{"type":"reasoning-delta","id":"r","delta":"` + s + `"}`
}
func toolCall(id, name, input string) string {
	return `{"type":"tool-call","toolCallId":"` + id + `","toolName":"` + name + `","input":"` + input + `"}`
}
func finish(reason string) string {
	return `{"type":"finish","finishReason":{"unified":"` + reason + `","raw":"` + reason + `"}}`
}
func finishUsage(reason string, in, out int) string {
	return fmt.Sprintf(`{"type":"finish","finishReason":{"unified":"%s"},"usage":{"inputTokens":{"total":%d},"outputTokens":{"total":%d}}}`, reason, in, out)
}

// stubTools records executed calls and returns a canned outcome.
type stubTools struct {
	calls []string
	out   ToolOutcome
	err   error
}

func (s *stubTools) Execute(_ context.Context, _, _, name, args string) (ToolOutcome, error) {
	s.calls = append(s.calls, name+"("+args+")")
	return s.out, s.err
}

func newClient(t *testing.T, url string) *llm.Client {
	t.Helper()
	return llm.New(llm.ProviderCredentials{BaseURL: url, APIKey: "k"}, nil)
}

func TestTurnTextOnly(t *testing.T) {
	srv := sseServer(t, []string{
		textDelta("Hel"),
		textDelta("lo"),
		finishUsage("stop", 3, 2),
	})
	defer srv.Close()

	rec := &recorder{}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 3, Tools: &stubTools{},
	}, Input{Model: "m", RunID: "run-1", History: []llm.Message{{Role: llm.RoleUser, Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "Hello" || res.Reason != "stop" || res.Steps != 1 {
		t.Errorf("res = %+v", res)
	}
	got := rec.names()
	want := []string{"message-added", "start-step", "text-delta", "text-delta", "finish-step", "message-added", "finish", "turn-complete"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("events = %v\nwant   %v", got, want)
	}
	ma, _ := rec.first("message-added")
	if ma.Params["message_id"] == "" || ma.Params["streaming"] != true {
		t.Errorf("message-added = %v", ma.Params)
	}
	if res.InputTokens != 3 || res.OutputTokens != 2 {
		t.Errorf("usage = %d/%d", res.InputTokens, res.OutputTokens)
	}
}

func TestTurnToolLoop(t *testing.T) {
	srv := sseServer(t,
		[]string{toolCall("call_1", "todo-write", `{\"todos\":[]}`), finish("tool-calls")},
		[]string{textDelta("done"), finish("stop")},
	)
	defer srv.Close()

	rec := &recorder{}
	tools := &stubTools{out: ToolOutcome{Content: "ok", Metadata: map[string]any{"change_id": "c9"}}}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 5, Tools: tools,
	}, Input{
		Model: "m", RunID: "run-2",
		History: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		Tools:   []llm.ToolDef{{Name: "todo-write"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Steps != 2 || res.Reason != "stop" || res.Text != "done" {
		t.Errorf("res = %+v", res)
	}
	if len(tools.calls) != 1 || tools.calls[0] != `todo-write({"todos":[]})` {
		t.Errorf("tool calls = %v", tools.calls)
	}
	tr, ok := rec.first("tool-result")
	if !ok {
		t.Fatal("no tool-result emitted")
	}
	if tr.Params["toolCallId"] != "call_1" || tr.Params["formatted"] != "ok" || tr.Params["change_id"] != "c9" {
		t.Errorf("tool-result params = %v", tr.Params)
	}
	var added int
	for _, e := range rec.events {
		if e.Name == "message-added" {
			added++
		}
	}
	if added != 4 {
		t.Errorf("message-added count = %d, want 4", added)
	}
}

func TestTurnToolErrorPairsWithCall(t *testing.T) {
	srv := sseServer(t,
		[]string{toolCall("call_x", "boom", "{}"), finish("tool-calls")},
		[]string{textDelta("recovered"), finish("stop")},
	)
	defer srv.Close()

	rec := &recorder{}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 5,
		Tools: &stubTools{err: fmt.Errorf("kaboom")},
	}, Input{Model: "m", RunID: "r", History: []llm.Message{{Role: llm.RoleUser, Content: "go"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Reason != "stop" {
		t.Errorf("res = %+v", res)
	}
	te, ok := rec.first("tool-error")
	if !ok || te.Params["toolCallId"] != "call_x" {
		t.Errorf("tool-error = %v", te.Params)
	}
}

func TestTurnMaxSteps(t *testing.T) {
	srv := sseServer(t, []string{toolCall("c", "t", "{}"), finish("tool-calls")})
	defer srv.Close()

	rec := &recorder{}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 1, Tools: &stubTools{out: ToolOutcome{Content: "ok"}},
	}, Input{Model: "m", RunID: "r", History: []llm.Message{{Role: llm.RoleUser, Content: "go"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Reason != "max-steps" {
		t.Errorf("res.Reason = %q, want max-steps", res.Reason)
	}
}

func TestTurnRetryEmitsSameMessageID(t *testing.T) {
	var n int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		attempt := n
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"rate limited","type":"rate_limit_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", textDelta("ok"))
		fmt.Fprintf(w, "data: %s\n\n", finish("stop"))
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	rec := &recorder{}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 1,
		Retry: llm.RetryPolicy{MaxRetries: 2, InitialDelay: time.Millisecond},
		Tools: &stubTools{},
	}, Input{Model: "m", RunID: "r", History: []llm.Message{{Role: llm.RoleUser, Content: "go"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Text != "ok" {
		t.Errorf("res = %+v", res)
	}
	rt, ok := rec.first("retry")
	if !ok {
		t.Fatal("no retry event")
	}
	ma, _ := rec.first("message-added")
	if rt.Params["message_id"] != ma.Params["message_id"] {
		t.Errorf("retry message_id %v != message-added %v", rt.Params["message_id"], ma.Params["message_id"])
	}
}
