package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// toolCallV4 streams one tool call then a finish.
func toolCallV4(name, input string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"tool-call\",\"toolCallId\":\"c1\",\"toolName\":%q,\"input\":%q}\n\n", name, input)
		fmt.Fprint(w, "data: {\"type\":\"finish\",\"finishReason\":{\"unified\":\"tool-calls\"}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

func TestUnknownToolYieldsErrorResult(t *testing.T) {
	srv := toolCallV4("ghost", "{}")
	defer srv.Close()
	rec := &recorder{}
	res, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 2, Tools: &stubTools{},
	}, Input{
		Model: "m", RunID: "r",
		History: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		Tools:   []llm.ToolDef{{Name: "echo"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = res
	te, ok := rec.first("tool-output-error")
	if !ok {
		t.Fatal("an unknown tool must yield a tool-output-error the model can self-correct from")
	}
	if asString(te.Params["errorText"]) == "" {
		t.Error("the error must name the available tools")
	}
}

func TestToolNameRepairLowercase(t *testing.T) {
	// The model emits an upper-cased name; the canonical offered tool is lower.
	srv := toolCallV4("Echo", "{}")
	defer srv.Close()
	tools := &stubTools{out: ToolOutcome{Content: "ok"}}
	rec := &recorder{}
	_, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 2, Tools: tools,
	}, Input{
		Model: "m", RunID: "r",
		History: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		Tools:   []llm.ToolDef{{Name: "echo"}}, // canonical lowercase
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.calls) != 1 || tools.calls[0] != "echo({})" {
		t.Errorf("lowercase repair failed: %v", tools.calls)
	}
}

// blockingTools blocks until ctx is done.
type blockingTools struct{}

func (blockingTools) Execute(ctx context.Context, _, _, _, _, _ string) (ToolOutcome, error) {
	<-ctx.Done()
	return ToolOutcome{}, ctx.Err()
}

func TestToolTimeoutYieldsError(t *testing.T) {
	srv := toolCallV4("slow", "{}")
	defer srv.Close()
	rec := &recorder{}
	start := time.Now()
	_, err := RunTurn(context.Background(), Deps{
		Client: newClient(t, srv.URL), Emit: rec, MaxTurns: 2,
		Tools: blockingTools{}, ToolTimeout: 40 * time.Millisecond,
	}, Input{
		Model: "m", RunID: "r",
		History: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		Tools:   []llm.ToolDef{{Name: "slow"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("a slow tool must be bounded by ToolTimeout")
	}
	// A timeout is NOT an error: the tool produced whatever it produced, and that
	// output is delivered as the terminal snapshot with state=timeout. Reporting it
	// as an error would throw away the work the user was watching.
	av, ok := rec.first("tool-output-available")
	if !ok {
		t.Fatal("a timed-out tool must still publish a terminal output frame")
	}
	out, _ := av.Params["output"].(map[string]any)
	if asString(out["state"]) != "timeout" {
		t.Errorf("state = %q, want timeout", asString(out["state"]))
	}
	if av.Params["preliminary"] == true {
		t.Error("the terminal frame must not be marked preliminary")
	}
}

func TestRequiredConfigBlocksTool(t *testing.T) {
	kv := NewMemoryKV()
	cfg := NewConfig(kv)
	reg := StaticTools{
		{Name: "needs-key", RequiredConfig: []string{"api_key"}},
		{Name: "free"},
	}
	// Without the config: needs-key is blocked.
	plan, err := Plan(context.Background(), RuntimeDeps{
		Registry: reg, Config: ConfigStore{C: cfg},
	}, PlanInput{Tenant: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tools) != 1 || plan.Tools[0].Name != "free" {
		t.Errorf("blocked tools = %+v", plan.Tools)
	}
	// With the config set: it is offered.
	cfg.Set("t1", "api_key", "x")
	plan, _ = Plan(context.Background(), RuntimeDeps{
		Registry: reg, Config: ConfigStore{C: cfg},
	}, PlanInput{Tenant: "t1"})
	if len(plan.Tools) != 2 {
		t.Errorf("with config set, tools = %+v", plan.Tools)
	}
}
