package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

/**
 * TOOL PROGRESS MUST NEVER REACH THE MODEL.
 *
 * This is a CONTRACT, not an implementation detail, and it is the reason `ToolProgress` exists as a
 * separate stream from the tool's result:
 *
 *   - a job that prints ten thousand lines costs the transcript nothing while it runs;
 *   - the model is never asked to reason about a half-finished command, which is how a turn
 *     concludes something about work that has not happened yet.
 *
 * The frames DO go to the session's event stream — a client renders them live and a reconnect
 * replays them — so the only thing to prove here is that none of them is in the request body the
 * provider receives.
 *
 * ASSERTED ON THE SECOND REQUEST, because that is the one carrying the tool result: if progress had
 * leaked into the model's context, then is when it would appear.
 */
func TestToolProgressNeverReachesTheModel(t *testing.T) {
	var mu sync.Mutex
	var bodies []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		raw, _ := json.Marshal(body)
		mu.Lock()
		bodies = append(bodies, string(raw))
		first := len(bodies) == 1
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		if first {
			// One tool call, so the second request carries its result.
			for _, line := range []string{
				toolCall("call_1", "sandbox-job-start", `{}`),
				finish("tool-calls"),
			} {
				fmtWrite(w, line)
			}
		} else {
			for _, line := range []string{textDelta("done"), finish("stop")} {
				fmtWrite(w, line)
			}
		}
		if fl != nil {
			fl.Flush()
		}
	}))
	defer srv.Close()

	// A tool that reports a distinctive progress line before returning.
	tools := &progressingTools{line: "SECRET-PROGRESS-MARKER"}

	deps := Deps{
		Client: newClient(t, srv.URL), Emit: &captureEmitter{}, MaxTurns: 3, Tools: tools,
		ToolProgress: func(ctx context.Context, _, _ string) (<-chan ToolProgressUpdate, func(), error) {
			ch := make(chan ToolProgressUpdate, 4)
			ch <- ToolProgressUpdate{CallID: "call_1", Text: "SECRET-PROGRESS-MARKER"}
			close(ch)
			return ch, func() {}, nil
		},
	}

	_, err := RunTurn(context.Background(), deps, Input{
		Model: "m", RunID: "run-1", Session: "s1", Tenant: "t1",
		History: []llm.Message{{Role: llm.RoleUser, Content: "go"}},
		Tools:   []llm.ToolDef{{Name: "sandbox-job-start"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) < 2 {
		t.Fatalf("expected at least two provider requests, got %d", len(bodies))
	}
	for i, body := range bodies {
		if strings.Contains(body, "SECRET-PROGRESS-MARKER") {
			t.Fatalf("request %d carried tool progress into the model's context", i+1)
		}
	}
}

// progressingTools returns a result immediately; its progress is supplied by the test's
// ToolProgress function, which is the seam the turn reads.
type progressingTools struct{ line string }

func (p *progressingTools) Execute(context.Context, string, string, string, string, string) (ToolOutcome, error) {
	return ToolOutcome{Content: "finished"}, nil
}

// fmtWrite emits one SSE data line.
func fmtWrite(w http.ResponseWriter, payload string) {
	_, _ = w.Write([]byte("data: " + payload + "\n\n"))
}

// TestToolProgressIsRelayedToTheEventStream is the other half of the contract: the frames must
// appear on the CLIENT stream, or "keep it out of context" would be indistinguishable from
// "drop it entirely".
func TestToolProgressIsRelayedToTheEventStream(t *testing.T) {
	em := &captureEmitter{}
	deps := Deps{
		Emit:  em,
		Tools: &progressingTools{},
		ToolProgress: func(ctx context.Context, _, _ string) (<-chan ToolProgressUpdate, func(), error) {
			// A FULL BATCH, not two lines: the relay flushes on a batch bound and otherwise on a
			// 200ms ticker, so a short stream would make this test a race against the clock.
			ch := make(chan ToolProgressUpdate, liveFlushLines)
			for i := 0; i < liveFlushLines; i++ {
				ch <- ToolProgressUpdate{CallID: "call_1", Phase: "running", Text: "line"}
			}
			close(ch)
			return ch, func() {}, nil
		},
	}
	stop := relayInto(context.Background(), deps,
		Input{Tenant: "t1", Session: "s1", RunID: "r1"},
		"call_1", "step-1", "sandbox-job-start", newResultStream())
	time.Sleep(120 * time.Millisecond)
	stop()

	var sawOutput bool
	for _, ev := range em.snapshot() {
		if ev.Name == "tool-output-available" {
			sawOutput = true
		}
	}
	if !sawOutput {
		t.Fatal("tool progress produced no client event; keeping it out of context must not mean dropping it")
	}
}
