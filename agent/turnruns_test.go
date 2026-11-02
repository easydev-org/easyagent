package agent

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easyagent/llm"
)

// The ordering guarantees the concurrent step introduced. These are the properties the OLD
// sequential loop provided, and every one of them still has to hold — a read-only optimisation that
// let a write overtake a read would be a correctness regression wearing a latency label.

func tool(name string) llm.ToolDef { return llm.ToolDef{Name: name} }
func roTool(name string) llm.ToolDef {
	return llm.ToolDef{Name: name, ReadOnly: true}
}
func call(id, name string) llm.ToolCall { return llm.ToolCall{ID: id, Name: name} }

// runSizes flattens a plan into the number of calls per run, which is the whole observable shape.
func runSizes(runs [][]llm.ToolCall) []int {
	out := make([]int, len(runs))
	for i, r := range runs {
		out[i] = len(r)
	}
	return out
}

func TestPlanToolRunsGroupsAdjacentReads(t *testing.T) {
	tools := []llm.ToolDef{roTool("read-a"), roTool("read-b"), tool("write"), roTool("read-c")}
	calls := []llm.ToolCall{
		call("1", "read-a"), call("2", "read-b"),
		call("3", "write"),
		call("4", "read-c"),
	}

	got := runSizes(planToolRuns(calls, tools))
	want := []int{2, 1, 1}
	if len(got) != len(want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// A WRITE NEVER SHARES A RUN, which is the guarantee the old loop made by running everything alone.
func TestPlanToolRunsNeverBatchesWrites(t *testing.T) {
	tools := []llm.ToolDef{tool("w1"), tool("w2"), roTool("r1")}
	calls := []llm.ToolCall{call("1", "w1"), call("2", "w2"), call("3", "r1")}

	for i, run := range planToolRuns(calls, tools) {
		if len(run) > 1 {
			t.Fatalf("run %d batched %d calls; writes must run alone: %+v", i, len(run), run)
		}
	}
}

// ADJACENT READS OVERLAP, BUT A READ AFTER A WRITE DOES NOT JOIN THE RUN BEFORE IT. This is the
// `git-file-read` then `git-file-patch` case: the patch must observe the read, so the read cannot be
// hoisted into a batch that the patch then waits behind in a different order.
func TestPlanToolRunsReadAfterWriteIsItsOwnRun(t *testing.T) {
	tools := []llm.ToolDef{roTool("read"), tool("write")}
	calls := []llm.ToolCall{
		call("1", "read"), call("2", "write"), call("3", "read"),
	}

	runs := planToolRuns(calls, tools)
	if got := runSizes(runs); len(got) != 3 {
		t.Fatalf("runs = %v, want three separate runs (read | write | read)", got)
	}
	if runs[0][0].Name != "read" || runs[1][0].Name != "write" || runs[2][0].Name != "read" {
		t.Fatalf("run order changed: %+v", runs)
	}
}

// An UNKNOWN TOOL is planned as a write, so it runs alone and its error is reported in order.
func TestPlanToolRunsUnknownToolRunsAlone(t *testing.T) {
	tools := []llm.ToolDef{roTool("read")}
	calls := []llm.ToolCall{call("1", "read"), call("2", "ghost"), call("3", "read")}

	runs := planToolRuns(calls, tools)
	if got := runSizes(runs); len(got) != 3 || got[1] != 1 {
		t.Fatalf("runs = %v, want read | ghost | read with ghost alone", got)
	}
}

// A RUN OF READS IS ACTUALLY CONCURRENT. Three 200ms reads must finish in roughly one, which is the
// entire point of the change; a sequential implementation would take three.
func TestRunToolCallsReadsAreConcurrent(t *testing.T) {
	const n = 3
	const delay = 120 * time.Millisecond

	tools := []llm.ToolDef{roTool("r1"), roTool("r2"), roTool("r3")}
	calls := []llm.ToolCall{call("1", "r1"), call("2", "r2"), call("3", "r3")}

	ex := &sleepyTools{delay: delay}
	deps := Deps{Tools: ex}

	start := time.Now()
	res := runToolCalls(context.Background(), deps, Input{}, "step", tools, calls)
	elapsed := time.Since(start)

	if len(res) != n {
		t.Fatalf("results = %d, want %d", len(res), n)
	}
	// One delay plus scheduling slack; anything near n*delay means they ran in sequence.
	if elapsed > 2*delay {
		t.Fatalf("3 reads took %v with a %v delay each — they ran sequentially, not concurrently", elapsed, delay)
	}
	// RESULTS STAY IN CALL ORDER even though completion order is arbitrary.
	for i, r := range res {
		if want := calls[i].ID; r.call.ID != want {
			t.Fatalf("result %d is call %q, want %q — results must stay in call order", i, r.call.ID, want)
		}
	}
}

// A SINGLE CALL DOES NOT SPAWN A GOROUTINE. The sequential path stays the sequential path, which
// keeps stack traces readable and the common case free.
func TestRunToolCallsSingleCallInlines(t *testing.T) {
	tools := []llm.ToolDef{tool("w")}
	calls := []llm.ToolCall{call("1", "w")}

	ex := &sleepyTools{delay: 0}
	res := runToolCalls(context.Background(), Deps{Tools: ex}, Input{}, "step", tools, calls)
	if len(res) != 1 || res[0].execErr != nil {
		t.Fatalf("single call did not run cleanly: %+v", res)
	}
	if got := ex.order(); len(got) != 1 || got[0] != "w" {
		t.Fatalf("executor saw %v, want [w]", got)
	}
}

// sleepyTools records the order calls STARTED and sleeps, so a test can tell sequence from overlap.
type sleepyTools struct {
	delay time.Duration
	mu    sync.Mutex
	begun []string
}

func (s *sleepyTools) Execute(_ context.Context, _, _, _, name, _ string) (ToolOutcome, error) {
	s.mu.Lock()
	s.begun = append(s.begun, name)
	s.mu.Unlock()
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return ToolOutcome{Content: name + " ok"}, nil
}

func (s *sleepyTools) order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.begun...)
	sort.Strings(out)
	return out
}
