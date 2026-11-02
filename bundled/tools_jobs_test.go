package bundled

import (
	"context"
	"strings"
	"testing"
	"time"
)

/**
 * THE TWO GENERIC JOB TOOLS, AND WHAT THEY REFUSE TO DO.
 *
 * The interesting cases are the ones where a naive implementation would invent an answer: an
 * unknown job, a timeout, a deployment with no job list. Each of those has a wrong reply that looks
 * right, and each is pinned here.
 */

// jobDeps is a stub whose job list the test owns.
type jobDeps struct {
	stubDeps
	rows []JobRow
	err  error
}

func (d *jobDeps) JobsInSession(string, string) ([]JobRow, error) { return d.rows, d.err }

func newJobExt(rows []JobRow) *Extension {
	return &Extension{deps: &jobDeps{rows: rows}}
}

func exit(v int) *int { return &v }

func TestJobListRendersEachStateWithoutInventingAnExit(t *testing.T) {
	e := newJobExt([]JobRow{
		{ID: "sandbox-job.a", Kind: "sandbox-job", Title: "cargo build", State: "running"},
		{ID: "sandbox-job.b", Kind: "sandbox-job", Title: "make", State: "done", ExitCode: exit(1)},
	})
	out, err := e.jobList(context.Background(), "t", "s", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	// A RUNNING JOB HAS NO EXIT CODE, and printing "exit 0" for it would read as a success.
	if strings.Contains(out.Content, "sandbox-job.a") && strings.Contains(out.Content, "exit 0") {
		t.Fatalf("a running job was given an exit code:\n%s", out.Content)
	}
	if !strings.Contains(out.Content, "exit 1") {
		t.Fatalf("a finished job lost its exit code:\n%s", out.Content)
	}
}

func TestJobListFiltersByKind(t *testing.T) {
	e := newJobExt([]JobRow{
		{ID: "sandbox-job.a", Kind: "sandbox-job", State: "running"},
		{ID: "ops.build.b", Kind: "ops-build", State: "running"},
	})
	out, _ := e.jobList(context.Background(), "t", "s", map[string]any{"kind": "ops-build"})
	if strings.Contains(out.Content, "sandbox-job.a") {
		t.Fatalf("a kind filter let another kind through:\n%s", out.Content)
	}
	if !strings.Contains(out.Content, "ops.build.b") {
		t.Fatalf("a kind filter dropped the kind asked for:\n%s", out.Content)
	}
}

func TestJobListSaysSoWhenThereAreNone(t *testing.T) {
	e := newJobExt(nil)
	out, _ := e.jobList(context.Background(), "t", "s", map[string]any{})
	// AN EMPTY LIST MUST BE A SENTENCE. An empty body reads as a broken tool.
	if strings.TrimSpace(out.Content) == "" {
		t.Fatal("an empty job list rendered nothing at all")
	}
	if !strings.Contains(strings.ToLower(out.Content), "no ") {
		t.Fatalf("an empty list should say so:\n%s", out.Content)
	}
}

/**
 * A MISSING JOB IS ANSWERED, NOT WAITED FOR.
 *
 * It may have aged out of the bucket, or the id may be wrong. Blocking for the full cap to arrive at
 * the same answer would waste the caller's turn — and a waiter that hung for a minute on a typo
 * looks like a hung turn.
 */
func TestJobWaitReturnsImmediatelyForAnUnknownJob(t *testing.T) {
	e := newJobExt(nil)
	start := time.Now()
	out, err := e.jobWait(context.Background(), "t", "s", map[string]any{"job-id": "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("an unknown job was waited on for %s", elapsed)
	}
	if !strings.Contains(out.Content, "nope") {
		t.Fatalf("the reply does not name the job asked about:\n%s", out.Content)
	}
}

func TestJobWaitReturnsAtOnceForAFinishedJob(t *testing.T) {
	e := newJobExt([]JobRow{
		{ID: "sandbox-job.a", Kind: "sandbox-job", Title: "make", State: "done", ExitCode: exit(0)},
	})
	start := time.Now()
	out, _ := e.jobWait(context.Background(), "t", "s", map[string]any{"job-id": "sandbox-job.a"})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("a finished job was waited on for %s", elapsed)
	}
	if !strings.Contains(out.Content, "done") {
		t.Fatalf("the reply does not report the state:\n%s", out.Content)
	}
}

func TestJobWaitRequiresAnId(t *testing.T) {
	e := newJobExt(nil)
	out, _ := e.jobWait(context.Background(), "t", "s", map[string]any{})
	if !strings.Contains(strings.ToLower(out.Content), "job-id") {
		t.Fatalf("a missing id should be named as the problem:\n%s", out.Content)
	}
}

/**
 * A DEPLOYMENT WITHOUT A JOB LIST IS NOT AN EMPTY ONE.
 *
 * Both render as "nothing here" if the code returns a nil slice, and only one of them is a
 * misconfiguration. The tools report the difference.
 */
func TestJobListReportsAMissingSourceInsteadOfEmpty(t *testing.T) {
	// THE REAL DEP WITH NO FUNCTION WIRED, not the test stub: the stub answers nil,nil because that
	// is what a conversation with no jobs looks like, and the question here is what an UNWIRED
	// deployment looks like — which is StoreDeps with JobsFn unset.
	e := &Extension{deps: &StoreDeps{}}
	out, _ := e.jobList(context.Background(), "t", "s", map[string]any{})
	if strings.Contains(strings.ToLower(out.Content), "no background jobs") {
		t.Fatalf("a missing job source read as an empty list:\n%s", out.Content)
	}
	if !strings.Contains(strings.ToLower(out.Content), "not available") {
		t.Fatalf("a missing job source should be reported:\n%s", out.Content)
	}
}

func TestJobRowTerminality(t *testing.T) {
	// AN UNKNOWN STATE IS TERMINAL: waiting on a word a newer extension wrote would block forever.
	for _, s := range []string{"done", "cancelled", "lost", "something-new"} {
		if !(JobRow{State: s}).Terminal() {
			t.Errorf("%q should be terminal", s)
		}
	}
	if (JobRow{State: "running"}).Terminal() {
		t.Error("running must not be terminal, or job-wait would never block")
	}
}
