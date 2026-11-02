package bundled

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * JobRow is one background job as the tools see it.
 *
 * IT MIRRORS THE JOB ENTRY AN EXTENSION PUBLISHES, deliberately without importing that package:
 * bundled tools sit in the agent, and the agent should not take a dependency on an SDK that
 * extensions use to reach it. The cost is a struct that has to be kept in step; the benefit is that
 * a change to the extension SDK cannot force a rebuild of the agent.
 *
 * NO OUTPUT. A job's output lives wherever the work ran, and reading it is that extension's tool —
 * `sandbox-job-output`, and so on. A generic reader would have to route through the extension that
 * owns the job, which is a cross-process call these tools do not make.
 */
type JobRow struct {
	ID       string
	Kind     string
	Title    string
	State    string
	ExitCode *int
	StartedAt int64
	FinishedAt int64
	Reason   string
}

// Terminal reports whether a state is an ending, so a waiter knows when to stop asking.
//
// AN UNKNOWN STATE IS TERMINAL: waiting forever on a job written by a newer extension is worse than
// returning and letting the caller decide what to do with a word it does not recognise.
func (j JobRow) Terminal() bool {
	switch j.State {
	case "running":
		return false
	case "done", "cancelled", "lost":
		return true
	}
	return true
}

// jobWaitCap is how long one `job-wait` call will block.
//
// SIXTY SECONDS, because a tool call that blocks longer than a turn is a turn that looks hung. A
// caller that wants to wait longer calls again — which is also what lets it change its mind.
const jobWaitCap = 60 * time.Second

// jobPollEvery is how often the waiter re-reads the list.
//
// ONE SECOND: the states it waits for are produced by a process that writes them once, so a
// millisecond poll would be pure load, and a five-second one would make a job that finished just
// after the last check look stuck.
const jobPollEvery = time.Second

/**
 * jobList renders the jobs one conversation started, newest first.
 *
 * IT FILTERS BY KIND WHEN ASKED. A session may mix shell jobs from a sandbox with builds from an
 * ops extension, and a caller that wants "my builds" should not have to read past the rest.
 */
func (e *Extension) jobList(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	rows, err := e.deps.JobsInSession(tenant, session)
	if err != nil {
		return agent.ToolOutcome{Content: "cannot read the job list: " + err.Error()}, nil
	}
	kind := strings.TrimSpace(strArg(args, "kind"))
	rows = filterKind(rows, kind)

	if len(rows) == 0 {
		if kind != "" {
			return agent.ToolOutcome{Content: fmt.Sprintf("No %s jobs in this conversation.", kind)}, nil
		}
		return agent.ToolOutcome{Content: "No background jobs in this conversation."}, nil
	}
	return agent.ToolOutcome{
		Content: renderJobList(rows),
		Metadata: map[string]any{"jobs": rows},
	}, nil
}

/**
 * jobWait blocks until a job ends or the cap is reached.
 *
 * IT NEVER FAILS ON A TIMEOUT. Reaching the cap means "still running", which is an answer, and a
 * tool error would make a caller retry something that is working perfectly well. The reply says so
 * and the caller may ask again.
 */
func (e *Extension) jobWait(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	id := strings.TrimSpace(strArg(args, "job-id"))
	if id == "" {
		return agent.ToolOutcome{Content: "job-id is required."}, nil
	}

	deadline := time.Now().Add(jobWaitCap)
	for {
		row, found, err := findJob(e.deps, tenant, session, id)
		if err != nil {
			return agent.ToolOutcome{Content: "cannot read the job: " + err.Error()}, nil
		}
		/**
		 * A MISSING JOB IS NOT A WAIT. It may have aged out of the bucket — a running entry lapses in
		 * a minute if its owner stops renewing — or the id may be wrong. Either way there is nothing
		 * to wait for, and blocking until the cap would waste the caller's turn to arrive at the same
		 * answer.
		 */
		if !found {
			return agent.ToolOutcome{Content: fmt.Sprintf("No job %q in this conversation. It may have finished and aged out, or the id may be wrong.", id)}, nil
		}
		if row.Terminal() {
			return agent.ToolOutcome{
				Content: renderJobRow(row),
				Metadata: map[string]any{"job": row},
			}, nil
		}
		if time.Now().After(deadline) {
			// SAY WHAT IT IS, NOT WHAT IT IS NOT. "Still running" plus the elapsed time tells a
			// caller whether to wait again or move on.
			return agent.ToolOutcome{
				Content: fmt.Sprintf("Job %s is still running after %s.\nCall job-wait again to keep waiting, or continue with other work — a completion notice will arrive when it ends.",
					row.ID, jobWaitCap),
				Metadata: map[string]any{"job": row, "stillRunning": true},
			}, nil
		}
		select {
		case <-ctx.Done():
			// THE CALLER'S CONTEXT ENDING IS NOT THE JOB ENDING: report what is known rather than
			// inventing an outcome.
			return agent.ToolOutcome{Content: fmt.Sprintf("Stopped waiting for job %s.", row.ID)}, nil
		case <-time.After(jobPollEvery):
		}
	}
}

// findJob looks one job up by id within the conversation.
func findJob(deps Deps, tenant, session, id string) (JobRow, bool, error) {
	rows, err := deps.JobsInSession(tenant, session)
	if err != nil {
		return JobRow{}, false, err
	}
	for _, r := range rows {
		if r.ID == id {
			return r, true, nil
		}
	}
	return JobRow{}, false, nil
}

// filterKind keeps the rows of one kind, or all of them when kind is empty.
func filterKind(rows []JobRow, kind string) []JobRow {
	if kind == "" {
		return rows
	}
	out := make([]JobRow, 0, len(rows))
	for _, r := range rows {
		if strings.EqualFold(r.Kind, kind) {
			out = append(out, r)
		}
	}
	return out
}

/**
 * renderJobList writes one line per job.
 *
 * NEWEST FIRST, because the job a caller just started is the one being asked about, and a session
 * that has run fifty builds should not put the interesting one at the bottom.
 */
func renderJobList(rows []JobRow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d background job(s):\n", len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		b.WriteString(renderJobRow(rows[i]))
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// renderJobRow renders one job as a single line plus its title.
//
// THE EXIT CODE IS SHOWN ONLY WHEN THERE IS ONE. A running job has none, and printing "exit=0" for
// it would read as a successful finish.
func renderJobRow(r JobRow) string {
	state := r.State
	if r.ExitCode != nil {
		state = fmt.Sprintf("%s, exit %d", state, *r.ExitCode)
	}
	line := fmt.Sprintf("- %s [%s] %s", r.ID, state, r.Title)
	if r.Reason != "" {
		line += " — " + r.Reason
	}
	return line
}
