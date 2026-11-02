package server

import (
	"context"

	"connectrpc.com/connect/v2"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easybus-sdk/job"
)

/**
 * ListJobs returns the background jobs one conversation started.
 *
 * READ FROM THE SHARED BUCKET, not from anything this process holds. A job belongs to the extension
 * running it; what the agent has is a view of the list every extension writes to. That is why this
 * survives a restart, and why it shows jobs started before this process booted.
 *
 * IT IS A SNAPSHOT, NOT A SUBSCRIPTION. A caller that wants the list to change under it should watch
 * the bucket — which is what the shell does — but a page that asks "what is running" once and offers
 * a refresh does not need a stream it has to tear down.
 */
func (s *Server) ListJobs(ctx context.Context, req *agentv1.ListJobsRequest) (*agentv1.ListJobsResponse, error) {
	tenant := tenantOf(ctx)
	session := req.GetId()
	if session == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session id is required")
	}
	// The session must exist: listing the jobs of a name nobody owns would answer "none" for a typo,
	// which reads as "that conversation started nothing".
	if _, ok, err := s.db.GetSession(tenant, session); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "no such session")
	}
	if s.runtimeBus == nil {
		// IN-MEMORY MODE THERE IS NO SHARED LIST, and an empty answer would be a lie about a
		// deployment that simply cannot see jobs.
		return nil, connect.NewError(connect.CodeUnavailable,
			"background jobs need a runtime bus; this deployment has none")
	}

	reg := job.NewRegistry(s.runtimeBus.Transport())
	ch, cancel, err := reg.Watch(ctx, tenant, session)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	defer cancel()

	out := make([]*agentv1.Job, 0, 8)
	for ev := range ch {
		// THE SNAPSHOT IS THE LIST. Live events describe changes that a later call will see, and
		// waiting for them would make this call never return on a busy bucket.
		if ev.SnapshotDone {
			break
		}
		if ev.Deleted || !ev.HasValue {
			continue
		}
		j := ev.Job
		out = append(out, &agentv1.Job{
			Id:         j.ID,
			Kind:       j.Kind,
			Title:      j.Title,
			State:      string(j.State),
			ExitCode:   exitCodePtr(j.ExitCode),
			StartedAt:  j.StartedAt,
			FinishedAt: j.FinishedAt,
			CancelMode: string(j.CancelMode),
			Reason:     j.Reason,
		})
	}
	return &agentv1.ListJobsResponse{Jobs: out}, nil
}

// exitCodePtr converts an optional code, keeping "no code" distinct from zero.
//
// A CANCELLED OR LOST JOB HAS NO EXIT CODE, and reporting 0 for one would render as a successful
// finish — the one thing a cancelled job is not.
func exitCodePtr(v *int) *int32 {
	if v == nil {
		return nil
	}
	out := int32(*v)
	return &out
}
