package server

import (
	"context"
	"sort"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easybus-sdk/denial"
)

/**
 * ListDenials returns the capabilities this session was refused in the last day.
 *
 * READ FROM THE SHARED BUCKET, so it survives a restart and shows refusals that happened on another
 * replica. The bucket's TTL is what makes it a reminder rather than a log.
 *
 * NEWEST FIRST, because the refusal a reader is about to ask about is the one that just happened —
 * a transcript reads top to bottom, and the panel should agree with it.
 */
func (s *Server) ListDenials(ctx context.Context, req *agentv1.ListDenialsRequest) (*agentv1.ListDenialsResponse, error) {
	tenant := tenantOf(ctx)
	session := req.GetId()
	if session == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session id is required")
	}
	if s.runtimeBus == nil {
		// IN-MEMORY MODE THERE IS NO SHARED LIST, and an empty answer would claim nobody was ever
		// refused — a different fact from "this deployment cannot see refusals".
		return nil, connect.NewError(connect.CodeUnavailable,
			"permission refusals need a runtime bus; this deployment has none")
	}

	recs, err := denial.NewRegistry(s.runtimeBus.Transport()).List(ctx, tenant, session)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].LastAt > recs[j].LastAt })

	out := make([]*agentv1.Denial, 0, len(recs))
	for _, r := range recs {
		out = append(out, &agentv1.Denial{
			ExtId:      r.ExtID,
			Capability: r.Capability,
			Tool:       r.Tool,
			FirstAt:    r.FirstAt,
			LastAt:     r.LastAt,
			Count:      int32(r.Count),
		})
	}
	return &agentv1.ListDenialsResponse{Denials: out}, nil
}
