package server

import (
	"context"
	"sort"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easybus-sdk/denial"
)

/**
 * ListPermissionStates joins what a session HOLDS with what it was REFUSED.
 *
 * THE TWO HALVES BELONG IN DIFFERENT STORES: a grant is permanent and relational, a refusal is a
 * twenty-four hour reminder with a TTL. That split is not the problem. The problem was making every
 * client do the join — the panel called two RPCs and matched rows by (extension, capability) by hand,
 * and a second client would have had to rediscover that they are one question asked twice.
 *
 * ONE ROW PER (extension, capability), and the two sides do not overlap in practice: granting clears the
 * refusal, so a capability that is held has nothing to complain about and one that was refused is not
 * held. Where they DO overlap — a grant from before the refusal, or a grant that raced the clear — the
 * grant wins: the reader can act on "you have this", and a stale complaint beside it would be telling
 * them to fix something already fixed.
 */
func (s *Server) ListPermissionStates(ctx context.Context, req *agentv1.ListPermissionStatesRequest) (*agentv1.ListPermissionStatesResponse, error) {
	tenant := tenantOf(ctx)
	session := req.GetId()
	if session == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session id is required")
	}

	grants, err := s.db.ListGrants(tenant, session)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	/**
	 * A FAILED REFUSAL READ IS NOT A FAILED PERMISSION LIST.
	 *
	 * The grants are the answer to "what may this session do", and a deployment without a runtime bus can
	 * answer that perfectly well while having no refusals to report. Failing the whole call would take
	 * away the list a reader can act on because of a section they could not have acted on anyway.
	 */
	var refusals []denial.Record
	if s.runtimeBus != nil {
		refusals, _ = denial.NewRegistry(s.runtimeBus.Transport()).List(ctx, tenant, session)
	}

	type key struct{ ext, cap string }
	rows := make(map[key]*agentv1.PermissionState, len(grants)+len(refusals))
	for _, g := range grants {
		k := key{g.ExtID, g.Capability}
		rows[k] = &agentv1.PermissionState{
			ExtId: g.ExtID, Capability: g.Capability, Granted: grantToMsg(g),
		}
	}
	for _, r := range refusals {
		k := key{r.ExtID, r.Capability}
		if _, ok := rows[k]; ok {
			// HELD WINS — see the note above.
			continue
		}
		rows[k] = &agentv1.PermissionState{
			ExtId: r.ExtID, Capability: r.Capability,
			Refused: &agentv1.Denial{
				ExtId: r.ExtID, Capability: r.Capability, Tool: r.Tool,
				FirstAt: r.FirstAt, LastAt: r.LastAt, Count: int32(r.Count),
			},
		}
	}

	out := make([]*agentv1.PermissionState, 0, len(rows))
	for _, st := range rows {
		out = append(out, st)
	}
	// REFUSALS FIRST, THEN BY EXTENSION: a reader arriving because something stopped working wants the
	// one row that explains it, not an alphabetical catalogue.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if (a.Refused != nil) != (b.Refused != nil) {
			return a.Refused != nil
		}
		if a.ExtId != b.ExtId {
			return a.ExtId < b.ExtId
		}
		return a.Capability < b.Capability
	})
	return &agentv1.ListPermissionStatesResponse{States: out}, nil
}

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
