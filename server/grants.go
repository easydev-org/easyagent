package server

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// Session permission RPCs.
//
// These record which of an extension's declared permissions a session holds. The agent
// stores `params` as an opaque blob: its shape belongs to whichever extension declared it,
// and interpreting it here would mean teaching the agent's API what a "repository" is.
// The extension decides on every call whether a grant covers what is being attempted.

// ListGrants returns a session's live grants.
func (s *Server) ListGrants(ctx context.Context, req *agentv1.ListGrantsRequest) (*agentv1.ListGrantsResponse, error) {
	tenant := tenantOf(ctx)
	rows, err := s.db.ListGrants(tenant, req.GetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.Grant, 0, len(rows))
	for _, r := range rows {
		out = append(out, grantToMsg(r))
	}
	return &agentv1.ListGrantsResponse{Grants: out}, nil
}

// CreateGrant records one authorisation.
func (s *Server) CreateGrant(ctx context.Context, req *agentv1.CreateGrantRequest) (*agentv1.CreateGrantResponse, error) {
	tenant := tenantOf(ctx)
	if req.GetId() == "" || req.GetExtId() == "" || req.GetCapability() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"session id, ext_id and capability are required")
	}
	// The session must exist: a grant on a session that does not is either a typo or a
	// client bug, and either way it would be dead weight nobody could later find.
	if _, ok, err := s.db.GetSession(tenant, req.GetId()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "no such session")
	}

	params := strings.TrimSpace(req.GetParams())
	if params != "" && !json.Valid([]byte(params)) {
		// Only the SHAPE is checked here (is it JSON at all). Whether the fields make
		// sense is the extension's business — it declared them.
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"params must be a JSON object")
	}

	row, err := s.db.AddGrant(tenant, req.GetId(), store.SessionGrant{
		ExtID:      req.GetExtId(),
		Capability: req.GetCapability(),
		Params:     params,
		Risk:       req.GetRisk(),
		GrantedBy:  "user",
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.CreateGrantResponse{Grant: grantToMsg(row)}, nil
}

// RevokeGrant withdraws one authorisation.
//
// Takes effect on the NEXT tool call: the executor reads grants per call rather than
// caching them, so there is no window where a revoked permission still works. An
// in-flight call is deliberately not interrupted — killing a merge midway leaves a worse
// state than letting it finish.
func (s *Server) RevokeGrant(ctx context.Context, req *agentv1.RevokeGrantRequest) (*agentv1.RevokeGrantResponse, error) {
	tenant := tenantOf(ctx)
	if req.GetId() == "" || req.GetGrantId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"session id and grant_id are required")
	}
	if err := s.db.RevokeGrant(tenant, req.GetId(), req.GetGrantId()); err != nil {
		if errors.Is(err, store.ErrGrantNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err.Error())
		}
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.RevokeGrantResponse{}, nil
}

// grantToMsg maps a stored grant onto the wire type.
func grantToMsg(r store.SessionGrant) *agentv1.Grant {
	return &agentv1.Grant{
		Id:         r.ID,
		ExtId:      r.ExtID,
		Capability: r.Capability,
		Params:     r.Params,
		Risk:       r.Risk,
		GrantedAt:  r.GrantedAt,
		GrantedBy:  r.GrantedBy,
	}
}

// ListPermissionDecls publishes what the discovered extensions can require.
//
// The console renders a grant form from this, which is what keeps the permission UI
// generic: it knows nothing about repositories or sandboxes, only that a permission has a
// key, a risk and a params shape.
func (s *Server) ListPermissionDecls(ctx context.Context, _ *agentv1.ListPermissionDeclsRequest) (*agentv1.ListPermissionDeclsResponse, error) {
	if s.tools == nil {
		// No extension surface wired (a bare server): an empty list is the truth, not an
		// error.
		return &agentv1.ListPermissionDeclsResponse{}, nil
	}
	byExt := s.tools.AllPermissions()
	// Stable order: extension id, then the author's declaration order (which reflects how
	// the extension is used and would be lost by sorting).
	extIDs := make([]string, 0, len(byExt))
	for id := range byExt {
		extIDs = append(extIDs, id)
	}
	sort.Strings(extIDs)

	out := make([]*agentv1.PermissionDecl, 0, len(byExt))
	for _, id := range extIDs {
		for _, p := range byExt[id] {
			out = append(out, &agentv1.PermissionDecl{
				ExtId:        id,
				Key:          p.Key,
				Label:        p.Label,
				Descriptions: p.Descriptions,
				Risk:         p.Risk,
				ParamsFields: toolConfigFields(p.ParamsFields),
			})
		}
	}
	return &agentv1.ListPermissionDeclsResponse{Decls: out}, nil
}

// toolConfigFields maps a config field onto the wire shape the console already renders.
func toolConfigFields(in []bundled.ConfigField) []*agentv1.ToolConfigField {
	if len(in) == 0 {
		return nil
	}
	out := make([]*agentv1.ToolConfigField, 0, len(in))
	for _, f := range in {
		fld := &agentv1.ToolConfigField{
			Name:        f.Name,
			Type:        f.Type,
			Description: f.Description,
		}
		out = append(out, fld)
	}
	return out
}
