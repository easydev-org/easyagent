package server

import (
	"context"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

// Health reports liveness.
func (s *Server) Health(context.Context, *agentv1.HealthRequest) (*agentv1.HealthResponse, error) {
	return &agentv1.HealthResponse{Ok: true, Name: "easyagent-go"}, nil
}

// GetIdentity returns the caller's resolved identity.
func (s *Server) GetIdentity(ctx context.Context, _ *agentv1.GetIdentityRequest) (*agentv1.GetIdentityResponse, error) {
	t := tenantOf(ctx)
	// The admin token pins an EMPTY tenant; report role=admin + its scope
	// ("*" for the platform admin, a subtree root id for a delegated admin).
	if t == "" {
		return &agentv1.GetIdentityResponse{Tenant: "", TenantName: "", Role: "admin", Scope: scopeOf(ctx)}, nil
	}
	name := t
	if ten, ok, _ := s.db.GetTenant(t); ok && ten.Name != "" {
		name = ten.Name
	}
	return &agentv1.GetIdentityResponse{Tenant: t, TenantName: name, Role: "tenant"}, nil
}

// ListSessions returns the tenant's sessions.
func (s *Server) ListSessions(ctx context.Context, _ *agentv1.ListSessionsRequest) (*agentv1.ListSessionsResponse, error) {
	rows, err := s.db.ListSessions(tenantOf(ctx))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.Session, 0, len(rows))
	for _, r := range rows {
		out = append(out, sessionToMsg(r, s.statusOf(ctx, r.ID)))
	}
	return &agentv1.ListSessionsResponse{Sessions: out}, nil
}

/**
 * CreateSession creates a session. THE ID IS SERVER-ASSIGNED AND RANDOM.
 *
 * THERE IS NO NAME-CONFLICT CHECK ANY MORE, because there is no name to conflict on: the id is random
 * and the name is a label two sessions may share. Creating the same "check the build" twice is a
 * legitimate thing to want, and it used to be an error.
 *
 * The caller may pass a name; it is stored as the display label verbatim (including empty, which the
 * console renders as the id).
 */
func (s *Server) CreateSession(ctx context.Context, req *agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
	tenant := tenantOf(ctx)
	model := req.Model
	if model == "" && s.config != nil {
		if v, ok := s.config.Get(tenant, "default_model"); ok {
			model = v
		}
	}
	preset := req.Preset
	if preset == "" && s.config != nil {
		if v, ok := s.config.Get(tenant, "default_preset"); ok {
			preset = v
		}
	}
	// Fall back to the built-in default preset (mirrors the TS agent: a new
	// session always carries a preset, never empty).
	if preset == "" {
		preset = DefaultPreset
	}
	in := &store.SessionInput{
		Name: req.Name, Model: model, Variant: req.Variant, Preset: preset,
		Locale: req.Locale,
	}
	if err := s.db.CreateSession(tenant, in); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// EVERYTHING BELOW ADDRESSES THE SESSION BY ID, never by the label the caller typed. CreateSession
	// writes the generated id back into `in`, so there is no second read — and no window where the row
	// exists but its identity is unknown to the caller.
	//
	// Give the session its built-in channel so anything can reach it by id.
	//
	// A failure here does NOT fail the session: the session exists and works, it is just
	// not reachable on its channel until the subscription is written. Killing the creation
	// would be worse — the user asked for a session, and a broker hiccup should not lose it.
	if s.channels != nil {
		_ = s.channels.EnsureDefault(ctx, tenant, in.ID)
	}
	s.lifecycleFire(ctx, tenant, "created", in.ID, map[string]any{"kind": "created", "tenant": tenant, "session_id": in.ID})
	return &agentv1.CreateSessionResponse{Ok: true, SessionId: in.ID, Name: in.Name}, nil
}

// GetSession returns one session.
func (s *Server) GetSession(ctx context.Context, req *agentv1.GetSessionRequest) (*agentv1.GetSessionResponse, error) {
	tenant := tenantOf(ctx)
	row, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	return &agentv1.GetSessionResponse{Session: sessionToMsg(*row, s.statusOf(ctx, req.Id))}, nil
}

// DeleteSession removes a session (+ its mailbox) and aborts any live turn.
func (s *Server) DeleteSession(ctx context.Context, req *agentv1.DeleteSessionRequest) (*agentv1.DeleteSessionResponse, error) {
	tenant := tenantOf(ctx)
	if s.rt != nil {
		s.rt.Interrupt(tenant, req.Id)
	}
	if err := s.db.DeleteSession(tenant, req.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// A DELETED SESSION MUST NOT STAY IN A GROUP'S MEMBER LIST. The group would keep advertising a
	// session that cannot answer, and nothing would ever remove it — the row's owner is gone.
	//
	// Best-effort: the session is already gone, and failing here would report an error for a deletion
	// that succeeded. A leftover row is repaired by the next group read, which cannot resolve it.
	_ = s.db.DeleteGroupMemberships(tenant, req.Id)
	s.lifecycleFire(ctx, tenant, "deleted", req.Id, map[string]any{"kind": "deleted", "tenant": tenant, "session_name": req.Id})
	return &agentv1.DeleteSessionResponse{Ok: true}, nil
}

/**
 * forkExclusions answers which capabilities the fork must not inherit.
 *
 * BOTH NON-INHERIT POLICIES END UP AS AN EXCLUSION SET: `INHERIT_NONE` excludes everything the parent
 * holds, which the store cannot express as "all" without reading the parent first. Doing it here keeps
 * one code path in the store instead of three, and the copy loop already has the rows in hand.
 *
 * AN UNKNOWN POLICY VALUE FALLS BACK TO INHERITING EVERYTHING, which is the pre-existing behaviour —
 * a client from before this field existed sends 0 anyway, and quietly giving forks no permissions
 * would break every caller that never asked about policy.
 */
func forkExclusions(req *agentv1.ForkRequest) map[string]bool {
	if req.GetPermissions() != agentv1.ForkPermissionPolicy_FORK_PERMISSION_POLICY_INHERIT_EXCEPT_SOME {
		return nil
	}
	except := make(map[string]bool, len(req.GetExcludeCapabilities()))
	for _, k := range req.GetExcludeCapabilities() {
		except[k] = true
	}
	return except
}

/**
 * Rename changes a session's DISPLAY NAME. The id does not move.
 *
 * IT USED TO CREATE A NEW ROW AND DELETE THE OLD ONE, which was the only way to change the identity
 * while the identity was the name. That made a rename a cascade: the session got a new mailbox subject,
 * a new wake subject, a new subscription pattern, and every group slot naming the old id became a slot
 * pointing at nothing.
 *
 * Now it is one UPDATE of one column. Nothing that addresses the session changes, so nothing has to be
 * migrated, re-provisioned or re-added to a group.
 *
 * NO UNIQUENESS CHECK: two sessions may share a display name. Refusing the second would make the label
 * serve the database instead of the reader.
 */
func (s *Server) Rename(ctx context.Context, req *agentv1.RenameRequest) (*agentv1.RenameResponse, error) {
	tenant := tenantOf(ctx)
	row, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	if err := s.db.UpdateSessionSettings(tenant, req.Id, map[string]any{"name": req.Name}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row.Name = req.Name
	s.lifecycleFire(ctx, tenant, "renamed", req.Id, map[string]any{
		"kind": "renamed", "tenant": tenant, "session_id": req.Id, "name": req.Name,
	})
	return &agentv1.RenameResponse{Session: sessionToMsg(*row, s.statusOf(ctx, req.Id))}, nil
}

// SetModel updates the session's model (+ variant).
func (s *Server) SetModel(ctx context.Context, req *agentv1.SetModelRequest) (*agentv1.SetModelResponse, error) {
	tenant := tenantOf(ctx)
	patch := map[string]any{"model": req.Model}
	if req.Variant != "" {
		patch["variant"] = req.Variant
	}
	if err := s.db.UpdateSessionSettings(tenant, req.Id, patch); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row, ok, _ := s.db.GetSession(tenant, req.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	return &agentv1.SetModelResponse{Session: sessionToMsg(*row, s.statusOf(ctx, req.Id))}, nil
}

// UpdateSettings patches the session settings.
func (s *Server) UpdateSettings(ctx context.Context, req *agentv1.UpdateSettingsRequest) (*agentv1.UpdateSettingsResponse, error) {
	tenant := tenantOf(ctx)
	patch := map[string]any{}
	if req.Model != "" {
		patch["model"] = req.Model
	}
	if req.Preset != "" {
		patch["preset"] = req.Preset
	}
	if req.Locale != "" {
		patch["locale"] = agent.NormalizeLocale(req.Locale)
	}
	if req.Variant != "" {
		patch["variant"] = req.Variant
	}
	// ABSENT MEANS "LEAVE IT", PRESENT-BUT-EMPTY MEANS "CLEAR IT". The request fields are `optional`
	// precisely so both are expressible: clearing is how a session goes back to the derived identicon,
	// or to saying nothing about itself, and a non-optional string would make clearing impossible
	// without a second RPC.
	if req.AvatarFile != nil {
		patch["avatar_file"] = req.GetAvatarFile()
	}
	if req.Intro != nil {
		patch["intro"] = req.GetIntro()
	}
	// THE DISPLAY NAME LIVES HERE SO ONE SAVE COVERS THE PROFILE PAGE. It is the same column `Rename`
	// writes; `Rename` remains for the callers that only change the label.
	if req.Name != nil {
		patch["name"] = req.GetName()
	}
	if err := s.db.UpdateSessionSettings(tenant, req.Id, patch); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row, ok, _ := s.db.GetSession(tenant, req.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	return &agentv1.UpdateSettingsResponse{Session: sessionToMsg(*row, s.statusOf(ctx, req.Id))}, nil
}

// Undo withdraws the chain back to a message (moves the tip; no new message).
func (s *Server) Undo(ctx context.Context, req *agentv1.UndoRequest) (*agentv1.UndoResponse, error) {
	tenant := tenantOf(ctx)
	row, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	var newTip *string
	if req.MessageId != "" {
		m, ok, err := s.db.GetMessage(tenant, req.MessageId)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		if !ok {
			return nil, connect.NewError(connect.CodeNotFound, "message not found")
		}
		newTip = m.PrevID
		if row.TipID != nil && *row.TipID != "" {
			inChain, err := s.db.IsInChain(tenant, *row.TipID, req.MessageId)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err.Error())
			}
			if !inChain {
				return nil, connect.NewError(connect.CodeInvalidArgument, "message not in this session chain")
			}
		}
	}
	updatedAt, err := s.db.SetTip(tenant, req.Id, newTip)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// Patch the row we already read instead of re-reading it: SetTip only
	// changes tip_id + updated_at, and it hands back the timestamp it wrote.
	row.TipID = newTip
	row.UpdatedAt = updatedAt
	return &agentv1.UndoResponse{Session: sessionToMsg(*row, "idle")}, nil
}

// Fork creates a child session sharing the parent chain at a fork point.
func (s *Server) Fork(ctx context.Context, req *agentv1.ForkRequest) (*agentv1.ForkResponse, error) {
	tenant := tenantOf(ctx)
	parent, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	var forkTip *string
	if req.MessageId != "" {
		if _, ok, _ := s.db.GetMessage(tenant, req.MessageId); !ok {
			return nil, connect.NewError(connect.CodeNotFound, "fork message not found")
		}
		forkTip = &req.MessageId
	} else {
		tip := ""
		if parent.TipID != nil {
			tip = *parent.TipID
		}
		forkTip, err = s.db.ForkBase(tenant, tip)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
	}
	preset := req.Preset
	if preset == "" {
		preset = parent.Preset
	}
	if preset == "" {
		preset = DefaultPreset
	}
	// THE ID IS SERVER-ASSIGNED, and `in.ID` comes back filled — the fork is addressed by it from here
	// on, never by the label the caller passed.
	in := &store.SessionInput{
		Name: req.Name, Model: parent.Model, Variant: parent.Variant, Preset: preset,
		TipID: forkTip, SystemPrompt: parent.SystemPrompt, MaxTurns: parent.MaxTurns,
		Locale: parent.Locale,
	}
	if err := s.db.CreateSession(tenant, in); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// A fork inherits the parent's permissions.
	//
	// Otherwise forking — the normal way to try a different approach — would silently strip
	// every capability the session had, and the model would start failing on tools that
	// worked a moment earlier. The copies are INDEPENDENT rows tagged "auto:fork:<parent>",
	// so revoking one in the child leaves the parent intact and a user can see where each
	// permission came from.
	//
	// A failure here is not fatal: the session exists and simply starts with no grants,
	// which is the safe direction for a permission problem.
	//
	// THE POLICY COMES FROM THE CALLER because "same session, different approach" and "a clean slate
	// for a worker" are different intents, and a fork that guessed would either strip a session the user
	// meant to keep whole or hand a worker the grants it needs to recruit.
	_, _ = s.db.CopyGrantsFromExcept(tenant, req.Id, in.ID,
		forkExclusions(req),
		req.GetPermissions() == agentv1.ForkPermissionPolicy_FORK_PERMISSION_POLICY_INHERIT_NONE)
	// A fork is a NEW session, so it needs its OWN default channel. It does not inherit the
	// parent's: a message addressed to the parent must not reach the child, and a child that
	// was unreachable by name would be a strange kind of session.
	if s.channels != nil {
		_ = s.channels.EnsureDefault(ctx, tenant, in.ID)
	}
	s.publish(tenant, in.ID, "session-changed", map[string]any{"session_id": in.ID})
	s.lifecycleFire(ctx, tenant, "forked", in.ID, map[string]any{"kind": "forked", "tenant": tenant, "parent": req.Id})
	row, _, _ := s.db.GetSession(tenant, in.ID)
	return &agentv1.ForkResponse{Session: sessionToMsg(*row, "idle")}, nil
}

// MarkRead advances the shared read watermark.
func (s *Server) MarkRead(ctx context.Context, req *agentv1.MarkReadRequest) (*agentv1.MarkReadResponse, error) {
	tenant := tenantOf(ctx)
	if _, err := s.db.MarkRead(tenant, req.Id, int64(req.Seq)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row, ok, _ := s.db.GetSession(tenant, req.Id)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	return &agentv1.MarkReadResponse{Session: sessionToMsg(*row, s.statusOf(ctx, req.Id))}, nil
}

// statusOf reports a session's runtime status ("busy" while a turn holds it).
func (s *Server) statusOf(ctx context.Context, session string) string {
	if s.rt != nil && s.rt.IsActive(tenantOf(ctx), session) {
		return "busy"
	}
	return "idle"
}
