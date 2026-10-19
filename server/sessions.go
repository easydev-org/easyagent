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
		out = append(out, sessionToMsg(r, s.statusOf(ctx, r.Name)))
	}
	return &agentv1.ListSessionsResponse{Sessions: out}, nil
}

// CreateSession creates a session (idempotent on name conflict -> error).
func (s *Server) CreateSession(ctx context.Context, req *agentv1.CreateSessionRequest) (*agentv1.CreateSessionResponse, error) {
	tenant := tenantOf(ctx)
	if req.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "name is required")
	}
	exists, err := s.db.SessionExists(tenant, req.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if exists {
		return nil, connect.NewError(connect.CodeAlreadyExists, "session already exists")
	}
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
	in := store.SessionInput{
		Name: req.Name, Model: model, Variant: req.Variant, Preset: preset,
		Group: req.Group, Locale: req.Locale,
	}
	if err := s.db.CreateSession(tenant, in); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	s.lifecycleFire(ctx, tenant, "created", req.Name, map[string]any{"kind": "created", "tenant": tenant, "session_name": req.Name})
	return &agentv1.CreateSessionResponse{Ok: true, SessionName: req.Name}, nil
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
	s.lifecycleFire(ctx, tenant, "deleted", req.Id, map[string]any{"kind": "deleted", "tenant": tenant, "session_name": req.Id})
	return &agentv1.DeleteSessionResponse{Ok: true}, nil
}

// Rename renames a session (a new row with the same settings, the old removed).
func (s *Server) Rename(ctx context.Context, req *agentv1.RenameRequest) (*agentv1.RenameResponse, error) {
	tenant := tenantOf(ctx)
	old, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	if exists, _ := s.db.SessionExists(tenant, req.Name); exists {
		return nil, connect.NewError(connect.CodeAlreadyExists, "session already exists")
	}
	in := store.SessionInput{
		Name: req.Name, Model: old.Model, Variant: old.Variant, Preset: old.Preset,
		TipID: old.TipID, SystemPrompt: old.SystemPrompt, MaxTurns: old.MaxTurns,
		Locale: old.Locale, Group: old.Group,
	}
	if err := s.db.CreateSession(tenant, in); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.db.DeleteSession(tenant, req.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	s.lifecycleFire(ctx, tenant, "renamed", req.Name, map[string]any{"kind": "renamed", "tenant": tenant, "from": req.Id, "to": req.Name})
	row, _, _ := s.db.GetSession(tenant, req.Name)
	return &agentv1.RenameResponse{Session: sessionToMsg(*row, "idle")}, nil
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
	if err := s.db.SetTip(tenant, req.Id, newTip); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	row, _, _ = s.db.GetSession(tenant, req.Id)
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
	if exists, _ := s.db.SessionExists(tenant, req.Name); exists {
		return nil, connect.NewError(connect.CodeAlreadyExists, "session already exists")
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
	in := store.SessionInput{
		Name: req.Name, Model: parent.Model, Variant: parent.Variant, Preset: preset,
		TipID: forkTip, SystemPrompt: parent.SystemPrompt, MaxTurns: parent.MaxTurns,
		Locale: parent.Locale, Group: parent.Group,
	}
	if err := s.db.CreateSession(tenant, in); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	s.publish(tenant, req.Name, "session-changed", map[string]any{"session_name": req.Name})
	s.lifecycleFire(ctx, tenant, "forked", req.Name, map[string]any{"kind": "forked", "tenant": tenant, "parent": req.Id})
	row, _, _ := s.db.GetSession(tenant, req.Name)
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
