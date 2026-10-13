package server

import (
	"context"
	"encoding/json"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
)

// ListMessages reads a session's chain (anchored / backward-paged).
func (s *Server) ListMessages(ctx context.Context, req *agentv1.ListMessagesRequest) (*agentv1.ListMessagesResponse, error) {
	tenant := tenantOf(ctx)
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	}
	tip, err := s.db.SessionTip(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if req.After != "" {
		d, err := s.db.DeltaSince(tenant, tip, req.After, limit)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		resync := !d.AnchorReached && d.ReachedRoot
		msgs := d.Messages
		if resync {
			msgs = nil
		}
		proto, err := s.hydrate(tenant, msgs)
		if err != nil {
			return nil, err
		}
		return &agentv1.ListMessagesResponse{Ok: true, Messages: proto, Resync: resync, TipId: tip}, nil
	}
	msgs, err := s.db.Chain(tenant, tip, limit, req.Before)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	proto, err := s.hydrate(tenant, msgs)
	if err != nil {
		return nil, err
	}
	return &agentv1.ListMessagesResponse{Ok: true, Messages: proto, TipId: tip}, nil
}

func (s *Server) hydrate(tenant string, msgs []storeMessage) ([]*agentv1.Message, error) {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	parts, err := s.db.PartsByMessages(tenant, ids)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return messagesToProto(msgs, parts), nil
}

// Prompt enqueues a user prompt (the mailbox is the single writer of the
// chain) and streams the run's events. The first frame is `accepted` with the
// logical message id.
func (s *Server) Prompt(ctx context.Context, req *agentv1.PromptRequest, stream agentv1connect.AgentServicePromptServerStream) error {
	tenant := tenantOf(ctx)
	if _, ok, err := s.db.GetSession(tenant, req.Id); err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return connect.NewError(connect.CodeNotFound, "session not found")
	}
	messageID := newUUID()
	payload := agent.WakePayload{Type: "trigger", MessageID: messageID, Text: req.Prompt}
	for _, a := range req.Attachments {
		payload.Attachments = append(payload.Attachments, agent.FileRef{
			Code: a.Code, Name: a.Name, Size: int(a.Size),
		})
	}
	if _, err := s.db.Enqueue(tenant, req.Id, "trigger", payload, "user"); err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := stream.Send(&agentv1.PromptResponse{
		Event: "accepted", Params: map[string]string{"message_id": messageID},
	}); err != nil {
		return err
	}
	// Subscribe BEFORE waking so no early event is missed.
	ch, cancel := s.hub.Subscribe(tenant, req.Id)
	defer cancel()
	if s.rt != nil {
		go func() { _ = s.rt.Wake(ctx, tenant, req.Id) }()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			if err := stream.Send(eventToPrompt(ev)); err != nil {
				return err
			}
			if ev.Name == "turn-complete" {
				return nil
			}
		}
	}
}

// WatchSession streams a session's live events. Resume is by STREAM SEQUENCE
// (`since_seq`): with the durable stream wired the server replays retained
// frames at/after that sequence then streams live; without it, it streams the
// in-process hub from now.
func (s *Server) WatchSession(ctx context.Context, req *agentv1.WatchSessionRequest, stream agentv1connect.AgentServiceWatchSessionServerStream) error {
	tenant := tenantOf(ctx)
	// Open the stream eagerly so a client's WatchSession() call returns as soon
	// as the connection is established, instead of blocking until the first
	// event (which may be long in coming). connecthttp's SendHeaders is a no-op
	// server-side, so a real first frame is what flushes the response headers.
	if err := stream.Send(&agentv1.WatchSessionResponse{Event: "ready"}); err != nil {
		return err
	}
	if s.stream != nil {
		ch, cancel, err := s.stream.SubscribeSessionEvents(ctx, tenant, req.Id, uint64(req.SinceSeq))
		if err != nil {
			return connect.NewError(connect.CodeInternal, err.Error())
		}
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return nil
			case raw, ok := <-ch:
				if !ok {
					return nil
				}
				var f Frame
				if err := json.Unmarshal(raw, &f); err != nil {
					continue
				}
				if err := stream.Send(&agentv1.WatchSessionResponse{
					Event: f.Event, Params: eventParams(agent.Event{Name: f.Event, Params: f.Params}),
					Eid: f.Eid, Seq: f.Seq,
				}); err != nil {
					return err
				}
			}
		}
	}
	ch, cancel := s.hub.Subscribe(tenant, req.Id)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			if err := stream.Send(&agentv1.WatchSessionResponse{
				Event: ev.Name, Params: eventParams(ev), Seq: s.hub.NextSeq(),
			}); err != nil {
				return err
			}
		}
	}
}

// WatchSessions streams the session list (an initial snapshot then upserts).
func (s *Server) WatchSessions(ctx context.Context, _ *agentv1.WatchSessionsRequest, stream agentv1connect.AgentServiceWatchSessionsServerStream) error {
	tenant := tenantOf(ctx)
	rows, err := s.db.ListSessions(tenant)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	}
	upserts := make([]*agentv1.Session, 0, len(rows))
	for _, r := range rows {
		upserts = append(upserts, sessionToMsg(r, s.statusOf(ctx, r.Name)))
	}
	if err := stream.Send(&agentv1.WatchSessionsResponse{Upserts: upserts, Snapshot: true}); err != nil {
		return err
	}
	// Live upserts are driven by session-changed nudges carried on the hub
	// under a per-tenant key. (A multi-replica build backs this with the
	// durable session-changed stream.)
	ch, cancel := s.hub.Subscribe(tenant, "\x00sessions")
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			name := asStr(ev.Params["session_name"])
			if name == "" {
				continue
			}
			if row, ok, _ := s.db.GetSession(tenant, name); ok {
				if err := stream.Send(&agentv1.WatchSessionsResponse{
					Upserts: []*agentv1.Session{sessionToMsg(*row, s.statusOf(ctx, name))},
				}); err != nil {
					return err
				}
			} else {
				// The session is gone (deleted or renamed away) → emit a removal.
				if err := stream.Send(&agentv1.WatchSessionsResponse{Removed: []string{name}}); err != nil {
					return err
				}
			}
		}
	}
}

// State reports the session's runtime status.
func (s *Server) State(ctx context.Context, req *agentv1.StateRequest) (*agentv1.StateResponse, error) {
	tenant := tenantOf(ctx)
	row, ok, err := s.db.GetSession(tenant, req.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, "session not found")
	}
	return &agentv1.StateResponse{State: structFromMap(map[string]any{
		"status": s.statusOf(ctx, req.Id), "tip_id": deref(row.TipID),
		"message_seq": row.MessageSeq, "read_seq": row.ReadSeq,
	})}, nil
}

// Mailbox returns a newest-first page of the session's mailbox.
func (s *Server) Mailbox(ctx context.Context, req *agentv1.MailboxRequest) (*agentv1.MailboxResponse, error) {
	tenant := tenantOf(ctx)
	rows, hasMore, err := s.db.ListMailboxPage(tenant, req.Id, int(req.Limit), req.Before)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.MailboxEntry, 0, len(rows))
	for _, m := range rows {
		e := &agentv1.MailboxEntry{
			Id: m.ID, SessionName: m.SessionName, MsgType: m.MsgType, Source: m.Source,
			Payload: m.Payload, Status: m.Status, CreatedAt: m.CreatedAt,
		}
		if m.EffectiveAt != nil {
			e.EffectiveAt = *m.EffectiveAt
		}
		if m.ConsumedAt != nil {
			e.ConsumedAt = *m.ConsumedAt
		}
		if m.Seq != nil {
			e.Seq = *m.Seq
		}
		out = append(out, e)
	}
	return &agentv1.MailboxResponse{Ok: true, Mailbox: out, HasMore: hasMore}, nil
}

// Interrupt aborts a session's in-flight turn.
func (s *Server) Interrupt(ctx context.Context, req *agentv1.InterruptRequest) (*agentv1.InterruptResponse, error) {
	tenant := tenantOf(ctx)
	interrupted := false
	if s.rt != nil {
		interrupted = s.rt.Interrupt(tenant, req.Id)
	}
	// Also enqueue a durable interrupt so a turn on ANOTHER replica aborts.
	if _, err := s.db.Enqueue(tenant, req.Id, "interrupt", map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.InterruptResponse{Ok: true, Interrupted: interrupted}, nil
}

// Compact enqueues a manual compaction request.
func (s *Server) Compact(ctx context.Context, req *agentv1.CompactRequest) (*agentv1.CompactResponse, error) {
	tenant := tenantOf(ctx)
	if _, err := s.db.Enqueue(tenant, req.Id, "compact", map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if s.rt != nil {
		go func() { _ = s.rt.Wake(ctx, tenant, req.Id) }()
	}
	return &agentv1.CompactResponse{Ok: true}, nil
}
