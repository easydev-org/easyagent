package server

import (
	"context"
	"time"
	"encoding/json"
	"fmt"

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

// publishMailbox sends one mailbox message through the single authoritative path.
//
// The server used to enqueue and then call rt.Wake directly, which is correct only while THIS
// replica holds the session lease. A prompt arriving when another replica holds it enqueued and
// woke nobody; the 60-second boot backstop was the only thing that eventually ran it.
func (s *Server) publishMailbox(ctx context.Context, tenant, session, typ string, payload any, source string) error {
	if s.runtimeBus != nil {
		// Enqueue + cross-replica signal. The bus owns both halves so no caller can do one.
		return s.runtimeBus.PublishMailbox(ctx, tenant, session, typ, payload, source)
	}
	// In-memory mode: no bus, so no other replica to notify and no signal to send. Enqueue and
	// wake THIS runtime — the only one there is.
	if err := s.enqueue(ctx, tenant, session, typ, payload, source); err != nil {
		return err
	}
	if s.rt != nil {
		go func() { _ = s.rt.Wake(context.WithoutCancel(ctx), tenant, session) }()
	}
	return nil
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
	// The same publish path every other producer uses: enqueue (durable) then signal
	// (cross-replica). Calling db.Enqueue + rt.Wake here would be correct only while the lease
	// happens to be on THIS replica — a prompt that arrives when another replica holds it would
	// enqueue and then wake nobody, and only the 60s boot backstop would notice.
	if err := s.publishMailbox(ctx, tenant, req.Id, "trigger", payload, "user"); err != nil {
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

/**
 * Mailbox returns a newest-first page of the session's mailbox.
 *
 * THE QUEUE RETURNS THE WHOLE LIST and the page is taken here, because the cursor is a presentation
 * concern: a bucket has no stable server-side cursor across a TTL sweep, and inventing one would put
 * pagination state in the store where it would have to expire alongside the messages.
 */
func (s *Server) Mailbox(ctx context.Context, req *agentv1.MailboxRequest) (*agentv1.MailboxResponse, error) {
	tenant := tenantOf(ctx)
	if s.mail == nil {
		// NOT AN EMPTY LIST: "this deployment has no queue" is a different answer from "nothing was sent",
		// and showing the second when the first is true would hide a misconfiguration.
		return nil, connect.NewError(connect.CodeUnavailable,
			"the mailbox needs a runtime bus; this deployment has none")
	}
	msgs, err := s.mail.Page(ctx, tenant, req.GetId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	hasMore := len(msgs) > limit
	if hasMore {
		msgs = msgs[:limit]
	}
	out := make([]*agentv1.MailboxEntry, 0, len(msgs))
	for _, m := range msgs {
		e := &agentv1.MailboxEntry{
			Id: m.ID, SessionName: req.GetId(), MsgType: m.Type, Source: m.Source,
			Status: m.Status, CreatedAt: millisToText(m.CreatedAt),
		}
		// THE PAYLOAD IS AN OBJECT NOW, rendered as JSON text for the wire — the entry's field is a
		// string and has consumers that do not need to change.
		if raw, jerr := json.Marshal(m.Payload); jerr == nil {
			e.Payload = string(raw)
		}
		if m.ConsumedAt > 0 {
			e.ConsumedAt = millisToText(m.ConsumedAt)
		}
		out = append(out, e)
	}
	return &agentv1.MailboxResponse{Ok: true, Mailbox: out, HasMore: hasMore}, nil
}

// millisToText renders a Unix-millisecond timestamp the way the mailbox wire type expects.
func millisToText(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

/**
 * enqueue puts a message where the runtime will find it.
 *
 * A DEPLOYMENT WITH NO QUEUE REFUSES rather than dropping the message. The alternative — a nil-safe no-op
 * — would let `Prompt` return "accepted" for a prompt nothing would ever run, which is the same class of
 * silently-broken the mailbox redesign exists to remove.
 */
func (s *Server) enqueue(ctx context.Context, tenant, session, typ string, payload any, source string) error {
	if s.mail == nil {
		return fmt.Errorf("publish mailbox: no queue configured in this deployment")
	}
	_, err := s.mail.Enqueue(ctx, tenant, session, "", typ, payload, source)
	return err
}

// Interrupt aborts a session's in-flight turn.
func (s *Server) Interrupt(ctx context.Context, req *agentv1.InterruptRequest) (*agentv1.InterruptResponse, error) {
	tenant := tenantOf(ctx)
	interrupted := false
	if s.rt != nil {
		interrupted = s.rt.Interrupt(tenant, req.Id)
	}
	// Also enqueue a durable interrupt so a turn on ANOTHER replica aborts.
	if err := s.enqueue(ctx, tenant, req.Id, "interrupt", map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.InterruptResponse{Ok: true, Interrupted: interrupted}, nil
}

// Compact enqueues a manual compaction request.
func (s *Server) Compact(ctx context.Context, req *agentv1.CompactRequest) (*agentv1.CompactResponse, error) {
	tenant := tenantOf(ctx)
	if err := s.enqueue(ctx, tenant, req.Id, "compact", map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if s.rt != nil {
		go func() { _ = s.rt.Wake(ctx, tenant, req.Id) }()
	}
	return &agentv1.CompactResponse{Ok: true}, nil
}
