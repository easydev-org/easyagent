package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easybus-sdk/mailbox"
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

// publishMailbox puts one message in the durable queue and signals the replicas.
//
// IT USED TO BRANCH ON `runtimeBus != nil`. With a bus it delegated to `runtimeBus.PublishMailbox`; without
// one it enqueued and woke this runtime directly. Two paths for one operation, and the branch existed
// only because the bus was optional — the ORDER (enqueue, then signal) was the same either way.
//
// The bus, when it exists, already IS the durable queue plus the cross-replica signal, so delegating is
// right; the in-process case is what a deployment without NATS looks like, and there the queue is local
// and `Wake` is the only subscriber. Keeping both here is what let the two drift: a caller could take a
// path whose signal meant something different.
//
// ENQUEUE FIRST. A signal that arrives before the row exists wakes a runtime that finds nothing and
// returns — the turn simply never happens, and the only trace is a log line saying the drain was empty.
func (s *Server) publishMailbox(ctx context.Context, tenant, session string, typ mailbox.Type, payload any, source string) error {
	if s.runtimeBus != nil {
		return s.runtimeBus.PublishMailbox(ctx, tenant, session, typ, payload, source)
	}
	if s.mail == nil {
		// Refusing is deliberate. Falling through to signal-only would look like success from every
		// angle except the session waking up.
		return fmt.Errorf("publish mailbox: no queue configured in this deployment")
	}
	if _, err := s.mail.Enqueue(ctx, tenant, session, "", typ, payload, source); err != nil {
		return fmt.Errorf("enqueue mailbox: %w", err)
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
	// ONE BODY TYPE, shared by every producer. The queue's `Payload` stays untyped on the wire
	// because an extension may send fields this agent does not know; what the AGENT writes is this.
	payload := mailbox.Body{MessageID: messageID, Text: req.Prompt}
	for _, a := range req.Attachments {
		payload.FileRefs = append(payload.FileRefs, mailbox.FileRef{
			Code: a.Code, Name: a.Name, Size: int(a.Size),
		})
	}
	// The same publish path every other producer uses: enqueue (durable) then signal
	// (cross-replica). Calling db.Enqueue + rt.Wake here would be correct only while the lease
	// happens to be on THIS replica — a prompt that arrives when another replica holds it would
	// enqueue and then wake nobody, and only the 60s boot backstop would notice.
	if err := s.publishMailbox(ctx, tenant, req.Id, mailbox.Trigger, payload, "user"); err != nil {
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
		// A PAGE REFRESH HAS NO `seq`, only the newest message id it holds. Resolve that to a
		// timestamp here, because by the time frames are read back the id is gone.
		//
		// Done only when since_seq is unset: the proto says the sequence wins, and resolving an id
		// the client did not mean to use would filter out frames it needs.
		var sinceAt int64
		if req.SinceSeq == 0 && req.SinceMsg != "" {
			if m, ok, merr := s.db.GetMessage(tenant, req.SinceMsg); merr == nil && ok && m != nil {
				if t, terr := time.Parse(time.RFC3339Nano, m.CreatedAt); terr == nil {
					sinceAt = t.UnixMilli()
				}
			}
		}
		ch, cancel, err := s.stream.SubscribeSessionEvents(ctx, tenant, req.Id, uint64(req.SinceSeq), sinceAt)
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
				Event: ev.Name, Params: eventParams(ev),
				// THE FRAME'S OWN STAMP, not a fresh one. These are what the client resumes from
				// and dedupes on, so they must be the frame's identity rather than a counter tick
				// taken at send time.
				Eid: ev.Eid, Seq: ev.Seq,
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
		upserts = append(upserts, sessionToMsg(r, s.statusOf(ctx, r.ID)))
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
			Id: m.ID, SessionName: req.GetId(), MsgType: string(m.Type), Source: m.Source,
			Status: string(m.Status), CreatedAt: millisToText(m.CreatedAt),
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
func (s *Server) enqueue(ctx context.Context, tenant, session string, typ mailbox.Type, payload any, source string) error {
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
	if err := s.enqueue(ctx, tenant, req.Id, mailbox.Interrupt, map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.InterruptResponse{Ok: true, Interrupted: interrupted}, nil
}

// Compact enqueues a manual compaction request.
func (s *Server) Compact(ctx context.Context, req *agentv1.CompactRequest) (*agentv1.CompactResponse, error) {
	tenant := tenantOf(ctx)
	if err := s.enqueue(ctx, tenant, req.Id, mailbox.Compact, map[string]any{}, "user"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if s.rt != nil {
		go func() { _ = s.rt.Wake(ctx, tenant, req.Id) }()
	}
	return &agentv1.CompactResponse{Ok: true}, nil
}
