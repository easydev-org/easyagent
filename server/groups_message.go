package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/agentbus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/store"
)

/**
 * A group's transcript: store, then deliver.
 *
 * THE ORDER IS THE POINT. A message is written to the group BEFORE it is published to a channel, so a
 * reader who receives it can always find it, and a delivery failure leaves a message that exists and was
 * never heard rather than one that was heard and never existed. The second kind cannot be repaired; the
 * first can be re-published.
 *
 * IT IS THE OPPOSITE OF WHAT A CHANNEL ALONE GIVES. Publishing first meant a message could reach a member
 * while no transcript held it — and a member reading its own history later would find nothing to point at.
 */

func groupMessageToMsg(m store.GroupMessage) *agentv1.GroupMessage {
	return &agentv1.GroupMessage{
		Id:         m.ID,
		Group:      m.Group,
		Sender:     m.Sender,
		SenderName: m.SenderName,
		Body:       m.Body,
		Seq:        m.Seq,
		CreatedAt:  m.CreatedAt,
	}
}

/**
 * sendGroupMessage is the one path a message takes into a group.
 *
 * POSTING IS MEMBERSHIP-GATED FOR SESSIONS. An empty `sender` means a user (the console), which is allowed
 * for any group the caller can see; a session must be a member, so a guessed group name cannot inject a
 * message into a transcript its own reader will trust.
 *
 * THE RECIPIENT IS DECIDED BY THE ROLE, not by the caller:
 *
 *	from a user        → the COORDINATOR, who splits the work
 *	from the coordinator → the addressed member
 *	from a member      → the coordinator (a report)
 *
 * That rule lives here rather than in the channel layer because it needs the membership table, and the
 * channel layer deliberately knows nothing about groups.
 */
func (s *Server) sendGroupMessage(ctx context.Context, tenant, group, sender, text string) (*agentv1.GroupMessage, []string, error) {
	name := strings.TrimSpace(group)
	if name == "" {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil, connect.NewError(connect.CodeInvalidArgument, "message text is required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}

	coordinator, err := s.db.GroupCoordinator(tenant, name)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if coordinator == "" {
		return nil, nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Sprintf("group %q has no coordinator, so there is nowhere for this message to go", name))
	}

	senderName := ""
	if sender != "" {
		isMember, err := s.db.IsGroupMember(tenant, name, sender)
		if err != nil {
			return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		if !isMember {
			// NOT_FOUND, not PERMISSION_DENIED: the same answer as a group that does not exist, so this
			// cannot be used to discover which groups a tenant has.
			return nil, nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
		}
		if row, ok, _ := s.db.GetSession(tenant, sender); ok {
			senderName = row.Name
		}
	}

	delivered := groupRecipient(sender, coordinator)

	msg, err := s.db.AppendGroupMessage(store.GroupMessage{
		ID:         newUUID(),
		Tenant:     tenant,
		Group:      name,
		Sender:     sender,
		SenderName: senderName,
		Body:       text,
	}, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
	}

	s.publishGroupMessage(ctx, tenant, name, sender, delivered, msg)
	return groupMessageToMsg(msg), delivered, nil
}

/**
 * groupRecipient answers who hears a message.
 *
 * ONE RECIPIENT, ONE RULE: THE COORDINATOR. A user writes to the group and a member reports to it; both arrive
 * at the same place, because the coordinator is what a group's user-facing side means.
 *
 * A COORDINATOR'S OWN POST IS RECORDED AND DELIVERED TO NOBODY. It is addressing members through its own tool,
 * which stores and delivers that message itself — this RPC is not how a coordinator hands out work. Keeping the
 * rule here would duplicate a decision the tool already makes.
 *
 * THERE IS NO `to`. A field that a caller could set and the server would ignore is worse than no field: the
 * caller believes it addressed someone, and the failure looks like delivery rather than like a missing
 * capability.
 */
func groupRecipient(sender, coordinator string) []string {
	if sender == coordinator {
		return nil
	}
	return []string{coordinator}
}

/**
 * publishGroupMessage delivers a stored message to its recipients.
 *
 * A FAILURE HERE IS LOGGED, NOT RETURNED. The message is already in the transcript, which is the
 * authoritative record — failing the call would tell the sender nothing was said when it was. A delivery
 * that did not happen is a recipient that was not woken, which the next page or watch will show them.
 */
func (s *Server) publishGroupMessage(ctx context.Context, tenant, group, sender string, to []string, msg store.GroupMessage) {
	// THE ORIGIN NAMES THE SENDER, because the distributor uses it to drop a session's own message. `group:<name>`
	// was the label here and it names no session, so a coordinator's RPC-posted message would have echoed back to
	// it. `session:<id>` is the same form the tool path uses.
	origin := "user:group"
	if sender != "" {
		origin = "session:" + sender
	}
	if s.chanPub == nil || len(to) == 0 {
		return
	}
	payload := map[string]any{
		"group": group, "from": sender, "text": msg.Body,
		"message_id": msg.ID, "seq": msg.Seq,
	}
	for _, rcpt := range to {
		if err := s.chanPub.PublishChannel(ctx, tenant, agentbus.GroupChannel,
			agentbus.GroupMemberTopic(group, rcpt), payload, origin); err != nil {
			slog.Warn("group delivery failed",
				"tenant", tenant, "group", group, "recipient", rcpt, "err", err)
		}
	}
}

// SendGroupMessage posts a message into a group.
func (s *Server) SendGroupMessage(ctx context.Context, req *agentv1.SendGroupMessageRequest) (*agentv1.SendGroupMessageResponse, error) {
	msg, delivered, err := s.sendGroupMessage(ctx, tenantOf(ctx),
		req.GetGroup(), req.GetSender(), req.GetText())
	if err != nil {
		return nil, err
	}
	return &agentv1.SendGroupMessageResponse{Message: msg, DeliveredTo: delivered}, nil
}

/**
 * ListGroupMessages returns one page of a group's transcript.
 *
 * A SESSION MUST BE A MEMBER; a user may read any group. `latest_seq` rides along so a client can start a
 * watch from "now" without a second call.
 */
func (s *Server) ListGroupMessages(ctx context.Context, req *agentv1.ListGroupMessagesRequest) (*agentv1.ListGroupMessagesResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetGroup())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	if sid := req.GetSession(); sid != "" {
		member, err := s.db.IsGroupMember(tenant, name, sid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		if !member {
			return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
		}
	}

	rows, err := s.db.ListGroupMessages(tenant, name, req.GetAfterSeq(), int(req.GetLimit()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	latest, err := s.db.LatestGroupSeq(tenant, name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.GroupMessage, 0, len(rows))
	for _, r := range rows {
		out = append(out, groupMessageToMsg(r))
	}
	return &agentv1.ListGroupMessagesResponse{Messages: out, LatestSeq: latest}, nil
}

/**
 * WatchGroup streams a group's transcript.
 *
 * REPLAY IS A QUERY, NOT A REPLAYED LOG. The messages are rows, so "everything after N" is one SELECT; only
 * the live part needs a subscription. That is why this does not need the session-event machinery: there is
 * no chain to reconstruct and no frame sequence to resume, just a table and a cursor.
 *
 * THE POLL IS DELIBERATE, AND IT IS THE SIMPLEST THING THAT IS CORRECT. A push notification would need the
 * subscriber to be registered before the insert and to survive a replica restart; the table is already the
 * authoritative record, so reading it on a tick means a missed notification costs one interval rather than
 * a lost message. The tick is short because a group is interactive.
 */
func (s *Server) WatchGroup(ctx context.Context, req *agentv1.WatchGroupRequest, stream agentv1connect.AgentServiceWatchGroupServerStream) error {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetGroup())
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	if sid := req.GetSession(); sid != "" {
		member, err := s.db.IsGroupMember(tenant, name, sid)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err.Error())
		}
		if !member {
			return connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
		}
	}

	latest, err := s.db.LatestGroupSeq(tenant, name)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := stream.Send(&agentv1.WatchGroupResponse{Event: "ready", LatestSeq: latest}); err != nil {
		return err
	}

	cursor := req.GetSinceSeq()
	for {
		rows, err := s.db.ListGroupMessages(tenant, name, cursor, 100)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err.Error())
		}
		for _, r := range rows {
			if err := stream.Send(&agentv1.WatchGroupResponse{
				Event: "message", Message: groupMessageToMsg(r),
			}); err != nil {
				return err
			}
			cursor = r.Seq
		}
		if len(rows) > 0 {
			// MORE MAY HAVE ARRIVED while this page was being sent; loop again immediately rather than
			// waiting a tick for the rest.
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.groupWatchInterval()):
		}
	}
}

// groupWatchInterval is the poll period for WatchGroup; a var so a test can shorten it.
func (s *Server) groupWatchInterval() time.Duration {
	if s.groupWatchTick > 0 {
		return s.groupWatchTick
	}
	return 500 * time.Millisecond
}
