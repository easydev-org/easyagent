package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/agentbus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * WHOSE TURN IT IS TO ANSWER.
 *
 * A group is a place where one session asks another for something. Until this existed, "asked" was all the system
 * knew: `group-send` wrote the message and returned, and nothing recorded that a reply was now owed. A coordinator
 * could not tell a worker that was thinking from one that had finished and forgotten — the group showed the question
 * and silence, and silence looks the same either way.
 *
 * THE OBLIGATION IS RECORDED WHEN THE MESSAGE IS SENT (see `group-send`), and settled when the addressed session
 * answers ITS ASKER. This file is the third part: what happens when a turn ENDS with a debt still open.
 *
 * IT IS NOT A POLLER. A poller watching for "idle" would fire between every turn of every session, because idle is not
 * an event — it is the resting state. The runtime calls `OnTurnEnd` at the one moment the question is meaningful: the
 * turn has finished and nothing else is running for that session.
 */

// maxReplyReminders bounds how many times one obligation is chased.
//
// THREE, AND THEN THE ROW STAYS OPEN AND VISIBLE. Chasing forever turns an uncooperative session into an infinite
// loop that spends tokens to produce nothing; chasing never leaves a coordinator waiting on an answer that will not
// come. Stopping at three says "the system told it three times" and hands the decision to the person who can make it —
// which is the honest end state for an agent that will not answer.
const maxReplyReminders = 3

/**
 * ChaseUnansweredReplies is the turn-end check: does this session still owe a reply, and should it be reminded.
 *
 * EXPORTED BECAUSE THE RUNTIME CALLS IT. `agent` must not import `server` — the runtime drives turns and knows nothing
 * about groups — so the host wires `RuntimeDeps.OnTurnEnd` to this method. The dependency points one way.
 *
 * BEST-EFFORT AND NON-BLOCKING IN EFFECT. It runs on the turn's goroutine, so every failure is logged rather than
 * returned — a reminder that could not be sent must not turn into an error for a turn that otherwise succeeded.
 */
func (s *Server) ChaseUnansweredReplies(ctx context.Context, tenant, session string) {
	if s.db == nil {
		return
	}
	due, err := s.db.ObligationsDueForReminder(tenant, session, maxReplyReminders)
	if err != nil {
		slog.Warn("reply obligations unreadable", "tenant", tenant, "session", session, "err", err)
		return
	}
	for _, ob := range due {
		s.remindOnce(ctx, tenant, session, ob)
	}
}

/**
 * remindOnce posts ONE reminder and records that it was sent.
 *
 * ORDER: STORE, THEN DELIVER, THEN COUNT — the same order a group message takes, for the same reason. A reminder a
 * session can read in its transcript but that was never counted would be sent again on the next turn; one counted
 * but never stored would consume an attempt without ever reaching anyone.
 *
 * THE REMINDER IS A SYSTEM MESSAGE. `kind = "system"` and an empty `Sender` say "the platform wrote this" — not the
 * coordinator, and not the person. A reminder attributed to the asker would read as that session nagging, which is
 * exactly the confusion this feature exists to remove.
 */
func (s *Server) remindOnce(ctx context.Context, tenant, session string, ob store.GroupReplyObligation) {
	g, ok, err := s.db.GetGroup(tenant, ob.GroupID)
	if err != nil || !ok {
		// THE GROUP IS GONE. The debt belongs to a room that no longer exists; dropping it is the only outcome that
		// does not leave a reminder pointing at nothing.
		_ = s.db.DropObligationsInGroup(tenant, ob.GroupID, session)
		return
	}
	// THE ASKER MAY ALSO BE GONE. An obligation to answer a deleted session can never be discharged.
	askerOK := true
	if _, exists, err := s.db.GetSession(tenant, ob.From); err != nil || !exists {
		askerOK = false
	}
	if !askerOK {
		_ = s.db.DropObligationsInGroup(tenant, ob.GroupID, session)
		return
	}

	locale := ""
	if row, ok, _ := s.db.GetSession(tenant, session); ok {
		locale = row.Locale
	}
	body := replyReminderText(locale, g.Name, ob.From, sessionNameFor(s, tenant, ob.From), ob.RemindCount+1)

	now := nowRFC3339()
	msg, err := s.db.AppendGroupMessage(store.GroupMessage{
		ID: newUUID(), Tenant: tenant, GroupID: ob.GroupID,
		Kind: store.GroupMessageKindSystem, Sender: "", SenderName: "",
		Body: body,
	}, now)
	if err != nil {
		slog.Warn("cannot store a reply reminder", "tenant", tenant, "session", session, "err", err)
		return
	}
	// DELIVER TO THE OBLIGED SESSION ONLY. It is the one being chased; waking the whole group would announce a
	// private shortfall to everyone, and would wake members with no part in it.
	s.deliverGroupMessage(ctx, tenant, ob.GroupID, g.Name, msg, []string{session})

	if _, err := s.db.BumpReminder(tenant, ob.ID, now); err != nil && !errors.Is(err, store.ErrNoObligation) {
		slog.Warn("cannot record a reply reminder", "tenant", tenant, "obligation", ob.ID, "err", err)
	}
	// THE GROUP'S ROW CHANGED: a reader watching the list should see this without a reload.
	s.notifyListChange(tenant, agentbus.SessionListChange{Kind: agentbus.ListKindGroup, Name: ob.GroupID})
}

/**
 * deliverGroupMessage posts one stored message onto a group's channel.
 *
 * IT EXISTS BECAUSE THERE ARE NOW TWO PRODUCERS: the RPC path (`sendGroupMessage`) and this reminder. Both must build
 * the same payload, because a woken session reads the prompt built from it and a field missing here is a field the
 * model never saw.
 */
func (s *Server) deliverGroupMessage(ctx context.Context, tenant, groupID, groupName string, msg store.GroupMessage, to []string) {
	if s.chanPub == nil || len(to) == 0 {
		return
	}
	payload := groupMessagePayload(msg, groupName, msg.Body)
	for _, rcpt := range to {
		if err := s.chanPub.PublishChannel(ctx, tenant, agentbus.GroupChannel,
			agentbus.GroupMemberTopic(groupID, rcpt), payload, "system:reply-reminder"); err != nil {
			slog.Warn("reply reminder delivery failed",
				"tenant", tenant, "group", groupID, "recipient", rcpt, "err", err)
		}
	}
}

// sessionNameFor resolves a session's display label, falling back to the id.
func sessionNameFor(s *Server, tenant, session string) string {
	if row, ok, _ := s.db.GetSession(tenant, session); ok && strings.TrimSpace(row.Name) != "" {
		return row.Name
	}
	return session
}

/**
 * replyReminderText is what the chased session reads.
 *
 * IT NAMES THE ASKER, THE GROUP AND THE COUNT. The count is not decoration: "reminder 3 of 3" tells the model this is
 * the last automatic attempt, so ignoring it is a decision rather than an accident. It also tells a reader of the
 * transcript how long this has been going on.
 */
func replyReminderText(locale, groupName, fromID, fromName string, attempt int) string {
	who := fromName
	if strings.TrimSpace(who) == "" {
		who = fromID
	}
	if strings.HasPrefix(strings.ToLower(locale), "zh") {
		return fmt.Sprintf(
			"【系统提醒 %d/%d】在群「%s」里，%s 向你提了一个请求（kind=request），你还没有答复。\n\n"+
				"请调用 group-send：group 填本群 id，to 填 [\"%s\"]，kind 填 \"reply\"，text 填你的答复。"+
				"这是第 %d 次自动提醒，最多 %d 次；超过之后系统不再提醒，这笔未回复会留在群里显示。",
			attempt, maxReplyReminders, groupName, who, fromID, attempt, maxReplyReminders,
		)
	}
	return fmt.Sprintf(
		"[System reminder %d/%d] In group %q, %s asked you something (kind=request) and you have not answered.\n\n"+
			"Call group-send with group = this group's id, to = [\"%s\"], kind = \"reply\", and your answer as text. "+
			"This is automatic reminder %d of %d; after that the system stops, and the outstanding request stays "+
			"visible in the group.",
		attempt, maxReplyReminders, groupName, who, fromID, attempt, maxReplyReminders,
	)
}

// ObligationsForGroup exposes a group's outstanding debts to the RPC layer.
func (s *Server) ObligationsForGroup(ctx context.Context, tenant, groupID string) ([]store.GroupReplyObligation, error) {
	return s.db.ObligationsForGroup(tenant, groupID)
}

// ListReplyObligations is the RPC behind the group's "who still owes an answer" panel.
func (s *Server) ListReplyObligations(ctx context.Context, req *agentv1.ListReplyObligationsRequest) (*agentv1.ListReplyObligationsResponse, error) {
	groupID := strings.TrimSpace(req.GetGroupId())
	if groupID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group_id is required")
	}
	rows, err := s.db.ObligationsForGroup(tenantOf(ctx), groupID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.ReplyObligation, 0, len(rows))
	for _, r := range rows {
		out = append(out, &agentv1.ReplyObligation{
			Id: r.ID, Session: r.Session, From: r.From, Seq: r.Seq,
			MessageId: r.MessageID, CreatedAt: r.CreatedAt,
			RemindCount: int32(r.RemindCount), RemindedAt: r.RemindedAt,
		})
	}
	return &agentv1.ListReplyObligationsResponse{Obligations: out}, nil
}
