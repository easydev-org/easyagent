package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/agentbus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * ASK A SESSION A QUESTION, WITHOUT STARTING A TURN.
 *
 * A PERSON WATCHING A GROUP often wants to know one thing: "why did you do that", "what are you about to run", "which
 * file are you editing". Today the only way to ask is to post to the group, which STARTS A TURN — the session stops
 * what it was doing, reads the message, and answers in the transcript. The question changed the thing it was asking
 * about.
 *
 * SO THIS IS A SIDE CHANNEL. It reads the session's history, sends it to the model with NO TOOLS and one step, and
 * returns the answer. Nothing is written to the session: not a message, not a step, not a read watermark. A session
 * mid-turn is unaffected — this does not touch its mailbox or its lease.
 *
 * WHAT IT SEES IS THE PERSISTED CHAIN, NOT A LIVE VIEW. A turn's in-progress step is buffered in memory and written
 * only when the step ends, so anything still being produced is invisible here. That is deliberate and it is the honest
 * boundary: a question about work in progress would otherwise race the work, and half a tool call is not context.
 *
 * IT IS NOT A SESSION. It has no address, no mailbox, no lease, no grants and no tools. Treating it as one would mean
 * a second thing that can be woken, and the answer belongs in the group where the question was asked.
 */

// ErrAskNothingToAsk reports an empty question.
var ErrAskNothingToAsk = errors.New("a question is required")

// AskAnswer is one side-channel answer.
type AskAnswer struct {
	// SessionID is the session that was asked, for the caller to attribute the answer.
	SessionID string
	// SessionName is its display label at the time of asking.
	SessionName string
	// Text is the model's answer.
	Text string
}

/**
 * askSession sends one question to a session's context and returns the answer.
 *
 * THE SYSTEM PROMPT IS THE SESSION'S OWN, so the answer is written by the same voice a reader already sees in the
 * group — but the env block is NOT rebuilt from the current time and grants. Those describe what the session may DO,
 * and this call may do nothing; including them would invite an answer about capabilities rather than about the work.
 *
 * ONE STEP, NO TOOLS. `MaxTurns: 1` makes the turn loop drop the tool list on its only step, which is exactly the
 * shape wanted: the model can only answer in prose.
 */
func (s *Server) askSession(ctx context.Context, tenant, sessionID, question string) (AskAnswer, error) {
	question = strings.TrimSpace(question)
	if question == "" {
		return AskAnswer{}, ErrAskNothingToAsk
	}
	row, ok, err := s.db.GetSession(tenant, sessionID)
	if err != nil {
		return AskAnswer{}, err
	}
	if !ok {
		return AskAnswer{}, errors.New("session not found")
	}
	if row.ModelRef() == "" {
		return AskAnswer{}, fmt.Errorf("session %s has no model, so it cannot answer", sessionID)
	}
	if s.rt == nil {
		return AskAnswer{}, errors.New("the runtime is not configured")
	}

	answer, err := s.rt.AskOnce(ctx, tenant, *row, question)
	if err != nil {
		return AskAnswer{}, err
	}
	return AskAnswer{SessionID: sessionID, SessionName: row.Name, Text: answer}, nil
}

/**
 * PublishAskAnswer writes the answer into the group as a SYSTEM message, not a session's.
 *
 * WHO WROTE IT IS THE QUESTION. The words came from the model, but the model answered a question the READER posed —
 * attributing it to the session would put a message in the transcript that the session never sent and cannot explain,
 * and a reader scrolling back would believe the session had volunteered it. `kind = "system"` says what it is: the
 * platform relaying an answer.
 *
 * IT NAMES THE SESSION IT ASKED, in the body, because the group may have several members and "the following was asked
 * of X" is the difference between an answer and an assertion.
 *
 * IT IS STORED AND DELIVERED TO NOBODY: no member owes a reply to it, nobody is woken by it, and it opens no
 * obligation. A question a person asked in passing must not hand work to a session.
 */
func (s *Server) PublishAskAnswer(ctx context.Context, tenant, groupID, sessionID, sessionName, question, answer string) error {
	g, ok, err := s.db.GetGroup(tenant, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return store.ErrGroupNotFound
	}
	locale := ""
	if row, ok, _ := s.db.GetSession(tenant, sessionID); ok {
		locale = row.Locale
	}
	body := askAnswerText(locale, sessionName, sessionID, question, answer)
	msg, err := s.db.AppendGroupMessage(store.GroupMessage{
		ID: newUUID(), Tenant: tenant, GroupID: groupID,
		Kind: store.GroupMessageKindSystem,
		Body: body,
	}, nowRFC3339())
	if err != nil {
		return err
	}
	s.publishGroupMessage(ctx, tenant, groupID, "", nil, msg)
	s.notifyListChange(tenant, agentbus.SessionListChange{Kind: agentbus.ListKindGroup, Name: groupID})
	_ = g
	return nil
}

/**
 * askAnswerText frames the relayed answer.
 *
 * THE QUESTION IS ECHOED. Without it the transcript shows an answer to something nobody can see, and a reader arriving
 * later has no way to know what was asked — the same failure as a reply with no quote.
 */
func askAnswerText(locale, sessionName, sessionID, question, answer string) string {
	who := strings.TrimSpace(sessionName)
	if who == "" {
		who = sessionID
	}
	if strings.HasPrefix(strings.ToLower(locale), "zh") {
		return fmt.Sprintf("【提问 · %s】\n\n问：%s\n\n答：%s", who, question, answer)
	}
	return fmt.Sprintf("[Asked · %s]\n\nQ: %s\n\nA: %s", who, question, answer)
}

// Ask is the RPC: one question, one answer, nothing persisted.
func (s *Server) Ask(ctx context.Context, req *agentv1.AskRequest) (*agentv1.AskResponse, error) {
	res, err := s.askSession(ctx, tenantOf(ctx), strings.TrimSpace(req.GetId()), req.GetQuestion())
	if err != nil {
		return nil, askError(err)
	}
	return &agentv1.AskResponse{SessionId: res.SessionID, SessionName: res.SessionName, Answer: res.Text}, nil
}

/**
 * AskInGroup asks and posts the answer into a group.
 *
 * MEMBERSHIP IS CHECKED. A group's page may interrogate the work it is showing; it must not be able to reach into an
 * arbitrary session of the tenant, which would make this an oracle for any session's history.
 */
func (s *Server) AskInGroup(ctx context.Context, req *agentv1.AskInGroupRequest) (*agentv1.AskInGroupResponse, error) {
	tenant := tenantOf(ctx)
	groupID := strings.TrimSpace(req.GetGroupId())
	sessionID := strings.TrimSpace(req.GetSessionId())
	if groupID == "" || sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group_id and session_id are required")
	}
	member, err := s.db.IsGroupMember(tenant, groupID, sessionID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !member {
		// NOT_FOUND, so a caller cannot use this to learn which sessions exist.
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}

	res, err := s.askSession(ctx, tenant, sessionID, req.GetQuestion())
	if err != nil {
		return nil, askError(err)
	}
	if err := s.PublishAskAnswer(ctx, tenant, groupID, sessionID, res.SessionName, strings.TrimSpace(req.GetQuestion()), res.Text); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// READ BACK THE MESSAGE so the caller can render it without a second call.
	latest, err := s.db.LatestGroupSeq(tenant, groupID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	rows, err := s.db.ListGroupMessages(tenant, groupID, latest-1, 1)
	if err != nil || len(rows) == 0 {
		return &agentv1.AskInGroupResponse{Answer: res.Text}, nil
	}
	return &agentv1.AskInGroupResponse{Message: groupMessageToMsg(rows[0]), Answer: res.Text}, nil
}

/**
 * askError maps the side channel's failures onto RPC codes.
 *
 * A MISSING QUESTION IS THE CALLER'S MISTAKE; a missing session or model is NOT_FOUND/FAILED_PRECONDITION. Everything
 * else is internal — notably a provider error, which is the model's problem and not a bad request.
 */
func askError(err error) error {
	switch {
	case errors.Is(err, ErrAskNothingToAsk):
		return connect.NewError(connect.CodeInvalidArgument, err.Error())
	case strings.Contains(err.Error(), "session not found"):
		return connect.NewError(connect.CodeNotFound, err.Error())
	case strings.Contains(err.Error(), "has no model"):
		return connect.NewError(connect.CodeFailedPrecondition, err.Error())
	default:
		return connect.NewError(connect.CodeInternal, err.Error())
	}
}
