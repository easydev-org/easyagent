package server

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easybus-sdk/bus"

	"github.com/easydev-org/easyagent/agentbus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * Sending a user message to a group.
 *
 * IT PUBLISHES TO THE GROUP'S CHANNEL rather than writing to the coordinator's mailbox directly.
 *
 * That is the whole reason a group is a channel and not an address book with a delivery method: the
 * message goes through `Distributor`, so waking, queueing while busy, and survival across a restart
 * are answered by the SAME code that handles every other channel message. A direct mailbox write would
 * have been a second delivery path with its own answers to those questions, and the first time the two
 * disagreed would be the first time a coordinator was silently not woken.
 *
 * THE CHANNEL CARRIES THE MESSAGE; THE SUBSCRIPTION DECIDES WHO HEARS IT. The coordinator's pattern is
 * `<group>.>`, so `in` reaches it and the members' patterns (`<group>.to.>`) do not match `in` at all.
 * No recipient list is computed here — that is the point of a pub/sub address.
 */
func (s *Server) SendGroupMessage(ctx context.Context, req *agentv1.SendGroupMessageRequest) (*agentv1.SendGroupMessageResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetGroup())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	text := strings.TrimSpace(req.GetText())
	if text == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "message text is required")
	}

	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}

	coordinator, err := s.db.GroupCoordinator(tenant, name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if coordinator == "" {
		// A group with no coordinator routes nowhere. Saying so is better than accepting the message
		// and publishing it to a channel nobody follows: the user would see a sent message and no
		// reply, with nothing to point at.
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Sprintf("group %q has no coordinator, so there is nowhere for this message to go", name))
	}
	if s.chanPub == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			"channels are not configured on this deployment, so a group has no route")
	}

	// THE PUBLISHER IS THE TENANT, NOT THE CALLER'S SESSION: a group message comes from the user, and
	// attributing it to a session would make the coordinator believe another session wrote to it.
	if err := s.chanPub.PublishChannel(ctx, tenant, agentbus.GroupChannel,
		agentbus.GroupUserTopic(name), groupMessageBody(name, text), "user:group"); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}

	return &agentv1.SendGroupMessageResponse{DeliveredTo: []string{coordinator}}, nil
}

/**
 * groupMessageBody is the payload the coordinator receives.
 *
 * THE GROUP NAME IS IN THE PAYLOAD, not only in the source field. The model reads the rendered text;
 * a source string it never sees would tell it nothing, and "a user wrote to some group" is not enough
 * to answer with.
 */
func groupMessageBody(group, text string) map[string]any {
	return map[string]any{"group": group, "text": text}
}

// channelsForGroup is the narrow view of the bus this file needs, so a test can supply a fake.
type channelsForGroup interface {
	PublishChannel(ctx context.Context, tenant, channel, topic string, payload any, source string) error
}

var _ channelsForGroup = (*bus.Channels)(nil)
