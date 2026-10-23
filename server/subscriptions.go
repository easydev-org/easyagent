package server

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/bus"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// Channel subscription RPCs.
//
// A subscription is what a session LISTENS TO. It is stored in a NATS KV bucket rather than
// the agent's database because three processes need to agree on it: every agent replica
// (which routes messages), the extension publishing, and the console. A KV watch gives all
// three the same view without coordination, and a restart rebuilds it from the snapshot.

// SubscriptionStore is the slice of the subscription machinery these RPCs need.
type SubscriptionStore interface {
	// List returns a session's subscriptions.
	List(tenant, session string) []bus.Subscription
	// Put writes one subscription, returning its id.
	Put(ctx context.Context, tenant, session, channel, pattern string, wake bool) (bus.Subscription, error)
	// Delete removes one subscription. Implementations must refuse to remove a system one.
	Delete(ctx context.Context, tenant, session, id string) error
}

// SetSubscriptions installs the subscription store.
func (s *Server) SetSubscriptions(st SubscriptionStore) { s.subs = st }

// ListSubscriptions returns what a session follows.
func (s *Server) ListSubscriptions(ctx context.Context, req *agentv1.ListSubscriptionsRequest) (*agentv1.ListSubscriptionsResponse, error) {
	tenant := tenantOf(ctx)
	if req.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session id is required")
	}
	if s.subs == nil {
		// No broker: a session follows nothing, which is the truthful answer rather than an
		// error.
		return &agentv1.ListSubscriptionsResponse{}, nil
	}
	subs := s.subs.List(tenant, req.GetId())
	out := make([]*agentv1.Subscription, 0, len(subs))
	for _, sub := range subs {
		out = append(out, &agentv1.Subscription{
			Id: sub.ID, Channel: sub.Channel, Pattern: sub.Pattern,
			Wake: sub.Wake, System: sub.System,
		})
	}
	return &agentv1.ListSubscriptionsResponse{Subscriptions: out}, nil
}

// SubscribeChannel adds one subscription.
func (s *Server) SubscribeChannel(ctx context.Context, req *agentv1.SubscribeChannelRequest) (*agentv1.SubscribeChannelResponse, error) {
	tenant := tenantOf(ctx)
	if req.GetId() == "" || req.GetChannel() == "" || req.GetPattern() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"session id, channel and pattern are required")
	}
	if s.subs == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			"channels are not configured on this deployment")
	}
	// The pattern is matched against topics by the agent, and a malformed one would silently
	// match nothing. Checking it here means the user is told at subscription time.
	if err := validatePattern(req.GetPattern()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}
	sub, err := s.subs.Put(ctx, tenant, req.GetId(), req.GetChannel(), req.GetPattern(), req.GetWake())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.SubscribeChannelResponse{Subscription: &agentv1.Subscription{
		Id: sub.ID, Channel: sub.Channel, Pattern: sub.Pattern, Wake: sub.Wake, System: sub.System,
	}}, nil
}

// UnsubscribeChannel removes one subscription.
//
// The built-in one is refused: it is how anything reaches the session by name, and removing
// it would make the session unreachable with no way back through this API.
func (s *Server) UnsubscribeChannel(ctx context.Context, req *agentv1.UnsubscribeChannelRequest) (*agentv1.UnsubscribeChannelResponse, error) {
	tenant := tenantOf(ctx)
	if req.GetId() == "" || req.GetSubscriptionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"session id and subscription id are required")
	}
	if s.subs == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			"channels are not configured on this deployment")
	}
	// Check ownership and system-ness from the store's own view, not from the request: a
	// client that guessed an id must not be able to remove another session's subscription.
	var found *bus.Subscription
	for _, sub := range s.subs.List(tenant, req.GetId()) {
		if sub.ID == req.GetSubscriptionId() {
			cp := sub
			found = &cp
			break
		}
	}
	if found == nil {
		return nil, connect.NewError(connect.CodeNotFound, "no such subscription")
	}
	if found.System {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			"the built-in session-channel subscription cannot be removed")
	}
	if err := s.subs.Delete(ctx, tenant, req.GetId(), req.GetSubscriptionId()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.UnsubscribeChannelResponse{}, nil
}

// ListChannels publishes what the discovered extensions emit.
func (s *Server) ListChannels(ctx context.Context, _ *agentv1.ListChannelsRequest) (*agentv1.ListChannelsResponse, error) {
	if s.tools == nil {
		return &agentv1.ListChannelsResponse{}, nil
	}
	byExt := s.tools.AllChannels()
	extIDs := make([]string, 0, len(byExt))
	for id := range byExt {
		extIDs = append(extIDs, id)
	}
	sort.Strings(extIDs)

	out := make([]*agentv1.ChannelDecl, 0, len(byExt))
	for _, id := range extIDs {
		for _, c := range byExt[id] {
			topics := make([]*agentv1.ChannelTopicDecl, 0, len(c.Topics))
			for _, t := range c.Topics {
				topics = append(topics, &agentv1.ChannelTopicDecl{
					Pattern: t.Pattern, Description: t.Description, Descriptions: t.Descriptions,
				})
			}
			out = append(out, &agentv1.ChannelDecl{
				ExtId: id, Name: c.Name, Description: c.Description,
				Descriptions: c.Descriptions, Topics: topics,
			})
		}
	}
	return &agentv1.ListChannelsResponse{Channels: out}, nil
}

// validatePattern rejects a subscription pattern that could never match, or that would match
// far more than the user meant.
//
//	">" must be last, and only one is allowed
//	"*" and ">" must each be a whole segment
//	no empty segments
func validatePattern(p string) error {
	if strings.Contains(p, " ") {
		return fmt.Errorf("pattern %q must not contain spaces", p)
	}
	segs := strings.Split(p, ".")
	for i, seg := range segs {
		if seg == "" {
			return fmt.Errorf("pattern %q has an empty segment", p)
		}
		if seg == ">" {
			if i != len(segs)-1 {
				return fmt.Errorf("pattern %q: '>' must be the last segment", p)
			}
			continue
		}
		if strings.ContainsAny(seg, "*>") && seg != "*" {
			return fmt.Errorf("pattern %q: '%s' must be a whole segment", p, seg)
		}
	}
	return nil
}
