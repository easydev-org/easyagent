package agentbus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/easydev-org/easybus-sdk/protocol"
)

/**
 * Group traffic rides the SAME channel mechanism as everything else.
 *
 * THIS IS NOT A STYLE CHOICE. `Distributor` exists so that a message's arrival, wake decision, queueing
 * while busy, and survival across a restart are answered in exactly one place. A group that delivered
 * its messages by another route would need its own answers to all four, and the two sets would drift —
 * the first time they disagree is the first time a member is silently not woken.
 *
 * A GROUP IS THEREFORE AN ADDRESS PLUS A SUBSCRIPTION LIST, not a second message store.
 *
 * TWO TOPICS, so that "the user wrote to the group" and "the coordinator addressed one member" are
 * distinguishable by the wire rather than by parsing text:
 *
 *   <group>.in                the user's message. The coordination sees it; nobody else does.
 *   <group>.to.<session>      the coordinator addressing one member. That member sees it.
 *
 * "@" IS NOT IN THE TOPIC. It is how a coordinator says "address this member" in its own reasoning, and
 * it is rendered as `to.<session>` on the wire: the session name is already the unambiguous segment,
 * and "@" is not a legal subject character.
 */
const (
	// GroupChannel is the channel name every group publishes to, so a subscriber's channel is constant
	// and only the pattern distinguishes groups.
	GroupChannel = "group"
	// GroupTopicUser is the topic segment a user's message to the group arrives on.
	GroupTopicUser = "in"
	// GroupTopicToPrefix introduces a topic addressed to one member.
	GroupTopicToPrefix = "to"
)

// GroupUserTopic is the topic a user's message to a group arrives on.
//
// NOT `<group>.>`: a bare group name would match the addressed topics too, so a member subscribing to
// everything in the group would react to a message meant for someone else. The `in` segment keeps the
// two shapes disjoint.
func GroupUserTopic(group string) string { return group + "." + GroupTopicUser }

// GroupMemberTopic is the topic that addresses one member.
func GroupMemberTopic(group, session string) string {
	return group + "." + GroupTopicToPrefix + "." + session
}

/**
 * GroupPattern returns the subscription pattern for one member.
 *
 * THE COORDINATOR SEES THE WHOLE GROUP, A MEMBER SEES ONLY ITS OWN NAME.
 *
 * The coordinator needs both halves: the user's instruction and the members' replies. An ordinary
 * member deliberately gets neither — being addressed is the only thing that should start a turn.
 *
 * THE MEMBER'S PATTERN NAMES THE SESSION, not `to.>`. That is a correctness requirement rather than
 * tidiness: `to.>` matches every member's addressed topic, so each member would react to work handed to
 * someone else. The name is the only thing that makes "addressed to me" expressible in a subject.
 */
func GroupPattern(group, session string, coordinator bool) string {
	if coordinator {
		// `<group>.>` matches `in` AND `to.<session>`, which is what the coordinator needs: the user's
		// instruction and the members' replies.
		//
		// IT ALSO MATCHES THE COORDINATOR'S OWN DISPATCHES (`to.<member>`), and that self-echo is dropped in
		// the distributor rather than here: a narrower pattern would lose the replies, which is the half it
		// cannot do without. See `Distributor.deliverGroup`.
		return group + ".>"
	}
	return GroupMemberTopic(group, session)
}

// GroupSubscriptionID is the stable id of one session's group subscription.
//
// DETERMINISTIC, so re-provisioning after a membership edit updates the existing entry rather than
// adding a second one that matches the same messages — which would deliver every message twice.
func GroupSubscriptionID(group string) string {
	return "group-" + protocol.EscapeKVSegment(group)
}

/**
 * EnsureGroupSubscription makes a session follow its group.
 *
 * CALLED ON EVERY MEMBERSHIP CHANGE, including the ones that only alter a role: a member promoted to
 * coordinator must START seeing the group's user messages, and that is expressed by the pattern, not by
 * a flag somewhere else.
 *
 * Wake is TRUE for both roles. The distinction is not "wakes or not" but WHICH MESSAGES ARRIVE: a
 * member's subscription only matches its own addressed topic, so waking on it is the whole point.
 */
func EnsureGroupSubscription(ctx context.Context, kv kvPutter, tenant, group, session string, coordinator bool) error {
	if kv == nil {
		return nil
	}
	val, err := json.Marshal(subscriptionValue{
		Channel: GroupChannel,
		Pattern: GroupPattern(group, session, coordinator),
		Wake:    true,
		// NOT System: this one is derived from membership and must disappear when the membership does.
		// The built-in session-channel is System because nothing a user does may remove it; a group
		// subscription is the opposite — it is exactly as durable as the member list.
		System: false,
	})
	if err != nil {
		return err
	}
	return kv.KVPut(ctx, protocol.ChannelKVBucket,
		protocol.SubscriptionKVKey(tenant, session, GroupSubscriptionID(group)), string(val), 0)
}

// RemoveGroupSubscription drops a session's subscription to a group.
//
// Best-effort by design: a stale subscription that never matches is harmless, while failing a
// membership removal because the broker was briefly unavailable would leave the member list and the
// routing disagreeing — the failure that matters is the one users can see.
func RemoveGroupSubscription(ctx context.Context, kv kvPutter, tenant, group, session string) error {
	if kv == nil {
		return nil
	}
	return kv.KVDelete(ctx, protocol.ChannelKVBucket,
		protocol.SubscriptionKVKey(tenant, session, GroupSubscriptionID(group)))
}

/**
 * RewriteGroupSubscriptions brings every member's subscription in line with the member list.
 *
 * IT IS A FULL REWRITE rather than a delta, and that is the point: the member list is the source of
 * truth, and a provisioner that only applies "what changed" has to be told the change correctly. A
 * rewrite is idempotent, so the caller can run it after every edit without reasoning about which edit
 * it was, and the result is the same whether the process just started or has been running for a week.
 *
 * `members` carries every session AND whether it is the coordinator. Sessions no longer present are
 * unsubscribed, which is what makes leaving a group actually stop the messages.
 */
func RewriteGroupSubscriptions(ctx context.Context, kv kvPutter, tenant, group string, members map[string]bool, previous []string) error {
	if kv == nil {
		return nil
	}
	var firstErr error
	for session, coordinator := range members {
		if err := EnsureGroupSubscription(ctx, kv, tenant, group, session, coordinator); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, session := range previous {
		if _, still := members[session]; still {
			continue
		}
		if err := RemoveGroupSubscription(ctx, kv, tenant, group, session); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GroupSubscriptionError describes a group whose name cannot be a channel pattern.
func GroupSubscriptionError(group string) error {
	if !sessionNameSegment.MatchString(group) {
		return fmt.Errorf("group name %q must match %s to be addressable on a channel",
			group, sessionNameSegment.String())
	}
	return nil
}
