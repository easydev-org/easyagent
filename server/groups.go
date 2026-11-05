package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect/v2"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * Groups: a named set of sessions that can see each other.
 *
 * CREATED BY USERS ONLY. There is no group RPC that a session or an extension can drive into
 * existence: the console calls these, and the session-facing path is read-only (`GetGroup` with the
 * caller's own name, and `ListGroups` filtered to the caller's memberships). That asymmetry is the
 * security boundary — a group is a view of the tenant ("who is working with whom"), and an extension
 * that could create one could assemble a session set and read its membership back.
 *
 * MEMBERSHIP IS THE ONLY PERMISSION. A session asking about a group it is not in gets NOT_FOUND,
 * never PERMISSION_DENIED: the second answer confirms the group exists, which would let any session
 * enumerate the tenant by watching which names fail differently.
 */

// validateGroupName keeps a group name usable as a subject segment.
//
// The same rule a session name obeys, for the same reason: a group is addressed by name on a channel,
// and a dot would silently change the subject's shape ("a.b" reads as two segments) while ">" would
// match everything.
func validateGroupName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("group name is required")
	}
	if len(name) > 128 {
		return fmt.Errorf("group name is limited to 128 characters")
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '_' || r == '-' || r == '.'
		if !ok {
			return fmt.Errorf("group name %q may only contain letters, digits, '_', '-' and '.'", name)
		}
	}
	if name[0] == '.' || name[len(name)-1] == '.' {
		return fmt.Errorf("group name must not start or end with '.'")
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("group name must not contain an empty segment")
	}
	return nil
}

// groupToMsg renders a group with its member count.
func groupToMsg(g store.Group, memberCount int) *agentv1.Group {
	return &agentv1.Group{
		Name:        g.Name,
		AvatarFile:  g.AvatarFile,
		CreatedBy:   g.CreatedBy,
		CreatedAt:   g.CreatedAt,
		MemberCount: int32(memberCount),
	}
}

func memberToMsg(m store.GroupMember) *agentv1.GroupMember {
	role := m.Role
	if role == "" {
		role = store.GroupRoleMember
	}
	return &agentv1.GroupMember{
		Session: m.Session, Role: role, JoinedAt: m.JoinedAt, JoinedBy: m.JoinedBy,
	}
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

/**
 * syncGroupSubscriptions rewrites every member's channel subscription from the member list.
 *
 * CALLED AFTER EVERY MEMBERSHIP OR ROLE CHANGE, because the subscription is DERIVED from membership and
 * a derived thing that is not re-derived is just a stale copy. A promoted coordinator that kept a
 * member's pattern would never receive the group's user messages, and nothing would say so.
 *
 * FAILURES ARE REPORTED, NOT SWALLOWED, but they do not undo the membership: the member list is the
 * source of truth and the subscription is what catches up. A caller that lost the membership write
 * because of a broker hiccup would have a worse problem than a subscription that is one edit behind.
 */
func (s *Server) syncGroupSubscriptions(ctx context.Context, tenant, group string, previous []string) error {
	if s.channels == nil {
		return nil
	}
	members, err := s.db.ListGroupMembers(tenant, group)
	if err != nil {
		return err
	}
	bySession := make(map[string]bool, len(members))
	for _, m := range members {
		bySession[m.Session] = m.Role == store.GroupRoleCoordinator
	}
	return s.channels.SyncGroup(ctx, tenant, group, bySession, previous)
}

/**
 * requireSessionsExist checks that every named session is real, before anything is written.
 *
 * IT IS THE SAME RULE IN THREE PLACES — creating a group, adding members, and (in the tool layer) picking
 * a worker — and it is checked UP FRONT rather than per-row: a member list naming a session that is not
 * there would show a participant who can never answer, and nothing in the API would ever remove it.
 */
func (s *Server) requireSessionsExist(tenant string, sessions []string) error {
	for _, m := range sessions {
		if m == "" {
			continue
		}
		if _, ok, err := s.db.GetSession(tenant, m); err != nil {
			return connect.NewError(connect.CodeInternal, err.Error())
		} else if !ok {
			return connect.NewError(connect.CodeNotFound,
				fmt.Sprintf("session %q does not exist", m))
		}
	}
	return nil
}

// CreateGroup makes a group and enrolls its members.
func (s *Server) CreateGroup(ctx context.Context, req *agentv1.CreateGroupRequest) (*agentv1.CreateGroupResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetName())
	if err := validateGroupName(name); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
	}

	coordinator := strings.TrimSpace(req.GetCoordinator())
	if coordinator == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			"a group needs exactly one coordinator: the session that receives the group's messages")
	}

	// EVERY MEMBER IS CHECKED BEFORE THE GROUP EXISTS, so a name typo fails fast instead of producing
	// a group whose member list silently omits the session the user meant to add. The coordinator is
	// checked with the rest: it is a member, just one with a different routing role.
	toCheck := append([]string{coordinator}, req.GetMembers()...)
	if err := s.requireSessionsExist(tenant, toCheck); err != nil {
		return nil, err
	}

	now := nowRFC3339()
	g := store.Group{
		Name: name, AvatarFile: req.GetAvatarFile(),
		CreatedBy: req.GetCreatedBy(), CreatedAt: now, UpdatedAt: now,
	}
	if err := s.db.CreateGroup(tenant, g, coordinator, req.GetMembers(), now); err != nil {
		switch {
		case errors.Is(err, store.ErrGroupExists):
			return nil, connect.NewError(connect.CodeAlreadyExists, err.Error())
		case errors.Is(err, store.ErrAlreadyCoordinating):
			// A PRECONDITION OF THE SESSION, not of the request: it is well-formed, and it would succeed
			// once the other group is cleared up. Saying that is more useful than "conflict".
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				"that session already coordinates a group; a session may coordinate only one")
		}
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.syncGroupSubscriptions(ctx, tenant, name, nil); err != nil {
		// THE GROUP EXISTS AND IS CORRECT; only its routing is not established yet. Reported rather
		// than ignored so the operator can re-run, and not rolled back because deleting a group the user
		// just made is a worse outcome than a routing delay.
		return nil, connect.NewError(connect.CodeInternal,
			"group created but its subscriptions could not be written: "+err.Error())
	}
	// THE COUNT IS PART OF THE ANSWER, not decoration: `member_count: 0` on a group that HAS members is a
	// lie the client cannot detect, and it would render an empty member list on a group the user just made.
	members, err := s.db.ListGroupMembers(tenant, name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	return &agentv1.CreateGroupResponse{
		Group: groupToMsg(g, len(members)),
	}, nil
}

// ListGroups returns the groups the caller may see.
//
// A SESSION SEES ONLY ITS OWN. `req.session` is the whole filter: a session has no reason to
// enumerate groups it is not in, and nothing in the tenant's structure should reach it through a list
// it can call on a whim. An EMPTY `session` is the console's administrative view.
func (s *Server) ListGroups(ctx context.Context, req *agentv1.ListGroupsRequest) (*agentv1.ListGroupsResponse, error) {
	tenant := tenantOf(ctx)
	var (
		groups []store.Group
		err    error
	)
	if req.GetSession() != "" {
		groups, err = s.db.GroupsOfSession(tenant, req.GetSession())
	} else {
		groups, err = s.db.ListGroups(tenant)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}

	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}
	counts, err := s.db.GroupMemberCounts(tenant, names)
	if err != nil {
		// COUNTS ARE DECORATION. Losing them must not empty the list: an unreadable count is worth a
		// wrong number, not a blank page.
		counts = map[string]int{}
	}

	out := make([]*agentv1.Group, 0, len(groups))
	for _, g := range groups {
		out = append(out, groupToMsg(g, counts[g.Name]))
	}
	return &agentv1.ListGroupsResponse{Groups: out}, nil
}

// GetGroup returns a group and its members.
func (s *Server) GetGroup(ctx context.Context, req *agentv1.GetGroupRequest) (*agentv1.GetGroupResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}

	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}

	// A SESSION MUST BE A MEMBER; the console may ask for any group. NOT_FOUND rather than
	// PERMISSION_DENIED, so the answer does not confirm the group exists.
	if sid := req.GetSession(); sid != "" {
		member, err := s.db.IsGroupMember(tenant, name, sid)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		if !member {
			return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
		}
	}

	// THE SAME ARM EVERY OTHER RPC USES, so "the group as it now stands" is assembled in one place and a
	// change to what a group response carries cannot land in three of four handlers.
	gm, members, err := s.groupWithMembers(ctx, tenant, name)
	if err != nil {
		return nil, err
	}
	return &agentv1.GetGroupResponse{Group: gm, Members: members}, nil
}

// UpdateGroup changes the group's picture.
func (s *Server) UpdateGroup(ctx context.Context, req *agentv1.UpdateGroupRequest) (*agentv1.UpdateGroupResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}

	// `optional` IS THE POINT, exactly as on a session's avatar: absent = leave alone,
	// present-but-empty = clear, which is how a group goes back to its derived identicon.
	if req.AvatarFile != nil {
		if err := s.db.UpdateGroupAvatar(tenant, name, req.GetAvatarFile(), nowRFC3339()); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
	}

	gm, _, err := s.groupWithMembers(ctx, tenant, name)
	if err != nil {
		return nil, err
	}
	return &agentv1.UpdateGroupResponse{Group: gm}, nil
}

// DeleteGroup removes a group and its memberships.
func (s *Server) DeleteGroup(ctx context.Context, req *agentv1.DeleteGroupRequest) (*agentv1.DeleteGroupResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group name is required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	// UNSUBSCRIBE BEFORE THE ROWS GO: the member list is what says WHO to unsubscribe, so reading it
	// after the delete would find nobody and leave every member still listening to a group that no
	// longer exists.
	previous, err := s.db.SessionsInGroupIDs(tenant, name, "")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.db.DeleteGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if s.channels != nil {
		// A GROUP THAT STILL ROUTES AFTER ITS DELETION IS A LIE THE USER CANNOT SEE: members keep receiving
		// messages from a group the console says is gone. Reported rather than swallowed; the group rows
		// are already deleted, so re-running the delete repairs the subscriptions.
		if err := s.channels.DropGroup(ctx, tenant, name, previous); err != nil {
			return nil, connect.NewError(connect.CodeInternal,
				"group deleted but its subscriptions remain: "+err.Error())
		}
	}
	return &agentv1.DeleteGroupResponse{}, nil
}

// AddGroupMember enrolls sessions.
func (s *Server) AddGroupMember(ctx context.Context, req *agentv1.AddGroupMemberRequest) (*agentv1.AddGroupMemberResponse, error) {
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

	if err := s.requireSessionsExist(tenant, req.GetMembers()); err != nil {
		return nil, err
	}

	addedBy := req.GetAddedBy()
	if addedBy == "" {
		addedBy = "user"
	}
	previous, err := s.db.SessionsInGroupIDs(tenant, name, "")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.db.AddGroupMembers(tenant, name, req.GetMembers(), nowRFC3339(), addedBy); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.syncGroupSubscriptions(ctx, tenant, name, previous); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			"members added but their subscriptions could not be written: "+err.Error())
	}
	g, members, err := s.groupWithMembers(ctx, tenant, name)
	if err != nil {
		return nil, err
	}
	return &agentv1.AddGroupMemberResponse{Group: g, Members: members}, nil
}

// RemoveGroupMember drops one membership.
func (s *Server) RemoveGroupMember(ctx context.Context, req *agentv1.RemoveGroupMemberRequest) (*agentv1.RemoveGroupMemberResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetGroup())
	member := strings.TrimSpace(req.GetMember())
	if name == "" || member == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group and member are required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	if err := s.db.RemoveGroupMember(tenant, name, member); err != nil {
		switch {
		case errors.Is(err, store.ErrNotGroupMember):
			return nil, connect.NewError(connect.CodeNotFound, err.Error())
		case errors.Is(err, store.ErrCoordinatorRequired):
			// FAILED_PRECONDITION, not INVALID_ARGUMENT: the request is well-formed, the group is in a
			// state that cannot take it. The caller moves the role first.
			return nil, connect.NewError(connect.CodeFailedPrecondition, err.Error())
		}
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if err := s.syncGroupSubscriptions(ctx, tenant, name, nil); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			"member removed but their subscription could not be dropped: "+err.Error())
	}
	g, members, err := s.groupWithMembers(ctx, tenant, name)
	if err != nil {
		return nil, err
	}
	return &agentv1.RemoveGroupMemberResponse{Group: g, Members: members}, nil
}

/**
 * SetGroupCoordinator moves the routing role to another existing member.
 *
 * THE NEW COORDINATOR MUST ALREADY BE A MEMBER. Promoting a stranger would add a session to the group
 * as a side effect of naming it, which is the opposite of what an explicit member list is for: a user
 * cannot then tell "who did I add" from "who did I promote".
 */
func (s *Server) SetGroupCoordinator(ctx context.Context, req *agentv1.SetGroupCoordinatorRequest) (*agentv1.SetGroupCoordinatorResponse, error) {
	tenant := tenantOf(ctx)
	name := strings.TrimSpace(req.GetGroup())
	session := strings.TrimSpace(req.GetSession())
	if name == "" || session == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "group and session are required")
	}
	if _, ok, err := s.db.GetGroup(tenant, name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	} else if !ok {
		return nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	if err := s.db.SetGroupCoordinator(tenant, name, session); err != nil {
		if errors.Is(err, store.ErrAlreadyCoordinating) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				"that session already coordinates a group; a session may coordinate only one")
		}
		if errors.Is(err, store.ErrNotGroupMember) {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				"the new coordinator must already be a member of the group")
		}
		return nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	// THE PATTERN DEPENDS ON THE ROLE, so a role change is also a routing change: the promoted session
	// must start seeing the group's user messages and the demoted one must stop.
	if err := s.syncGroupSubscriptions(ctx, tenant, name, nil); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			"role changed but the subscriptions could not be rewritten: "+err.Error())
	}
	g, members, err := s.groupWithMembers(ctx, tenant, name)
	if err != nil {
		return nil, err
	}
	return &agentv1.SetGroupCoordinatorResponse{Group: g, Members: members}, nil
}

/**
 * groupWithMembers is the shared "return the group as it now stands" arm.
 *
 * IT RETURNS THE PIECES, not a response message: add and remove answer with different request types
 * but the same content, and building one message here would have forced one of the two callers to
 * copy its fields into the other type.
 */
func (s *Server) groupWithMembers(ctx context.Context, tenant, name string) (*agentv1.Group, []*agentv1.GroupMember, error) {
	g, ok, err := s.db.GetGroup(tenant, name)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	if !ok {
		return nil, nil, connect.NewError(connect.CodeNotFound, store.ErrGroupNotFound.Error())
	}
	members, err := s.db.ListGroupMembers(tenant, name)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, err.Error())
	}
	out := make([]*agentv1.GroupMember, 0, len(members))
	for _, m := range members {
		out = append(out, memberToMsg(m))
	}
	return groupToMsg(*g, len(members)), out, nil
}
