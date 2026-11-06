package server

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * A SESSION A PERSON MADE GETS A ROOM, and it is the room that session coordinates.
 *
 * THE POINT IS NOT THE ROW. A group page is where a person WATCHES work — plain conversation, no tool cards — and a
 * session transcript is where they INSPECT it. Without a room of its own there was nowhere to do the first, so
 * supervising a session meant reading its machinery.
 *
 * THE RESPONSE CARRIES THE ID because the caller needs it: matching on the label afterward would be the lookup the
 * id exists to remove, and labels are not unique.
 */
func TestCreatingASessionGivesItAGroup(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	res, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "协调者", Model: "gw/m"})
	if err != nil {
		t.Fatal(err)
	}
	if res.GroupId == "" {
		t.Fatal("the session was created without a group")
	}

	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: res.GroupId})
	if err != nil {
		t.Fatalf("the group the response named does not exist: %v", err)
	}
	// THE GROUP IS NAMED AFTER THE SESSION, because that is what a person recognises in a list.
	if got.Group.Name != "协调者" {
		t.Errorf("group name = %q, want the session's label", got.Group.Name)
	}
	// AND THE SESSION COORDINATES IT, which is what makes the room a place to give orders rather than a chat room
	// nobody is responsible for.
	if len(got.Members) != 1 || got.Members[0].Session != res.SessionId {
		t.Fatalf("members = %+v, want only the session", got.Members)
	}
	if got.Members[0].Role != string(bundled.GroupRoleCoordinator) {
		t.Errorf("role = %q, want coordinator", got.Members[0].Role)
	}
}

/**
 * THE ROOM CARRIES THE GROUP PERMISSIONS, SCOPED TO ITSELF.
 *
 * A session's own room is where a person types instructions, so the session may put workers to work there without a
 * separate grant — `group-create` then `spawn` is the pair the tools describe, and refusing the second would make the
 * first pointless.
 *
 * THE SCOPE IS THE ASSERTION. The grant must name this group and no other: a bare `group.spawn` would be authority
 * over every group the session can see, which is strictly more than was given.
 */
func TestASessionsOwnGroupCarriesScopedGroupPermissions(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	res, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s", Model: "gw/m"})
	if err != nil {
		t.Fatal(err)
	}

	grants, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: res.SessionId})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"group.create": false, "group.spawn": false, "group.send": false}
	for _, g := range grants.Grants {
		if g.ExtId != "bundled" {
			continue
		}
		if _, ok := want[g.Capability]; !ok {
			continue
		}
		if g.Scope != store.GroupScope(res.GroupId) {
			continue
		}
		want[g.Capability] = true
	}
	for cap, held := range want {
		if !held {
			t.Errorf("%s is not granted for the session's own group", cap)
		}
	}

	// AND NOTHING UNSCOPED WAS GRANTED: the room is the limit of the extra authority.
	for _, g := range grants.Grants {
		if g.ExtId == "bundled" && g.Scope == "" {
			t.Errorf("an unscoped %q grant was created, which reaches every group", g.Capability)
		}
	}
}

/**
 * A USER-MADE GROUP GRANTS NOTHING TO ITS COORDINATOR.
 *
 * THIS IS THE DISTINCTION THE SCOPE EXISTS FOR. A person creating a group and putting a session in charge of it is
 * saying "receive their reports and hand out work THERE". It is not saying "you may create more sessions inside my
 * group" — that is a different decision, and one a person makes explicitly.
 *
 * A CHECK THAT IGNORED SCOPE WOULD MAKE THESE THE SAME, and every coordinator of every group would be able to
 * spawn — which is the state the per-group permissions were introduced to end.
 */
func TestAUserMadeGroupGrantsNoGroupPermissions(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	sid := newSession(t, c, "s")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "the-user's-group", Coordinator: sid, CreatedBy: "user:ops",
	})
	if err != nil {
		t.Fatal(err)
	}

	grants, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: sid})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range grants.Grants {
		if g.Scope == store.GroupScope(created.Group.Id) {
			t.Errorf("the user's group granted %q to its coordinator", g.Capability)
		}
	}
}

/**
 * A FORK GETS A ROOM OF ITS OWN, and it is not the parent's.
 *
 * A fork is a session a person intends to direct — often the whole reason to fork is to send the branch somewhere
 * else and watch. It needs the same first-person screen the original has.
 *
 * THE PARENT KEEPS ITS OWN, unchanged: the fork does not take over the parent's room, and the parent does not gain
 * membership in the child's.
 */
func TestForkingASessionGivesItItsOwnGroup(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	parent := newSession(t, c, "parent")
	forked, err := c.Fork(ctx, &agentv1.ForkRequest{Id: parent, Name: "branch"})
	if err != nil {
		t.Fatal(err)
	}
	if forked.GroupId == "" {
		t.Fatal("the fork was created without a group")
	}

	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: forked.GroupId})
	if err != nil {
		t.Fatalf("the group the fork response named does not exist: %v", err)
	}
	if got.Group.Name != "branch" {
		t.Errorf("group name = %q, want the fork's label", got.Group.Name)
	}
	if len(got.Members) != 1 || got.Members[0].Session != forked.Session.Id {
		t.Fatalf("members = %+v, want only the fork", got.Members)
	}

	// THE PARENT'S ROOM IS STILL THE PARENT'S.
	parentGroups, err := c.ListGroups(ctx, &agentv1.ListGroupsRequest{Session: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(parentGroups.Groups) != 1 || parentGroups.Groups[0].Name != "parent" {
		t.Errorf("the parent's groups = %v, want only its own room", parentGroups.Groups)
	}
}

/**
 * A GROUP'S MEMBER LIST SAYS WHO IS WORKING.
 *
 * "IS ANYONE ACTUALLY DOING THIS" is the first question a group page has to answer, and it is the reason the status
 * lives on the member row instead of only on the session list: a person watching the room would otherwise have to
 * open each member's transcript in turn to find out, which is four page loads rather than one glance.
 *
 * THE STATUS IS THE LEASE'S, not this process's active set — see statusOf. That matters because a turn runs on
 * exactly one replica, and a process-local answer would show "idle" for working sessions on every other one.
 */
func TestGroupMembersCarryRuntimeStatus(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}

	// NOTHING IS RUNNING YET: every member is idle, and the field is PRESENT rather than omitted — a caller that
	// cannot tell "idle" from "unknown" has to ask again to learn anything.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: created.Group.Id})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range got.Members {
		if m.Status != "idle" {
			t.Errorf("%s has status %q, want idle", m.Session, m.Status)
		}
	}

	// THE RUNTIME REPORTS THE WORKER BUSY, and the member list follows. The stub is the same seam the real runtime
	// plugs into, so this exercises the wiring rather than a mock of it.
	srv.statusOverride = func(session string) string {
		if session == worker {
			return "busy"
		}
		return "idle"
	}
	got, err = c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: created.Group.Id})
	if err != nil {
		t.Fatal(err)
	}
	bySession := map[string]string{}
	for _, m := range got.Members {
		bySession[m.Session] = m.Status
	}
	if bySession[worker] != "busy" {
		t.Errorf("the working member reads %q, want busy", bySession[worker])
	}
	if bySession[coord] != "idle" {
		t.Errorf("the idle member reads %q, want idle", bySession[coord])
	}
}
