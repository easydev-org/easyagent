package server

import (
	"context"
	"testing"

	"connectrpc.com/connect/v2"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * A GROUP IS A LIST OF SESSIONS THAT CAN SEE EACH OTHER, AND THAT IS ALL.
 *
 * These tests pin the three properties the design rests on: membership is the only permission, a
 * session cannot learn that a group it is not in exists, and groups are never created by the thing
 * that gets put in them.
 */
func TestGroupMembershipIsTheOnlyPermission(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	alice := newSession(t, c, "alice")
	bob := newSession(t, c, "bob")
	carol := newSession(t, c, "carol")

	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "incident-42", Coordinator: alice, Members: []string{bob}, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatal(err)
	}

	// A MEMBER SEES THE OTHER MEMBERS. This is the whole feature: "who else is here".
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "incident-42"), Session: alice})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Members) != 2 {
		t.Fatalf("members = %d, want 2", len(got.Members))
	}
	names := map[string]bool{}
	for _, m := range got.Members {
		names[m.Session] = true
	}
	// MEMBERS ARE IDS. A display name is not unique, so a member list that carried names could not say
	// which session it meant when two are called "alice".
	if !names[alice] || !names[bob] {
		t.Errorf("members = %v, want alice and bob ids", names)
	}

	// A NON-MEMBER GETS NOT_FOUND, NOT PERMISSION_DENIED.
	//
	// The difference matters: PERMISSION_DENIED would confirm the group exists, which turns this call
	// into an oracle for enumerating every group in the tenant.
	_, err = c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "incident-42"), Session: carol})
	if err == nil {
		t.Fatal("a non-member must not be able to read the group")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound (PermissionDenied would confirm the group exists)", got)
	}

	// AND THE LIST IS FILTERED THE SAME WAY: carol sees nothing, not an empty-looking entry for a
	// group she cannot open.
	list, err := c.ListGroups(ctx, &agentv1.ListGroupsRequest{Session: carol})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Groups) != 0 {
		t.Errorf("non-member sees %d groups, want 0", len(list.Groups))
	}

	list, err = c.ListGroups(ctx, &agentv1.ListGroupsRequest{Session: bob})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Groups) != 1 || list.Groups[0].Name != "incident-42" {
		t.Errorf("bob's groups = %v, want [incident-42]", list.Groups)
	}
}

/**
 * A GROUP IS CREATED BY A PERSON, NEVER BY THE THING THAT GOES IN IT.
 *
 * There is no `session` field on CreateGroupRequest for exactly this reason — that absence is the
 * check. This test says so out loud, because "we forgot the field" and "we left the field out" look
 * identical in a diff six months later.
 */
func TestCreateGroupHasNoSessionCaller(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	s1 := newSession(t, c, "s1")
	// THE COORDINATOR IS ENROLLED BY THE CREATION ITSELF, so a group is never unreachable: the one
	// session that can dispatch its work is a member from the first moment.
	res, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "solo-group", Coordinator: s1, CreatedBy: "user:ops",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Group.Name != "solo-group" || res.Group.MemberCount != 1 {
		t.Errorf("group = %+v, want solo-group with the coordinator as its 1 member", res.Group)
	}

	list, err := c.ListGroups(ctx, &agentv1.ListGroupsRequest{Session: s1})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Groups) != 1 {
		t.Errorf("the coordinator must see its own group, got %v", list.Groups)
	}
}

/**
 * A MEMBER LIST MAY NOT NAME A SESSION THAT DOES NOT EXIST.
 *
 * The failure this prevents is quiet: the list shows a participant who can never answer, and nothing
 * in the API would ever remove them — the row's owner is gone.
 */
func TestAddGroupMemberRejectsUnknownSession(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	real := newSession(t, c, "real")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: real, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatal(err)
	}

	_, err := c.AddGroupMember(ctx, &agentv1.AddGroupMemberRequest{
		GroupId: groupIDByName(t, c, "g"), Members: []string{"ghost"},
	})
	if err == nil {
		t.Fatal("adding a session that does not exist must fail")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", got)
	}

	// AND THE REAL MEMBER IS STILL THERE: a rejected add must not have partially applied.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Members) != 1 || got.Members[0].Session != real {
		t.Errorf("members = %v, want [real]", got.Members)
	}
}

// Adding the same member twice is a no-op, not a conflict: a console cannot always tell whether the
// member is already there, and an error would be a puzzle rather than an answer.
func TestAddGroupMemberIsIdempotent(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	s := newSession(t, c, "s")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{Name: "g", Coordinator: s, CreatedBy: "user:ops"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		res, err := c.AddGroupMember(ctx, &agentv1.AddGroupMemberRequest{GroupId: groupIDByName(t, c, "g"), Members: []string{s}})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if res.Group.MemberCount != 1 {
			t.Errorf("after add %d, count = %d, want 1", i, res.Group.MemberCount)
		}
	}
}

// Removing a member takes it out of the list, and a session that is no longer a member can no longer
// read the group.
func TestRemoveGroupMemberRevokesVisibility(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	a := newSession(t, c, "a")
	b := newSession(t, c, "b")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: a, Members: []string{b}, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{GroupId: groupIDByName(t, c, "g"), Member: b}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g"), Session: b}); err == nil {
		t.Fatal("a removed member must lose visibility")
	}
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g"), Session: a})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Members) != 1 || got.Members[0].Session != a {
		t.Errorf("members = %v, want [a]", got.Members)
	}
}

// The picture follows the same `optional` rule as a session's: absent leaves it alone, empty clears
// it. Getting this wrong either wipes a picture on an unrelated save or makes clearing impossible.
func TestGroupAvatarClearAndKeep(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	s := newSession(t, c, "s")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", AvatarFile: "file:abc", Coordinator: s, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatal(err)
	}

	// An update that does not mention the avatar leaves it alone.
	if _, err := c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: groupIDByName(t, c, "g")}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if got.Group.AvatarFile != "file:abc" {
		t.Fatalf("avatar = %q, want file:abc (an absent field must not clear it)", got.Group.AvatarFile)
	}

	// Present-but-empty clears it.
	empty := ""
	res, err := c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: groupIDByName(t, c, "g"), AvatarFile: &empty})
	if err != nil {
		t.Fatal(err)
	}
	if res.Group.AvatarFile != "" {
		t.Errorf("avatar = %q, want cleared", res.Group.AvatarFile)
	}
}

// A group name is a subject segment, so anything that would change the subject's shape is refused
// here rather than turning one publish into a broadcast.
func TestGroupNameValidation(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	// A DIFFERENT COORDINATOR PER NAME, because a session may coordinate only one group — reusing one
	// would make every name after the first fail on that rule instead of on the one under test.
	coords := []string{
		newSession(t, c, "c0"),
		newSession(t, c, "c1"),
		newSession(t, c, "c2"),
	}
	next := 0
	coord := func() string { s := coords[next%3]; next++; return s }
	for _, name := range []string{"", "  ", "a b", "a>b", "a..b", ".a", "a."} {
		if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
			Name: name, Coordinator: coord(), CreatedBy: "user",
		}); err == nil {
			t.Errorf("name %q was accepted, want rejection", name)
		}
	}
	for _, name := range []string{"incident-42", "team.alpha", "A_b-9"} {
		if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
			Name: name, Coordinator: coord(), CreatedBy: "user",
		}); err != nil {
			t.Errorf("name %q rejected: %v", name, err)
		}
	}
}

// Deleting a group removes its memberships too, so a later group with the same name does not inherit
// the old member list.
func TestDeleteGroupClearsMemberships(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	s := newSession(t, c, "s")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: s, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteGroup(ctx, &agentv1.DeleteGroupRequest{Id: groupIDByName(t, c, "g")}); err != nil {
		t.Fatal(err)
	}

	// RECREATED: the old membership must not resurface, and the only member is the coordinator that
	// this creation named — not the one the previous incarnation had.
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{Name: "g", Coordinator: s, CreatedBy: "user:ops"}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Members) != 1 || got.Members[0].Session != s {
		t.Errorf("members = %v, want only the new coordinator", got.Members)
	}
	if got.Members[0].Role != "coordinator" {
		t.Errorf("role = %q, want coordinator", got.Members[0].Role)
	}
}

/**
 * RENAMING A GROUP CHANGES NOTHING BUT ITS LABEL.
 *
 * THIS IS WHAT THE IDENTITY SPLIT BOUGHT. While the name WAS the identity, a rename was a migration: memberships,
 * transcript rows and channel subscriptions were all keyed by the string that changed, so renaming either orphaned
 * them or was not offered at all. Now the name is a label and the id is what everything points at.
 *
 * THE ASSERTIONS ARE THE THINGS THAT WOULD HAVE BROKEN: the transcript is still readable, the members are still
 * members, and the id has not moved.
 */
func TestRenamingAGroupKeepsItsHistoryAndMembers(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "before", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	gid := created.Group.Id

	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: gid, Text: "said before the rename",
	}); err != nil {
		t.Fatal(err)
	}

	renamed := "after"
	if _, err := c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: gid, Name: &renamed}); err != nil {
		t.Fatal(err)
	}

	// THE ID IS UNCHANGED, and still addresses everything.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: gid})
	if err != nil {
		t.Fatal(err)
	}
	if got.Group.Name != "after" {
		t.Errorf("name = %q, want the new label", got.Group.Name)
	}
	if got.Group.Id != gid {
		t.Errorf("id = %q, want it unchanged", got.Group.Id)
	}
	if len(got.Members) != 2 {
		t.Errorf("members = %d, want the same 2", len(got.Members))
	}

	// THE TRANSCRIPT SURVIVED, under the same id.
	msgs, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: gid})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs.Messages) != 1 || msgs.Messages[0].Body != "said before the rename" {
		t.Fatalf("transcript = %+v", msgs.Messages)
	}
}

// A RENAME ONTO A TAKEN NAME IS REFUSED: two groups with one label defeats the only reason the label is unique.
func TestRenamingAGroupRefusesACollision(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	a, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{Name: "a", Coordinator: coord, CreatedBy: "user"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{Name: "b", Coordinator: coord, CreatedBy: "user"}); err != nil {
		t.Fatal(err)
	}

	taken := "b"
	_, err = c.UpdateGroup(ctx, &agentv1.UpdateGroupRequest{Id: a.Group.Id, Name: &taken})
	if err == nil {
		t.Fatal("a rename onto a taken name was accepted")
	}
	if got := connect.CodeOf(err); got != connect.CodeAlreadyExists {
		t.Errorf("code = %v, want AlreadyExists", got)
	}
}
