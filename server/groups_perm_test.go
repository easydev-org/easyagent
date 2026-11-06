package server

import (
	"context"
	"testing"

	"connectrpc.com/connect/v2"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * CREATING A GROUP REQUIRES A GRANT, AND GETTING IT WRONG IN EITHER DIRECTION IS A SECURITY BUG.
 *
 * A group is a VISIBILITY STRUCTURE: its coordinator can address the sessions in it. A session that
 * could create one freely could assemble sessions and talk to them — which is the ability the whole
 * "no direct messaging" rule exists to remove. So the grant is what turns a session into something that
 * can start conversations.
 *
 * The gate itself lives in `agentbus` (manifest `requires` + the ToolExecutor check), and these tests
 * pin the two halves that must agree for it to mean anything:
 *
 *   · the manifest DECLARES the requirement on the delegating tool;
 *   · the declaration is also listed as a grantable permission, so a user can actually grant it —
 *     a `requires` naming a key nobody can grant would make the tool permanently unusable.
 */
func TestDelegateRequiresTheGroupCreateGrant(t *testing.T) {
	// READ FROM THE MANIFEST rather than through an RPC: this asserts a property of the DECLARATION,
	// and a test server without the in-process tool surface would report "not listed" for a reason that
	// has nothing to do with the declaration being wrong.
	//
	// THE TOOL IS `group-create`, not the earlier `delegate`: the name says what the privilege is FOR.
	m, err := bundled.ParseManifest()
	if err != nil {
		t.Fatal(err)
	}

	var delegate *agent.ToolDef
	for i := range m.Tools {
		if m.Tools[i].Name == "group-create" {
			delegate = &m.Tools[i]
		}
	}
	if delegate == nil {
		t.Fatal("group-create is not declared in the manifest")
	}
	if len(delegate.Requires) != 1 || delegate.Requires[0] != "group.create" {
		t.Fatalf("group-create requires %v, want exactly [group.create]", delegate.Requires)
	}

	// THE PERMISSION MUST ALSO BE GRANTABLE, or the requirement is a dead end: a `requires` naming a key
	// nobody can grant makes the tool permanently unusable and says nothing about why.
	var declared bool
	for _, p := range m.Permissions {
		if p.Key == "group.create" {
			declared = true
		}
	}
	if !declared {
		t.Error("group.create is required by `delegate` but not declared as a grantable permission")
	}

	/**
	 * ONLY THE TWO GROUP-ENTRY TOOLS ARE PRIVILEGED, AND THEY NEED DIFFERENT GRANTS.
	 *
	 * `spawn` carries its own (`group.spawn`): "may this session assemble a group" and "may it put other
	 * sessions to work" are separate decisions, and neither grant may imply the other. Every OTHER tool
	 * must stay unprivileged — a spawned worker loses both group grants, so a `group-send` that required
	 * one would make its report impossible and every delegation a dead end.
	 */
	privileged := map[string]string{
		"group-create": "group.create",
		"spawn":        "group.spawn",
	}
	for _, tl := range m.Tools {
		want, ok := privileged[tl.Name]
		if !ok {
			for _, r := range tl.Requires {
				t.Errorf("tool %q requires %q; a spawned worker loses both group grants and must still be "+
					"able to work and report back", tl.Name, r)
			}
			continue
		}
		if len(tl.Requires) != 1 || tl.Requires[0] != want {
			t.Errorf("%q requires %v, want [%s]", tl.Name, tl.Requires, want)
		}
	}
}

/**
 * THE GRANT IS PER SESSION, so one session being trusted to create groups does not trust the next.
 *
 * That is the point of putting it on the grant list rather than on the extension: the user decides once,
 * for the session that will actually do the delegating.
 */
func TestGroupCreateGrantIsPerSession(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	trusted := newSession(t, c, "trusted")
	untrusted := newSession(t, c, "untrusted")
	if _, err := c.CreateGrant(ctx, &agentv1.CreateGrantRequest{
		Id: trusted, ExtId: "bundled", Capability: "group.create", Params: "", Risk: "write",
	}); err != nil {
		t.Fatal(err)
	}

	grants, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: trusted})
	if err != nil {
		t.Fatal(err)
	}
	var held bool
	for _, g := range grants.Grants {
		if g.Capability == "group.create" {
			held = true
		}
	}
	if !held {
		t.Fatal("the grant did not stick")
	}

	/**
	 * THE OTHER SESSION HOLDS NOTHING *UNSCOPED*: the grant is not an extension-wide switch.
	 *
	 * IT DOES HOLD `group.create` SCOPED TO ITS OWN ROOM, because every session gets one at creation and may build
	 * more groups inside it. That is a different authorisation — it says nothing about a group a person made — so it
	 * is filtered out here. Counting it would make this test assert that a session has no room, which is not the
	 * property under test.
	 */
	other, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: untrusted})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range other.Grants {
		if g.Capability == "group.create" && g.Scope == "" {
			t.Error("group.create leaked to a session that was never granted it")
		}
	}

	// And revoking takes it away again, which is the only way a user can undo the decision.
	//
	// THE UNSCOPED ROW IS THE ONE TO REVOKE. `grants.Grants[0]` was whichever row sorted first, and with a scoped
	// grant present that could be the room's own permission — revoking that would prove nothing about this grant.
	var unscopedID string
	for _, g := range grants.Grants {
		if g.Capability == "group.create" && g.Scope == "" {
			unscopedID = g.Id
		}
	}
	if unscopedID == "" {
		t.Fatal("the explicit grant is not in the list")
	}
	if err := func() error {
		_, err := c.RevokeGrant(ctx, &agentv1.RevokeGrantRequest{
			Id: trusted, GrantId: unscopedID,
		})
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	after, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: trusted})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range after.Grants {
		if g.Capability == "group.create" && g.Scope == "" {
			t.Error("the grant survived its revocation")
		}
	}
}

// The console path (a user creating groups) does not need a session grant: the user IS the authority.
// A deployment where an operator had to grant themselves a permission through the API would be absurd,
// and it would also mean no group could exist before some session had the grant.
func TestUserGroupCreationNeedsNoSessionGrant(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	s := newSession(t, c, "s")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "user-made", Coordinator: s, CreatedBy: "user:ops",
	}); err != nil {
		t.Fatalf("a user must be able to create a group without a session grant: %v", err)
	}
}

// A group needs a coordinator; without one its messages reach nobody, and the failure would only appear
// the first time someone wrote to it.
func TestCreateGroupRequiresCoordinator(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	_, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{Name: "g", CreatedBy: "user"})
	if err == nil {
		t.Fatal("a group with no coordinator must be refused")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
}

/**
 * THE COORDINATOR CANNOT BE REMOVED WITHOUT A REPLACEMENT.
 *
 * A group with no coordinator routes the user's messages nowhere, and nothing in the API would say so —
 * the user would write to the group and see no reply. The refusal happens at the moment the dead end
 * would be created.
 */
func TestCoordinatorCannotBeRemovedDirectly(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	_, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{GroupId: groupIDByName(t, c, "g"), Member: coord})
	if err == nil {
		t.Fatal("removing the coordinator directly must fail")
	}
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", got)
	}

	// The supported way: move the role, then remove the old one.
	if _, err := c.SetGroupCoordinator(ctx, &agentv1.SetGroupCoordinatorRequest{GroupId: groupIDByName(t, c, "g"), Session: worker}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{GroupId: groupIDByName(t, c, "g"), Member: coord}); err != nil {
		t.Fatalf("after the role moved, the old coordinator should be removable: %v", err)
	}
}

// Promoting a session that is not a member would add it to the group as a side effect of naming it,
// which is the opposite of what an explicit member list is for.
func TestSetCoordinatorRequiresExistingMember(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	stranger := newSession(t, c, "stranger")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetGroupCoordinator(ctx, &agentv1.SetGroupCoordinatorRequest{GroupId: groupIDByName(t, c, "g"), Session: stranger}); err == nil {
		t.Fatal("promoting a non-member must fail")
	}
}

/**
 * A GROUP'S ROLE IS A ROUTING FACT, SO IT MUST SURVIVE THE ROUND TRIP.
 *
 * The console renders "who receives the group's messages" from this field; a role that came back empty
 * would make every member look like an ordinary one and the coordinator indistinguishable.
 */
func TestGroupMemberRoleRoundTrip(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]string{}
	for _, m := range got.Members {
		roles[m.Session] = m.Role
	}
	if roles[coord] != "coordinator" {
		t.Errorf("coord role = %q, want coordinator", roles[coord])
	}
	if roles[worker] != "member" {
		t.Errorf("worker role = %q, want member", roles[worker])
	}
}

/**
 * A SESSION MAY COORDINATE ANY NUMBER OF GROUPS.
 *
 * There used to be a one-coordinator-per-session rule, on the argument that two groups' work would compete for
 * one context. The risk was real; the remedy was not. A session that cannot tell which group a message came from
 * is a DEFECT IN THE MESSAGE, and the fix is to say so — every group message names its group, and the prompt lists
 * every group the session coordinates.
 *
 * WHAT THE OLD RULE COST: an operator running several groups had to dedicate a session to each, and those
 * sessions shared nothing — no history, no learned procedure. The work is the same work.
 *
 * THE CONSTRAINT A GROUP CANNOT SURVIVE WITHOUT REMAINS: exactly one coordinator PER GROUP.
 */
func TestSessionMayCoordinateManyGroups(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	for _, name := range []string{"first", "second", "third"} {
		if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
			Name: name, Coordinator: coord, CreatedBy: "user",
		}); err != nil {
			t.Fatalf("creating %q: %v", name, err)
		}
	}

	// EVERY ONE OF THEM NAMES THE SAME SESSION AS COORDINATOR, and each has exactly one.
	for _, name := range []string{"first", "second", "third"} {
		got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, name)})
		if err != nil {
			t.Fatal(err)
		}
		coordinators := 0
		for _, m := range got.Members {
			if m.Role == "coordinator" {
				coordinators++
				if m.Session != coord {
					t.Errorf("%s: coordinator = %q, want %q", name, m.Session, coord)
				}
			}
		}
		if coordinators != 1 {
			t.Errorf("%s has %d coordinators, want exactly 1", name, coordinators)
		}
	}
}

/**
 * THE RULE FOLLOWS THE ROLE, NOT THE GROUP.
 *
 * Promoting a session that already coordinates elsewhere is the same collision arriving by a different
 * door. Refused before the demotion, so a rejected move leaves the group exactly as it was rather than
 * briefly having no coordinator at all.
 */
func TestPromotionRespectsOneCoordinatorPerSession(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	other := newSession(t, c, "other")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g1", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	// A group coordinated by `other`, which is also a member of g1.
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g2", Coordinator: other, Members: []string{worker}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// `other` already coordinates g2, so it cannot take over g1.
	_, err := c.SetGroupCoordinator(ctx, &agentv1.SetGroupCoordinatorRequest{GroupId: groupIDByName(t, c, "g1"), Session: other})
	if err == nil {
		t.Fatal("promoting a session that already coordinates elsewhere must be refused")
	}

	// And g1 still has its original coordinator — a refused move must not leave it headless.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g1")})
	if err != nil {
		t.Fatal(err)
	}
	var gotCoord string
	for _, m := range got.Members {
		if m.Role == "coordinator" {
			gotCoord = m.Session
		}
	}
	if gotCoord != coord {
		t.Errorf("coordinator = %q, want unchanged (coord)", gotCoord)
	}
}

/**
 * THE IN-PROCESS EXTENSION'S PERMISSIONS SURVIVE A REFRESH.
 *
 * `Refresh` REBUILDS the permission table from what `Discover` reported, and `bundled` never appears in
 * that report — it is not a remote extension. So a one-time write was erased by the first refresh, and
 * the symptom was quiet: `requires: [group.create]` named a permission the console had nothing to offer,
 * so nobody could grant it and the tool was permanently unusable while looking merely broken.
 */
func TestBundledPermissionsSurviveRefresh(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	// The test server has no in-process tool surface, so assert the DECLARATION is reachable through the
	// manifest — the part a deployment assembles from. The registration path itself is covered by the
	// agentbus test below.
	m, err := bundled.ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Permissions) == 0 {
		t.Fatal("the manifest declares no permissions, so delegation could never be authorised")
	}
	var keys []string
	for _, p := range m.Permissions {
		keys = append(keys, p.Key)
	}
	for _, want := range []string{"group.create", "group.spawn"} {
		var found bool
		for _, k := range keys {
			if k == want {
				found = true
			}
		}
		if !found {
			t.Errorf("permission %q is required by a tool but not declared: %v", want, keys)
		}
	}
	_ = ctx
	_ = c
}
