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
	m, err := bundled.ParseManifest()
	if err != nil {
		t.Fatal(err)
	}

	var delegate *agent.ToolDef
	for i := range m.Tools {
		if m.Tools[i].Name == "delegate" {
			delegate = &m.Tools[i]
		}
	}
	if delegate == nil {
		t.Fatal("delegate is not declared in the manifest")
	}
	if len(delegate.Requires) != 1 || delegate.Requires[0] != "group.create" {
		t.Fatalf("delegate requires %v, want exactly [group.create]", delegate.Requires)
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

	// AND NO OTHER TOOL IS PRIVILEGED: the grant is for delegating, not a blanket gate that would make
	// every tool fail for a session that never asked for group creation.
	for _, tl := range m.Tools {
		if tl.Name == "delegate" {
			continue
		}
		for _, r := range tl.Requires {
			t.Errorf("tool %q also requires %q; group.create should gate delegation only", tl.Name, r)
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

	for _, name := range []string{"trusted", "untrusted"} {
		if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: name, Model: "gw/m"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateGrant(ctx, &agentv1.CreateGrantRequest{
		Id: "trusted", ExtId: "bundled", Capability: "group.create", Params: "", Risk: "write",
	}); err != nil {
		t.Fatal(err)
	}

	grants, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: "trusted"})
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

	// The other session holds nothing: the grant is not an extension-wide switch.
	other, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: "untrusted"})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range other.Grants {
		if g.Capability == "group.create" {
			t.Error("group.create leaked to a session that was never granted it")
		}
	}

	// And revoking takes it away again, which is the only way a user can undo the decision.
	if err := func() error {
		_, err := c.RevokeGrant(ctx, &agentv1.RevokeGrantRequest{
			Id: "trusted", GrantId: grants.Grants[0].Id,
		})
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	after, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: "trusted"})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range after.Grants {
		if g.Capability == "group.create" {
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

	if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s", Model: "gw/m"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "user-made", Coordinator: "s", CreatedBy: "user:ops",
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

	for _, name := range []string{"coord", "worker"} {
		if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: name, Model: "gw/m"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: "coord", Members: []string{"worker"}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	_, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{Group: "g", Member: "coord"})
	if err == nil {
		t.Fatal("removing the coordinator directly must fail")
	}
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", got)
	}

	// The supported way: move the role, then remove the old one.
	if _, err := c.SetGroupCoordinator(ctx, &agentv1.SetGroupCoordinatorRequest{Group: "g", Session: "worker"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{Group: "g", Member: "coord"}); err != nil {
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

	for _, name := range []string{"coord", "stranger"} {
		if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: name, Model: "gw/m"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: "coord", CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetGroupCoordinator(ctx, &agentv1.SetGroupCoordinatorRequest{Group: "g", Session: "stranger"}); err == nil {
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

	for _, name := range []string{"coord", "worker"} {
		if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: name, Model: "gw/m"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: "coord", Members: []string{"worker"}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Name: "g"})
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]string{}
	for _, m := range got.Members {
		roles[m.Session] = m.Role
	}
	if roles["coord"] != "coordinator" {
		t.Errorf("coord role = %q, want coordinator", roles["coord"])
	}
	if roles["worker"] != "member" {
		t.Errorf("worker role = %q, want member", roles["worker"])
	}
}
