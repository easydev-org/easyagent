package server

import (
	"context"
	"testing"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * A FORK CAN INHERIT ALL, NONE, OR ALL-BUT-SOME OF THE PARENT'S PERMISSIONS.
 *
 * These are three different intents and a fork that guessed would get two of them wrong:
 *
 *   INHERIT_ALL          "the same session, trying a different approach" — stripping it would make it
 *                        fail on tools that worked a moment earlier, and the failure would look like a
 *                        bug in the fork rather than a policy choice.
 *   INHERIT_NONE         a clean slate, for a fork handed to someone else.
 *   INHERIT_EXCEPT_SOME  everything it needs to do the work, minus the grants that would let it start
 *                        a branch of its own. This is what `spawn` uses.
 *
 * AND NO FORK EVER INHERITS GROUP MEMBERSHIP. A fork is a new session, not a new seat: becoming the
 * coordinator of the parent's group would collide with the one-per-session rule, and the parent is
 * already in it.
 */
func TestForkPermissionPolicies(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	p := newSession(t, c, "p")
	// The parent holds two capabilities in one extension.
	for _, cap := range []string{"group.create", "group.spawn"} {
		if _, err := c.CreateGrant(ctx, &agentv1.CreateGrantRequest{
			Id: p, ExtId: "bundled", Capability: cap, Risk: "write",
		}); err != nil {
			t.Fatal(err)
		}
	}

	/**
	 * ONLY INHERITED GRANTS COUNT, which is why this skips scoped rows.
	 *
	 * A FORK GETS A GROUP OF ITS OWN, and that group carries the three group capabilities scoped to it — that is
	 * the room being usable, not the parent's permissions arriving. Counting them as "inherited" would make
	 * INHERIT_NONE look like it inherited everything, and the policy being tested would be unobservable.
	 */
	held := func(sid string) map[string]bool {
		t.Helper()
		res, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: sid})
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, g := range res.Grants {
			if g.ExtId == "bundled" && g.Scope == "" {
				out[g.Capability] = true
			}
		}
		return out
	}

	// INHERIT_ALL: everything the parent had.
	all, err := c.Fork(ctx, &agentv1.ForkRequest{Id: p, Name: "all"})
	if err != nil {
		t.Fatal(err)
	}
	got := held(all.Session.Id)
	if !got["group.create"] || !got["group.spawn"] {
		t.Errorf("INHERIT_ALL fork holds %v, want both", got)
	}

	// INHERIT_NONE: nothing at all.
	none, err := c.Fork(ctx, &agentv1.ForkRequest{
		Id: p, Name: "none",
		Permissions: agentv1.ForkPermissionPolicy_FORK_PERMISSION_POLICY_INHERIT_NONE,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := held(none.Session.Id); len(got) != 0 {
		t.Errorf("INHERIT_NONE fork holds %v, want nothing", got)
	}

	// INHERIT_EXCEPT_SOME: only what was not excluded.
	some, err := c.Fork(ctx, &agentv1.ForkRequest{
		Id: p, Name: "some",
		Permissions:         agentv1.ForkPermissionPolicy_FORK_PERMISSION_POLICY_INHERIT_EXCEPT_SOME,
		ExcludeCapabilities: []string{"bundled/group.spawn"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got = held(some.Session.Id)
	if !got["group.create"] {
		t.Errorf("INHERIT_EXCEPT_SOME fork lost an unexcluded capability: %v", got)
	}
	if got["group.spawn"] {
		t.Errorf("INHERIT_EXCEPT_SOME fork kept the excluded capability: %v", got)
	}
}

/**
 * THE EXCLUSION IS KEYED BY EXTENSION AND CAPABILITY TOGETHER.
 *
 * Two extensions may both declare `merge`, and excluding one must not exclude the other — a bare
 * capability key would silently strip the other extension's grant.
 */
func TestForkExclusionIsScopedToTheExtension(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	p := newSession(t, c, "p")
	for _, g := range []struct{ ext, cap string }{
		{"bundled", "group.create"}, {"gitdev", "group.create"},
	} {
		if _, err := c.CreateGrant(ctx, &agentv1.CreateGrantRequest{
			Id: p, ExtId: g.ext, Capability: g.cap, Risk: "write",
		}); err != nil {
			t.Fatal(err)
		}
	}

	forked, err := c.Fork(ctx, &agentv1.ForkRequest{
		Id: p, Name: "child",
		Permissions:         agentv1.ForkPermissionPolicy_FORK_PERMISSION_POLICY_INHERIT_EXCEPT_SOME,
		ExcludeCapabilities: []string{"bundled/group.create"},
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := c.ListGrants(ctx, &agentv1.ListGrantsRequest{Id: forked.Session.Id})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, g := range res.Grants {
		// SCOPED ROWS ARE THE FORK'S OWN GROUP, not an inheritance: see TestForkPermissionPolicies.
		if g.Scope != "" {
			continue
		}
		seen[g.ExtId+"/"+g.Capability] = true
	}
	if seen["bundled/group.create"] {
		t.Error("the excluded capability survived")
	}
	if !seen["gitdev/group.create"] {
		t.Error("excluding one extension's capability also stripped another's")
	}
}

/**
 * A FORK DOES NOT INHERIT GROUP MEMBERSHIP.
 *
 * It is a new session, not a new seat. Inheriting the coordinator role would collide with the
 * one-per-session rule (the parent already holds it), and inheriting a member slot would put a session
 * in a group nobody added it to.
 */
func TestForkDoesNotInheritGroups(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	p := newSession(t, c, "p")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: p, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	forked, err := c.Fork(ctx, &agentv1.ForkRequest{Id: p, Name: "child"})
	if err != nil {
		t.Fatal(err)
	}

	list, err := c.ListGroups(ctx, &agentv1.ListGroupsRequest{Session: forked.Session.Id})
	if err != nil {
		t.Fatal(err)
	}
	/**
	 * THE FORK HAS A GROUP OF ITS OWN — that is deliberate, and it is not an inheritance. What must not happen is
	 * the fork appearing in THE PARENT'S group, which is the membership question this test is about.
	 */
	parentGroup := groupIDByName(t, c, "g")
	for _, g := range list.Groups {
		if g.Id == parentGroup {
			t.Errorf("the fork joined the parent's group %v, want only its own", g)
		}
		if g.Name != "child" {
			t.Errorf("the fork is in an unexpected group %v, want only its own room", g)
		}
	}
	if len(list.Groups) != 1 {
		t.Errorf("the fork is in %d groups, want exactly its own room", len(list.Groups))
	}

	// And the parent is still the only coordinator.
	got, err := c.GetGroup(ctx, &agentv1.GetGroupRequest{Id: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Members) != 1 || got.Members[0].Session != p {
		t.Errorf("members = %v, want only p", got.Members)
	}
}
