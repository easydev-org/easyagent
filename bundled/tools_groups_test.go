package bundled

import (
	"context"
	"strings"
	"testing"
)

/**
 * GROUP TOOLS ARE READ-ONLY, AND THAT IS THE POINT.
 *
 * The manifest declares what the model can see; this file pins the behaviour behind it. Three
 * properties matter:
 *
 *   · a session sees the groups it is in, and only those;
 *   · a group it is not in produces an answer that does not confirm the group exists — the same rule
 *     the API layer enforces, which a tool must not leak around;
 *   · the session itself is marked in the member list rather than omitted, because "who else is
 *     here" includes knowing that you are one of them.
 */
type groupStub struct {
	*stubDeps
	groups  map[string][]GroupRow
	members map[string][]string
}

func (g *groupStub) GroupsOfSession(_, session string) ([]GroupRow, error) {
	return g.groups[session], nil
}
func (g *groupStub) IsGroupMember(_, group, session string) bool {
	for _, m := range g.members[group] {
		if m == session {
			return true
		}
	}
	return false
}
func (g *groupStub) GroupMembers(_, group string) []string { return g.members[group] }

func newGroupExt(t *testing.T, g *groupStub) *Extension {
	t.Helper()
	e, err := New(g)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestGroupListShowsOnlyOwnGroups(t *testing.T) {
	stub := &groupStub{
		stubDeps: &stubDeps{},
		groups: map[string][]GroupRow{
			"alice": {{Name: "incident", Members: 3}, {Name: "alpha", Members: 1}},
			"bob":   nil,
		},
	}
	e := newGroupExt(t, stub)
	ctx := context.Background()

	out, err := e.groupList(ctx, "t", "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"incident", "alpha", "2 group(s)"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("missing %q in:\n%s", want, out.Content)
		}
	}

	// A session in no group gets a sentence, not an empty string: an empty result reads as a failure
	// and the model will ask again.
	out, err = e.groupList(ctx, "t", "bob", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "no groups") {
		t.Errorf("empty result should say so, got %q", out.Content)
	}
}

func TestGroupMembersListsAndMarksTheAsker(t *testing.T) {
	stub := &groupStub{
		stubDeps: &stubDeps{},
		members:  map[string][]string{"incident": {"alice", "bob", "carol"}},
	}
	e := newGroupExt(t, stub)
	ctx := context.Background()

	out, err := e.groupMembers(ctx, "t", "bob", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"alice", "bob", "carol", "this session"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("missing %q in:\n%s", want, out.Content)
		}
	}
}

/**
 * A NON-MEMBER'S ANSWER MUST NOT CONFIRM THE GROUP EXISTS.
 *
 * The wording is deliberately "is not a member of" rather than "no such group": the store already
 * refuses to distinguish those two to a client, and a tool that reported "unknown group" would let a
 * model enumerate the tenant by asking about names one at a time.
 */
func TestGroupMembersRefusesANonMember(t *testing.T) {
	stub := &groupStub{
		stubDeps: &stubDeps{},
		members:  map[string][]string{"incident": {"alice"}},
	}
	e := newGroupExt(t, stub)
	ctx := context.Background()

	out, err := e.groupMembers(ctx, "t", "mallory", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "not a member") {
		t.Errorf("want a non-membership answer, got %q", out.Content)
	}
	// The members must not leak in the refusal.
	if strings.Contains(out.Content, "alice") {
		t.Errorf("the refusal leaked the member list: %q", out.Content)
	}
}

func TestGroupMembersRequiresAGroupName(t *testing.T) {
	e := newGroupExt(t, &groupStub{stubDeps: &stubDeps{}})
	out, err := e.groupMembers(context.Background(), "t", "s", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "required") {
		t.Errorf("empty group name should be rejected, got %q", out.Content)
	}
}

/**
 * A SESSION MAY CREATE ITS OWN GROUP, AND NOTHING ELSE.
 *
 * `group-create` exists and is gated by the `group.create` grant, so the user still decides whether a
 * session may assemble collaborators at all. What must NOT exist is any tool that edits membership or
 * deletes a group: those would let a session change the shape of a group a person set up, and the
 * member list would stop being something anyone can verify.
 *
 * This asserts the absences, because an absence is exactly the kind of property a later "helpful"
 * addition undoes — and unlike the positive rules, nothing else would notice.
 */
func TestOnlyGroupCreateMutatesGroups(t *testing.T) {
	e := newGroupExt(t, &groupStub{stubDeps: &stubDeps{}})
	for name := range e.handlers {
		switch name {
		case "group-add-member", "group-remove-member", "group-join", "group-leave",
			"group-update", "group-delete", "group-rename":
			t.Errorf("handler %q exists; a session may create its own group but never edit one", name)
		}
	}
}
