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
	groups map[string][]GroupRow
	/**
	 * members is the membership ROW: id, display name, intro, role. A map of ids alone was what the tool
	 * rendered, and the coordinator could not tell which row was itself or what anyone did.
	 */
	members     map[string][]GroupMemberRow
	coordinator map[string]string
}

func (g *groupStub) GroupsOfSession(_, session string) ([]GroupRow, error) {
	return g.groups[session], nil
}
func (g *groupStub) IsGroupMember(_, group, session string) bool {
	for _, m := range g.members[group] {
		if m.Session == session {
			return true
		}
	}
	return false
}
func (g *groupStub) GroupMemberProfiles(_, group string) ([]GroupMemberRow, error) {
	return g.members[group], nil
}
func (g *groupStub) GroupCoordinator(_, group string) (string, error) {
	return g.coordinator[group], nil
}

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
		members: map[string][]GroupMemberRow{"incident": {
			{Session: "alice", Name: "CI 巡检", Intro: "看着 main 的构建", Role: "coordinator"},
			{Session: "bob", Name: "仓库维护", Intro: "负责 PR 与发布", Role: "member"},
			{Session: "carol", Name: "文档整理", Role: "member"},
		}},
	}
	e := newGroupExt(t, stub)
	ctx := context.Background()

	out, err := e.groupMembers(ctx, "t", "bob", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"alice", "bob", "carol", "this session",
		// THE PROFILE, NOT JUST THE ID: the display name and the one-line intro are how a peer decides
		// whom to address and with what.
		"CI 巡检", "看着 main 的构建", "仓库维护", "负责 PR 与发布",
	} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("missing %q in:\n%s", want, out.Content)
		}
	}
	// A member with no intro is still listed, by name.
	if !strings.Contains(out.Content, "文档整理 (carol)") {
		t.Errorf("a member without an intro was mangled:\n%s", out.Content)
	}
}

/**
 * AN EMPTY NAME OR INTRO MUST NOT DROP THE MEMBER.
 *
 * A session is not required to have either, and a row silently omitted here would hide a session the group's
 * members are told exists — which is exactly the list other sessions use to decide whom to address.
 */
func TestGroupMembersKeepsAMemberWithNoProfile(t *testing.T) {
	stub := &groupStub{
		stubDeps: &stubDeps{},
		members:  map[string][]GroupMemberRow{"incident": {{Session: "carol", Role: "member"}}},
	}
	e := newGroupExt(t, stub)

	out, err := e.groupMembers(context.Background(), "t", "carol", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "- carol") {
		t.Errorf("a member with no name is not listed:\n%s", out.Content)
	}
	if strings.Contains(out.Content, "()") {
		t.Errorf("an empty name rendered as empty parentheses:\n%s", out.Content)
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
		members:  map[string][]GroupMemberRow{"incident": {{Session: "alice", Role: "coordinator"}}},
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

/**
 * THE MEMBER LIST SAYS WHO COORDINATES.
 *
 * A bare list of ids answers "who is here" but not "who is answerable for this group", and a member needs
 * both. A live coordinator reported exactly this gap: it could see the members, could not tell which one it
 * was, and put that uncertainty in its answer to the group. The role is a routing fact — the coordinator is
 * where members' reports arrive — so the routing tool states it.
 *
 * THE ROLE AND "THIS SESSION" ARE SEPARATE FACTS: the reader may be the coordinator, or an ordinary member
 * reading its own row, and a list that conflated them would answer only one of the two questions.
 */
func TestGroupMembersNamesTheCoordinator(t *testing.T) {
	stub := &groupStub{
		stubDeps: &stubDeps{},
		members: map[string][]GroupMemberRow{"incident": {
			{Session: "alice", Name: "CI 巡检", Role: "coordinator"},
			{Session: "bob", Name: "仓库维护", Role: "member"},
			{Session: "carol", Name: "文档整理", Role: "member"},
		}},
		coordinator: map[string]string{"incident": "alice"},
	}
	e := newGroupExt(t, stub)

	// A plain member reads the list: alice is named as the coordinator, bob as this session.
	out, err := e.groupMembers(context.Background(), "t", "bob", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "CI 巡检 (alice) — coordinator") {
		t.Errorf("the coordinator is not marked:\n%s", out.Content)
	}
	if !strings.Contains(out.Content, "仓库维护 (bob) — this session") {
		t.Errorf("the asker is not marked:\n%s", out.Content)
	}
	if strings.Contains(out.Content, "文档整理 (carol) — ") {
		t.Errorf("an ordinary member was given a role note:\n%s", out.Content)
	}

	// The coordinator reads the same list: both facts land on one row, and neither is dropped.
	out, err = e.groupMembers(context.Background(), "t", "alice", map[string]any{"group": "incident"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Content, "CI 巡检 (alice) — coordinator, this session") {
		t.Errorf("a coordinator should see both notes on its own row:\n%s", out.Content)
	}
}
