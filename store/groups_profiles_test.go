package store

import "testing"

/**
 * A MEMBER LIST IS A LIST OF SESSIONS, NOT OF IDS.
 *
 * The ids alone answered "who is here" and nothing else. A live coordinator read the list, could not tell which
 * row was itself, could not tell what anyone did, and said so in its answer to the group — because the display
 * name and the intro are how a peer decides whom to address and with what.
 *
 * ROLE COMES WITH IT because it is a routing fact: reports go to the coordinator, and a member that cannot tell
 * which row that is has to guess. The coordinator sorts first so the answer is visible without reading every
 * intro.
 */
func TestGroupMemberProfilesCarryNameIntroAndRole(t *testing.T) {
	d := newTestDB(t)

	coord := &SessionInput{Name: "发布协调", Model: "p/m", Locale: "zh", MaxTurns: 10}
	if err := d.CreateSession("t1", coord); err != nil {
		t.Fatal(err)
	}
	member := &SessionInput{Name: "CI 巡检", Model: "p/m", MaxTurns: 10}
	if err := d.CreateSession("t1", member); err != nil {
		t.Fatal(err)
	}
	// Intro is set through the settings patch, which is the only writer that touches it.
	if err := d.UpdateSessionSettings("t1", coord.ID, map[string]any{"intro": "协调发布流程"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateSessionSettings("t1", member.ID, map[string]any{"intro": "看着 main 的构建"}); err != nil {
		t.Fatal(err)
	}
	// A session with neither a name nor an intro: it must still appear, by id.
	bare := &SessionInput{Model: "p/m", MaxTurns: 10}
	if err := d.CreateSession("t1", bare); err != nil {
		t.Fatal(err)
	}

	if err := d.CreateGroup("t1", Group{Name: "release"}, coord.ID,
		[]string{member.ID, bare.ID}, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}

	got, err := d.GroupMemberProfiles("t1", "release")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d members, want 3: %+v", len(got), got)
	}

	// THE COORDINATOR COMES FIRST, so "who is answerable" is answered before any intro is read.
	if got[0].Session != coord.ID || got[0].Role != GroupRoleCoordinator {
		t.Errorf("first row = %+v, want the coordinator", got[0])
	}
	if got[0].Name != "发布协调" || got[0].Intro != "协调发布流程" {
		t.Errorf("the coordinator's profile was not returned: %+v", got[0])
	}

	byID := map[string]GroupMemberProfile{}
	for _, m := range got {
		byID[m.Session] = m
	}
	if m := byID[member.ID]; m.Name != "CI 巡检" || m.Intro != "看着 main 的构建" || m.Role != GroupRoleMember {
		t.Errorf("member row = %+v", m)
	}
	// AN EMPTY PROFILE KEEPS ITS ROW: dropping it would hide a member the group is told exists.
	if m, ok := byID[bare.ID]; !ok {
		t.Error("a member with no name and no intro was dropped")
	} else if m.Name != "" || m.Intro != "" || m.Role != GroupRoleMember {
		t.Errorf("bare member row = %+v", m)
	}
}

// A group with nobody in it returns an empty list rather than an error: "no members" is an answer.
func TestGroupMemberProfilesOnAnEmptyGroup(t *testing.T) {
	d := newTestDB(t)
	got, err := d.GroupMemberProfiles("t1", "nobody")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %d members of a group that does not exist: %+v", len(got), got)
	}
}
