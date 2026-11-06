package agent

import (
	"strings"
	"testing"
	"time"
)

/**
 * A COORDINATOR KNOWS WHAT ITS GROUP IS FOR.
 *
 * The group's intro cannot be recovered from the transcript: a coordinator woken by a group message gets the
 * message and the member ids, and nothing that says why these sessions are together. Left to infer it from the
 * member names, it answered a question that was meant for the group — the failure this section prevents.
 *
 * NIL MEANS "NO GROUP", and the section is then absent rather than empty: an empty <group></group> would read
 * as a group with no name.
 */
func TestEnvBlockNamesTheCoordinatedGroup(t *testing.T) {
	block := BuildEnvBlockWithGroup("en", time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC),
		"maintainer of acme/app",
		[]GroupEnv{{Name: "release", Intro: "release coordination for acme/app", Coordinator: "sess-1"}},
		nil)

	for _, want := range []string{
		"<group>", "<name>release</name>",
		"<purpose>release coordination for acme/app</purpose>",
		"<coordinator>sess-1</coordinator>",
		// The session's own intro is still there: the two are different facts.
		"maintainer of acme/app",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("missing %q in:\n%s", want, block)
		}
	}
}

// A session with no group gets no group section at all.
func TestEnvBlockOmitsAnAbsentGroup(t *testing.T) {
	block := BuildEnvBlockWithGroup("en", time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), "", nil, nil)
	if strings.Contains(block, "<group>") {
		t.Errorf("a group section appeared for a session with no group:\n%s", block)
	}
}

// A group with no intro still names its coordinator: the routing fact does not depend on the purpose.
func TestEnvBlockGroupWithoutAnIntro(t *testing.T) {
	block := BuildEnvBlockWithGroup("en", time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), "",
		[]GroupEnv{{Name: "release", Coordinator: "sess-1"}}, nil)
	if strings.Contains(block, "<purpose>") {
		t.Errorf("an empty purpose was rendered:\n%s", block)
	}
	if !strings.Contains(block, "<coordinator>sess-1</coordinator>") {
		t.Errorf("the coordinator is missing:\n%s", block)
	}
}

/**
 * A SESSION THAT COORDINATES SEVERAL GROUPS SEES ALL OF THEM.
 *
 * THIS IS WHAT MAKES THE ARRANGEMENT SAFE. A message arrives with its group named, and the prompt lists every
 * group the session coordinates — so matching the two is a lookup, not a guess.
 *
 * ONE GROUP PER SESSION WAS THE OLD RULE, and its justification was exactly this: a session hearing two groups
 * could not tell them apart. Stating the set is the fix the rule was standing in for.
 */
func TestEnvBlockListsEveryCoordinatedGroup(t *testing.T) {
	block := BuildEnvBlockWithGroup("en", time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC), "",
		[]GroupEnv{
			{Name: "release", Intro: "release coordination", Coordinator: "sess-1"},
			{Name: "incident", Intro: "5xx triage", Coordinator: "sess-1"},
		}, nil)

	for _, want := range []string{
		"<groups>",
		"<name>release</name>", "<purpose>release coordination</purpose>",
		"<name>incident</name>", "<purpose>5xx triage</purpose>",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("missing %q in:\n%s", want, block)
		}
	}
	// The section is a LIST, so a reader can count them.
	if strings.Count(block, "<group>") != 2 {
		t.Errorf("want two <group> entries:\n%s", block)
	}
}

// A session that coordinates none gets no section at all — not an empty <groups></groups>.
func TestEnvBlockOmitsGroupsWhenNone(t *testing.T) {
	block := BuildEnvBlockWithGroup("en", time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC), "", nil, nil)
	if strings.Contains(block, "<groups>") {
		t.Errorf("a groups section appeared for a session that coordinates none:\n%s", block)
	}
}
