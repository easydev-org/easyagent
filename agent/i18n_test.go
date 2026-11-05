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
		&GroupEnv{Name: "release", Intro: "release coordination for acme/app", Coordinator: "sess-1"},
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
		&GroupEnv{Name: "release", Coordinator: "sess-1"}, nil)
	if strings.Contains(block, "<purpose>") {
		t.Errorf("an empty purpose was rendered:\n%s", block)
	}
	if !strings.Contains(block, "<coordinator>sess-1</coordinator>") {
		t.Errorf("the coordinator is missing:\n%s", block)
	}
}
