package agentbus

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

/**
 * THE GROUP SUBJECT SHAPE IS DUPLICATED IN `bundled`, AND THIS IS WHY THAT IS SAFE.
 *
 * `bundled` cannot import agentbus (agentbus imports bundled), so the two literals — the channel name
 * and the `to.<session>` topic — exist in both packages. A silent divergence would publish to a subject
 * nobody subscribes to: the message is sent, the coordinator or member never hears it, and nothing in
 * the logs says which half is wrong.
 *
 * This test pins the pair. If either side renames, this fails at build time instead of at delivery
 * time.
 */
func TestGroupSubjectShapeMatchesBundled(t *testing.T) {
	// The literals bundled/depsstore.go uses.
	const bundledChannel = "group"
	bundledTopic := func(group, session string) string { return group + ".to." + session }

	if GroupChannel != bundledChannel {
		t.Fatalf("GroupChannel = %q, bundled publishes to %q", GroupChannel, bundledChannel)
	}
	if got, want := GroupMemberTopic("incident", "worker-1"), bundledTopic("incident", "worker-1"); got != want {
		t.Fatalf("GroupMemberTopic = %q, bundled publishes to %q", got, want)
	}
}

/**
 * THE COORDINATOR AND A MEMBER MUST NOT MATCH THE SAME MESSAGES.
 *
 * This is the routing rule the group role exists for: a user's message goes to the coordinator only,
 * and an addressed message goes to that member only. If the two patterns overlapped, every member would
 * react to instructions it was never given — which is precisely the failure that made `mail-send` worth
 * removing.
 */
func TestGroupPatternsSeparateRoles(t *testing.T) {
	coord := GroupPattern("incident", "", true)
	member := GroupPattern("incident", "w1", false)

	userTopic := GroupUserTopic("incident")         // incident.in
	addressed := GroupMemberTopic("incident", "w1") // incident.to.w1

	if !protocol.MatchSubject(coord, userTopic) {
		t.Errorf("coordinator must hear the user's message (%q vs %q)", coord, userTopic)
	}
	if protocol.MatchSubject(member, userTopic) {
		t.Errorf("a member must NOT hear the group's user message (%q vs %q)", member, userTopic)
	}
	if !protocol.MatchSubject(member, addressed) {
		t.Errorf("a member must hear what addresses it (%q vs %q)", member, addressed)
	}
	if !protocol.MatchSubject(coord, addressed) {
		t.Errorf("the coordinator must hear a member's reply (%q vs %q)", coord, addressed)
	}
	// And an addressed message for someone ELSE is not the member's business.
	if protocol.MatchSubject(member, GroupMemberTopic("incident", "w2")) {
		t.Error("a member must not hear another member's addressed message")
	}
	// Nor another group's traffic.
	if protocol.MatchSubject(member, GroupMemberTopic("other", "w1")) {
		t.Error("a group's pattern must not match another group's topics")
	}
}
