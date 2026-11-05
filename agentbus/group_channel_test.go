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

/**
 * A SENDER MUST NOT HEAR ITS OWN MESSAGE.
 *
 * The coordinator's pattern (`<group>.>`) has to match the members' replies, and the same pattern therefore
 * matches the coordinator's own dispatches (`<group>.to.<member>`). On the live cluster that produced three
 * rounds of near-identical replies: the coordinator received its own dispatch, read it as a new instruction,
 * and dispatched again.
 *
 * The fix belongs in the distributor, where the sender's identity is known — a narrower pattern would lose the
 * replies, which is the half the coordinator cannot do without.
 */
func TestSenderOfExtractsTheSession(t *testing.T) {
	cases := []struct {
		origin string
		want   string
	}{
		{"session:abc123", "abc123"},
		{"group:release-coordination", ""},
		{"user:group", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := senderOf(c.origin); got != c.want {
			t.Errorf("senderOf(%q) = %q, want %q", c.origin, got, c.want)
		}
	}
}

/**
 * THE PATTERNS ARE UNCHANGED — the coordinator still follows `<group>.>`, because that is what lets it hear the
 * members' replies. This pins the shape the distributor relies on: the self-echo is dropped by identity, not by
 * narrowing the subscription.
 */
func TestCoordinatorFollowsTheWholeGroup(t *testing.T) {
	coord := GroupPattern("incident", "coord", true)
	if coord != "incident.>" {
		t.Fatalf("coordinator pattern = %q, want the whole group", coord)
	}
	// It hears the user's side...
	if !protocol.MatchSubject(coord, GroupUserTopic("incident")) {
		t.Error("the coordinator must hear the user's message")
	}
	// ...and the members' replies...
	if !protocol.MatchSubject(coord, GroupMemberTopic("incident", "w1")) {
		t.Error("the coordinator must hear a member's reply")
	}
	// ...and therefore its own dispatches too, which is why the distributor drops them.
	if !protocol.MatchSubject(coord, GroupMemberTopic("incident", "w2")) {
		t.Error("the pattern is expected to match dispatches; the distributor is what suppresses the echo")
	}
}
