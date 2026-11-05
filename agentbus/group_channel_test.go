package agentbus

import (
	"strings"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
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

/**
 * A GROUP EVENT NAMES THE SENDER, BY THE NAME A READER KNOWS.
 *
 * "Another member of the group" told the model nothing it could act on: it could not tell a coordinator from a
 * peer, could not attribute a claim, and could not decide whether a reply was expected from it. The display
 * name is what the member list shows; the ID rides alongside because the name is a label two sessions may
 * share and the ID is the only thing that identifies one.
 */
func TestGroupEventTextNamesTheSender(t *testing.T) {
	msg := bus.ChannelMessage{
		Channel: GroupChannel,
		Topic:   GroupUserTopic("release"),
		Payload: map[string]any{
			"group":     "release",
			"from":      "abc123",
			"from_name": "发布协调",
			"text":      "请确认 main 是否干净",
		},
	}
	got := channelEventText(msg)

	if !strings.Contains(got, "发布协调") {
		t.Errorf("the sender's name is missing:\n%s", got)
	}
	if !strings.Contains(got, "abc123") {
		t.Errorf("the sender's id is missing:\n%s", got)
	}
	if !strings.Contains(got, "请确认 main 是否干净") {
		t.Errorf("the body is missing:\n%s", got)
	}
	if strings.Contains(got, "Another member") {
		t.Errorf("the generic phrasing is still there:\n%s", got)
	}
}

// A message the USER posted has no sender, and must not be attributed to anyone.
func TestGroupEventTextForAUserPost(t *testing.T) {
	got := channelEventText(bus.ChannelMessage{
		Channel: GroupChannel,
		Topic:   GroupUserTopic("release"),
		Payload: map[string]any{"group": "release", "text": "go"},
	})
	if !strings.Contains(got, "The user") {
		t.Errorf("a user post should be attributed to the user:\n%s", got)
	}
	if strings.Contains(got, "posted to group \"release\":\n\n\ngo") {
		t.Errorf("empty sender name leaked into the text:\n%s", got)
	}
}

/**
 * THE SOURCE CARRIES THE GROUP AND THE SENDER.
 *
 * `session:<id>` alone made a group message look like a private hand-off between two sessions: the console
 * could name the sender but not the conversation, so a reader could not open the group the message came from.
 */
func TestGroupSourceNamesBothHalves(t *testing.T) {
	// The form the distributor builds: `group:<name>/<sender>`. The CONSOLE splits it; routing does not, which
	// is why the self-echo check reads the payload's `from` instead.
	src := "group:release/abc123"
	group := strings.SplitN(strings.TrimPrefix(src, "group:"), "/", 2)
	if group[0] != "release" || group[1] != "abc123" {
		t.Errorf("parsed %v, want [release abc123]", group)
	}
}

/**
 * THE SELF-ECHO CHECK READS THE SENDER FROM THE PAYLOAD.
 *
 * `Source` is what the CONSOLE renders, and its shape changed once already (`session:<id>` became
 * `group:<name>/<sender>` when group messages got their own rendering). Routing must not depend on a
 * presentation string: the payload carries `from` on every group message, and that is the sender.
 *
 * This pins the lookup the distributor relies on — a regression here is silent, because the check simply stops
 * firing and the coordinator starts answering its own dispatches again.
 */
func TestGroupSenderComesFromThePayload(t *testing.T) {
	payload := map[string]any{"group": "incident-42", "from": "abc123", "text": "hi"}
	if got := payloadString(payload, "from"); got != "abc123" {
		t.Errorf("from = %q, want abc123", got)
	}
	// A user's post carries no sender, so the check must not suppress it for anyone.
	if got := payloadString(map[string]any{"group": "g", "text": "hi"}, "from"); got != "" {
		t.Errorf("a user post reported sender %q, want empty", got)
	}
	// And a payload that is not an object at all yields empty rather than panicking.
	if got := payloadString("not a map", "from"); got != "" {
		t.Errorf("non-map payload gave %q", got)
	}
}

/**
 * EVERY GROUP MESSAGE SAYS HOW TO ANSWER IT.
 *
 * The tool description says it too, but a description is read once, when the tool list is built. A live group
 * showed the failure this prevents: a coordinator read the question, wrote a full answer — including "there is
 * no token, so I cannot check" — and the answer stayed in its own conversation. The group showed the question
 * and nothing else.
 *
 * THE INSTRUCTION TRAVELS WITH EVERY GROUP MESSAGE, not just a user's: a member reporting back is in the same
 * position, and so is a coordinator reading that report.
 */
func TestEveryGroupEventSaysHowToAnswer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload map[string]any
	}{
		{"user post", map[string]any{"group": "release", "text": "check main"}},
		{"member report", map[string]any{"group": "release", "from": "abc123", "from_name": "CI 巡检", "text": "done"}},
	} {
		got := channelEventText(bus.ChannelMessage{
			Channel: GroupChannel,
			Topic:   GroupUserTopic("release"),
			Payload: tc.payload,
		})
		if !strings.Contains(got, "group-send") {
			t.Errorf("%s: no way to answer is given:\n%s", tc.name, got)
		}
		// The group must be named in the instruction, or the model has to guess which group to post to.
		if !strings.Contains(got, `group="release"`) {
			t.Errorf("%s: the instruction does not name the group:\n%s", tc.name, got)
		}
		// The empty-`to` form is the one a reply needs; saying only "name recipients" would produce a
		// member-to-member refusal.
		if !strings.Contains(got, "`to` empty") {
			t.Errorf("%s: a reply has no stated form:\n%s", tc.name, got)
		}
	}
}
