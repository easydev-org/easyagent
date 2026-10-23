package bus

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// The index is where a channel message becomes "these sessions get it", so every test here
// is about delivery correctness: a wrong match either leaks an event to a session that never
// subscribed, or silently loses it.

// put simulates what a KV watch delivers for a write.
func put(t *testing.T, x *SubscriptionIndex, tenant, session, id, channel, pattern string, wake bool) {
	t.Helper()
	x.apply(bus.KvEvent{
		Key:   protocol.SubscriptionKVKey(tenant, session, id),
		Value: `{"channel":"` + channel + `","pattern":"` + pattern + `","wake":` + boolStr(wake) + `}`,
	})
}

func del(t *testing.T, x *SubscriptionIndex, tenant, session, id string) {
	t.Helper()
	x.apply(bus.KvEvent{Key: protocol.SubscriptionKVKey(tenant, session, id), Deleted: true})
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestIndexMatchesChannelAndPattern(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-1", "gitea", "tag.agentdev/mrflow.>", true)

	if got := x.Match("alice", "gitea", "tag.agentdev/mrflow.v1.2.3"); len(got["s1"]) != 1 {
		t.Fatalf("matching topic should reach s1, got %+v", got)
	}
	// A different channel must not match even with a matching topic.
	if got := x.Match("alice", "group-chat", "tag.agentdev/mrflow.v1.2.3"); len(got) != 0 {
		t.Errorf("wrong channel should not match: %+v", got)
	}
	// A different repository must not match.
	if got := x.Match("alice", "gitea", "tag.other/repo.v1"); len(got) != 0 {
		t.Errorf("non-matching topic should not match: %+v", got)
	}
}

// The tenant comes from the subject, so a message in one tenant must never reach another
// tenant's session — even when the channel and pattern are identical.
func TestIndexIsolatesTenants(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-1", "gitea", "tag.>", true)
	put(t, x, "bob", "s2", "sub-1", "gitea", "tag.>", true)

	alice := x.Match("alice", "gitea", "tag.v1")
	if len(alice) != 1 || alice["s1"] == nil {
		t.Fatalf("alice should receive it, got %+v", alice)
	}
	if _, ok := alice["s2"]; ok {
		t.Error("bob's session must not receive alice's message")
	}

	bob := x.Match("bob", "gitea", "tag.v1")
	if _, ok := bob["s1"]; ok {
		t.Error("alice's session must not receive bob's message")
	}
}

// Several sessions following the same channel is the normal case for a group channel.
func TestIndexFansOut(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-1", "group-chat", "eng.>", true)
	put(t, x, "alice", "s2", "sub-1", "group-chat", "eng.>", true)
	put(t, x, "alice", "s3", "sub-1", "group-chat", "ops.>", true)

	got := x.Match("alice", "group-chat", "eng.general")
	if len(got) != 2 {
		t.Fatalf("expected 2 recipients, got %+v", got)
	}
	if got["s1"] == nil || got["s2"] == nil {
		t.Errorf("both eng subscribers should receive it: %+v", got)
	}
	if _, ok := got["s3"]; ok {
		t.Error("the ops subscriber must not receive an eng message")
	}
}

// A session may follow the same channel twice with different patterns; both fire.
func TestIndexMultipleSubscriptionsPerSession(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-a", "gitea", "tag.>", true)
	put(t, x, "alice", "s1", "sub-b", "gitea", "tag.agentdev/mrflow.>", false)

	got := x.Match("alice", "gitea", "tag.agentdev/mrflow.v1")
	subs := got["s1"]
	if len(subs) != 2 {
		t.Fatalf("both patterns should fire, got %+v", subs)
	}
	// The match reports WHICH subscription fired, because Wake differs between them and the
	// distribution step needs to know whether to wake the session.
	var sawWaking, sawNonWaking bool
	for _, s := range subs {
		if s.Wake {
			sawWaking = true
		} else {
			sawNonWaking = true
		}
	}
	if !sawWaking || !sawNonWaking {
		t.Errorf("expected one waking and one non-waking match, got %+v", subs)
	}
}

// Updating a subscription's pattern must replace the old one, not add to it: otherwise a
// user who narrows a subscription keeps receiving the wider set.
func TestIndexUpdateReplacesPattern(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-1", "gitea", "tag.>", true)
	put(t, x, "alice", "s1", "sub-1", "gitea", "tag.agentdev/mrflow.>", true)

	if got := x.Match("alice", "gitea", "tag.other/repo.v1"); len(got) != 0 {
		t.Errorf("the narrowed pattern must not still match the old set: %+v", got)
	}
	if got := x.Match("alice", "gitea", "tag.agentdev/mrflow.v1"); len(got["s1"]) != 1 {
		t.Errorf("the new pattern should match: %+v", got)
	}
}

func TestIndexDeleteRemoves(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-1", "gitea", "tag.>", true)
	del(t, x, "alice", "s1", "sub-1")

	if got := x.Match("alice", "gitea", "tag.v1"); len(got) != 0 {
		t.Errorf("a deleted subscription must stop matching: %+v", got)
	}
	if got := x.List("alice", "s1"); len(got) != 0 {
		t.Errorf("a deleted subscription must not be listed: %+v", got)
	}
}

// Delete removes ONE subscription, leaving the session's others intact.
func TestIndexDeleteOneLeavesOthers(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "s1", "sub-a", "gitea", "tag.>", true)
	put(t, x, "alice", "s1", "sub-b", "group-chat", "eng.>", true)
	del(t, x, "alice", "s1", "sub-a")

	if got := x.List("alice", "s1"); len(got) != 1 || got[0].Channel != "group-chat" {
		t.Fatalf("the other subscription should survive, got %+v", got)
	}
}

// A malformed value is dropped rather than defaulted. Defaulting a bad pattern to ">" would
// subscribe a session to everything, which is far worse than losing one subscription.
func TestIndexDropsMalformedValue(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	x.apply(bus.KvEvent{
		Key:   protocol.SubscriptionKVKey("alice", "s1", "sub-1"),
		Value: `{"channel":"","pattern":""}`,
	})
	if got := x.List("alice", "s1"); len(got) != 0 {
		t.Fatalf("a malformed subscription must be dropped, got %+v", got)
	}
	if got := x.Match("alice", "", ""); len(got) != 0 {
		t.Errorf("a dropped subscription must not match anything: %+v", got)
	}
}

// A session name containing a dot must survive the KV key round trip, or the index would
// file the subscription under the wrong session.
func TestIndexDottedSessionName(t *testing.T) {
	x := NewSubscriptionIndex(nil)
	put(t, x, "alice", "team.reviewer", "sub-1", "gitea", "tag.>", true)

	if got := x.List("alice", "team.reviewer"); len(got) != 1 {
		t.Fatalf("a dotted session name must round trip, got %+v", got)
	}
	if got := x.Match("alice", "gitea", "tag.v1"); got["team.reviewer"] == nil {
		t.Errorf("the dotted session should match, got %+v", got)
	}
}

func TestParseSubscriptionKey(t *testing.T) {
	tenant, session, id, ok := parseSubscriptionKey("t.alice.s.perm-parent.sub-1")
	if !ok || tenant != "alice" || session != "perm-parent" || id != "sub-1" {
		t.Fatalf("got (%q,%q,%q,%v)", tenant, session, id, ok)
	}
	for _, bad := range []string{"", "x.alice.s.a.b", "t.alice.s.a", "t..s.a.b"} {
		if _, _, _, ok := parseSubscriptionKey(bad); ok {
			t.Errorf("parseSubscriptionKey(%q) should have failed", bad)
		}
	}
}
