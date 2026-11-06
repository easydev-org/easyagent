package server

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * WATCHGROUP REPLAYS WHAT IS ALREADY THERE, THEN FOLLOWS LIVE.
 *
 * Unlike WatchSession there is no frame log to resume: the messages are rows, so "everything after N" is a
 * query. That is why the first assertion here is about a message posted BEFORE the watch opened — the
 * transcript is the source of truth, and a client that reconnects gets what it missed without any
 * server-side cursor being kept for it.
 */
func TestWatchGroupReplaysThenFollows(t *testing.T) {
	ts, db, _ := newTestServerWith(t, func(d *Deps) {
		// A short tick so the test does not wait half a second per message.
		d.GroupWatchTick = 20 * time.Millisecond
	})
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// Posted BEFORE the watch opens — this is the replay half.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "already here",
	}); err != nil {
		t.Fatal(err)
	}

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := c.WatchGroup(streamCtx, &agentv1.WatchGroupRequest{GroupId: groupIDByName(t, c, "g")})
	if err != nil {
		t.Fatal(err)
	}

	// The first frame is `ready` with the group's current position.
	first, err := stream.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetEvent() != "ready" {
		t.Fatalf("first event = %q, want ready", first.GetEvent())
	}
	if first.GetLatestSeq() != 1 {
		t.Errorf("latest_seq = %d, want 1", first.GetLatestSeq())
	}

	// Then the retained message.
	replayed, err := stream.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if replayed.GetEvent() != "message" || replayed.GetMessage().GetBody() != "already here" {
		t.Fatalf("replay = %+v", replayed)
	}

	// Then a LIVE one, posted after the stream was open.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupIDByName(t, c, "g"), Text: "live one",
	}); err != nil {
		t.Fatal(err)
	}
	live, err := stream.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if live.GetMessage().GetBody() != "live one" {
		t.Fatalf("live = %+v", live)
	}
	if live.GetMessage().GetSeq() != 2 {
		t.Errorf("live seq = %d, want 2", live.GetMessage().GetSeq())
	}
}

/**
 * `since_seq` IS EXCLUSIVE, so a reconnecting client does not receive the message it already has.
 *
 * Without that, every reconnect would duplicate the last message in the reader's view — the kind of bug that
 * looks like a delivery problem rather than a cursor one.
 */
func TestWatchGroupSinceSeqIsExclusive(t *testing.T) {
	ts, db, _ := newTestServerWith(t, func(d *Deps) {
		d.GroupWatchTick = 20 * time.Millisecond
	})
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{GroupId: groupIDByName(t, c, "g"), Text: "m"}); err != nil {
			t.Fatal(err)
		}
	}

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := c.WatchGroup(streamCtx, &agentv1.WatchGroupRequest{GroupId: groupIDByName(t, c, "g"), SinceSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Receive(); err != nil { // ready
		t.Fatal(err)
	}
	next, err := stream.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if next.GetMessage().GetSeq() != 3 {
		t.Errorf("after since_seq=2 the first message is seq %d, want 3", next.GetMessage().GetSeq())
	}
}

// A non-member cannot watch: NOT_FOUND, the same answer as a group that does not exist.
func TestWatchGroupRequiresMembership(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	outsider := newSession(t, c, "mallory")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	stream, err := c.WatchGroup(ctx, &agentv1.WatchGroupRequest{GroupId: groupIDByName(t, c, "g"), Session: outsider})
	if err == nil {
		// The error may arrive on the first Receive rather than the call itself, depending on the transport.
		if _, rerr := stream.Receive(); rerr == nil {
			t.Fatal("a non-member watched the group")
		}
	}
}
