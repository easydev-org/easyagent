package server

import (
	"context"
	"testing"
	"time"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// Ported from the TS agent's watch-abort.test.ts: a watch stream MUST return
// when the client disconnects (ctx cancelled), even while parked with an empty
// queue — otherwise its subscriptions leak. Here the client cancels its
// context and we assert the server handler returns promptly.

func TestWatchSessionReturnsOnClientAbort(t *testing.T) {
	ts, db := newTestServer(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	if _, err := c.CreateSession(context.Background(), &agentv1.CreateSessionRequest{Name: "s1", Model: "gw/m"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.WatchSession(ctx, &agentv1.WatchSessionRequest{Id: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	// The stream is open and parked (no events). Cancel the client.
	cancel()

	done := make(chan struct{})
	go func() {
		// Recv must unblock with an error once the context is cancelled.
		_, _ = stream.Receive()
		close(done)
	}()
	select {
	case <-done:
		// returned — good, no leak.
	case <-time.After(3 * time.Second):
		t.Fatal("WatchSession did not return after client abort")
	}
}

func TestWatchSessionsReturnsOnClientAbort(t *testing.T) {
	ts, db := newTestServer(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	if _, err := c.CreateSession(context.Background(), &agentv1.CreateSessionRequest{Name: "s1"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.WatchSessions(ctx, &agentv1.WatchSessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// First frame is the initial snapshot.
	first, err := stream.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if !first.Snapshot {
		t.Fatalf("first frame should be the snapshot: %+v", first)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		_, _ = stream.Receive()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("WatchSessions did not return after client abort")
	}
}
