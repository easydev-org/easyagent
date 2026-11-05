package server

import (
	"context"
	"testing"

	"connectrpc.com/connect/v2"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * A GROUP HAS ITS OWN TRANSCRIPT, AND A MESSAGE IS STORED BEFORE ANYONE HEARS IT.
 *
 * The order is the contract: a recipient that receives a message can always find it. Delivering first would
 * allow a message to be heard and never recorded — the one failure that cannot be repaired, because there
 * is nothing to point at.
 */
func TestGroupMessageIsStoredThenDelivered(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	member := newSession(t, c, "worker")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{member}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// A USER posts: it reaches the coordinator and lands in the transcript.
	res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "please split this",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Message == nil || res.Message.Seq != 1 {
		t.Fatalf("stored message = %+v, want seq 1", res.Message)
	}
	// AN EMPTY SENDER MEANS THE USER, which is a real distinction the console renders.
	if res.Message.Sender != "" {
		t.Errorf("sender = %q, want empty for a user post", res.Message.Sender)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("delivered to %v, want the coordinator only", res.DeliveredTo)
	}

	// AND IT IS READABLE afterwards — the whole point of a transcript.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 1 || list.Messages[0].Body != "please split this" {
		t.Fatalf("transcript = %+v", list.Messages)
	}
	if list.LatestSeq != 1 {
		t.Errorf("latest_seq = %d, want 1", list.LatestSeq)
	}
}

// Sequence numbers are assigned per group and strictly increase, so a watch can resume from the newest one
// it saw without gaps.
func TestGroupMessageSeqIncreases(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
			Group: "g", Text: "m", Sender: coord,
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Message.Seq != int64(i) {
			t.Fatalf("message %d got seq %d", i, res.Message.Seq)
		}
	}

	// The cursor is EXCLUSIVE: after 2 means "3 onward".
	page, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g", AfterSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].Seq != 3 {
		t.Fatalf("after_seq=2 returned %+v", page.Messages)
	}
}

/**
 * A NON-MEMBER CANNOT POST, AND CANNOT READ.
 *
 * Both answers are NOT_FOUND rather than PERMISSION_DENIED: the difference would let any session discover
 * which groups a tenant has by watching which names fail differently.
 */
func TestGroupMessagesRequireMembership(t *testing.T) {
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

	_, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "hi", Sender: outsider,
	})
	if err == nil {
		t.Fatal("a non-member posted to the group")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", got)
	}

	_, err = c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g", Session: outsider})
	if err == nil {
		t.Fatal("a non-member read the transcript")
	}
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Errorf("code = %v, want NotFound", got)
	}

	// And the member CAN.
	if _, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g", Session: coord}); err != nil {
		t.Fatalf("the coordinator could not read its own group: %v", err)
	}
}

// Deleting a group deletes its transcript: the rows would otherwise be unreachable and a later group with
// the same name would inherit a stranger's conversation.
func TestDeleteGroupRemovesTranscript(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{Group: "g", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteGroup(ctx, &agentv1.DeleteGroupRequest{Name: "g"}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 0 {
		t.Errorf("the recreated group inherited %d old message(s)", len(list.Messages))
	}
	if list.LatestSeq != 0 {
		t.Errorf("latest_seq = %d, want 0", list.LatestSeq)
	}
}

/**
 * A GROUP ALWAYS HAS A COORDINATOR, AND THAT IS ENFORCED WHERE THE STATE WOULD BE CREATED.
 *
 * The store refuses to remove the last coordinator ("move the role first"), so the "routes nowhere" case
 * cannot be reached through the API — which is the right place to prevent it. This test records that
 * guarantee rather than simulating an unreachable state: a handler for it would be code no deployment can
 * exercise, and the next reader would assume it protects something.
 */
func TestCoordinatorCannotBeLeftAbsent(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RemoveGroupMember("acme", "g", coord); err == nil {
		t.Fatal("the store allowed the last coordinator to be removed")
	}
	// And the group still posts, because nothing was changed.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{Group: "g", Text: "x"}); err != nil {
		t.Fatalf("the group stopped accepting messages: %v", err)
	}
}
