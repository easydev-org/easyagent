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

/**
 * `to` IS THE "@" — AND WHAT IT MAY CONTAIN DEPENDS ON WHO IS SENDING.
 *
 * A GROUP ROUTES UPWARD AND DOWNWARD, NEVER SIDEWAYS. The coordinator addresses members; a member addresses
 * the coordinator; a user addresses the group (which means the coordinator). Letting a member name a peer
 * would make the coordinator optional, and it is the one thing that knows what it handed out.
 */
func TestGroupSendTargetsFollowTheRole(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	w1 := newSession(t, c, "w1")
	w2 := newSession(t, c, "w2")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{w1, w2}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	// COORDINATOR → the members it names, and only those.
	res, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "both of you", Sender: coord, To: []string{w1, w2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 2 {
		t.Fatalf("delivered to %v, want both members", res.DeliveredTo)
	}

	// Duplicates collapse: naming one twice is a mistake, not two deliveries.
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "once", Sender: coord, To: []string{w1, w1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 1 {
		t.Errorf("delivered to %v, want one", res.DeliveredTo)
	}

	// COORDINATOR with no `to` is RECORDED BUT ADDRESSES NOBODY — distinct from delivering to everyone.
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "a note", Sender: coord,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 0 {
		t.Errorf("delivered to %v, want nobody", res.DeliveredTo)
	}

	// A USER reaches the coordinator by default...
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{Group: "g", Text: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("a user's message went to %v, want the coordinator", res.DeliveredTo)
	}

	// ...AND CANNOT NAME MEMBERS. A user writes to the GROUP; its coordinator decides who works on what. Letting
	// a user reach past it would make the coordinator optional at the moment its judgement is the point.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "w1, look at this", To: []string{w1},
	}); err == nil {
		t.Error("a user addressed a member directly")
	}

	// A MEMBER reports upward. Naming anyone — even the coordinator — is refused, because the default is
	// already the coordinator and an explicit name would suggest peer addressing exists.
	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "done", Sender: w1, To: []string{w2},
	}); err == nil {
		t.Error("a member addressed a peer")
	}
	res, err = c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{Group: "g", Text: "done", Sender: w1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DeliveredTo) != 1 || res.DeliveredTo[0] != coord {
		t.Errorf("a member's report went to %v, want the coordinator", res.DeliveredTo)
	}
}

/**
 * A BAD NAME REFUSES THE WHOLE MESSAGE — NOTHING IS STORED AND NOBODY IS WOKEN.
 *
 * A half-addressed instruction is worse than a refused one: some members work on something the others never
 * saw, and the transcript does not say which.
 */
func TestGroupSendRefusesTheWholeMessageOnABadTarget(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	w1 := newSession(t, c, "w1")
	outsider := newSession(t, c, "mallory")
	if _, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{w1}, CreatedBy: "user",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: "g", Text: "x", Sender: coord, To: []string{w1, outsider},
	}); err == nil {
		t.Fatal("a non-member was addressed")
	}

	// NOTHING WAS STORED: the transcript is still empty, so no recipient can find a half-run instruction.
	list, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Messages) != 0 {
		t.Errorf("a refused message was still stored: %+v", list.Messages)
	}
}
