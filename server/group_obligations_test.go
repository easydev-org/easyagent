package server

import (
	"context"
	"strings"
	"testing"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * BEING ADDRESSED CREATES A DEBT, AND ANSWERING YOUR ASKER SETTLES IT.
 *
 * THIS IS THE WHOLE FEATURE IN TWO ASSERTIONS. `group-send` with a `to` list now records that each named session owes
 * a reply, and a reply ADDRESSED BACK to the asker settles it. Everything else — the reminder, the count, the bound —
 * exists because these two facts are now knowable.
 *
 * THE ADDRESSING IS THE RULE. A message to the whole group settles nothing: it is seen by everyone and confirms
 * nothing to anyone in particular, so allowing it would let "收到" satisfy an obligation whose point was to get an
 * answer.
 */
func TestAddressingCreatesAnObligationAndTheReplySettlesIt(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	gid := created.Group.Id

	/**
	 * THE COORDINATOR ASKS THE WORKER SOMETHING.
	 *
	 * THROUGH THE STORE, because the RPC cannot express it: `SendGroupMessage.to` is reserved — a message posted
	 * through the console has exactly one recipient, decided by the sender's role, and only the `group-send` TOOL
	 * addresses members. The tool path is covered by the bundled tests; what this file pins is the obligation those
	 * calls create.
	 */
	posted, err := db.AppendGroupMessage(store.GroupMessage{
		ID: "m1", Tenant: "acme", GroupID: gid, Kind: store.GroupMessageKindSession,
		Sender: coord, Body: "please check main",
	}, nowRFC3339())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReplyObligations("acme", gid, coord, []string{worker}, posted.Seq, posted.ID, nowRFC3339()); err != nil {
		t.Fatal(err)
	}

	open, err := db.ObligationsForGroup("acme", gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 {
		t.Fatalf("obligations = %d, want 1", len(open))
	}
	if open[0].Session != worker || open[0].From != coord {
		t.Errorf("obligation = %+v, want worker owes coord", open[0])
	}

	// A MESSAGE TO THE GROUP DOES NOT SETTLE IT — nothing addressed it to the asker.
	open, _ = db.ObligationsForGroup("acme", gid)
	if len(open) != 1 {
		t.Fatalf("a group broadcast settled the debt; obligations = %d, want 1", len(open))
	}

	// ADDRESSING THE ASKER DOES.
	if _, err := db.FulfilReplyObligations("acme", gid, worker, coord, nowRFC3339()); err != nil {
		t.Fatal(err)
	}
	open, _ = db.ObligationsForGroup("acme", gid)
	if len(open) != 0 {
		t.Fatalf("the reply did not settle it; obligations = %+v", open)
	}
}

/**
 * A TURN THAT ENDS WITH A DEBT OPEN IS CHASED, ONCE, AS A SYSTEM MESSAGE.
 *
 * THE AUTHOR IS THE POINT: `kind = "system"` and an empty sender. Rendering it as the asker's message would read as
 * that session nagging — the exact confusion this feature exists to remove — and rendering it as the user's would
 * make the person appear to chase their own worker.
 */
func TestAnEndedTurnWithAnOpenDebtIsReminded(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	gid := created.Group.Id
	posted, err := db.AppendGroupMessage(store.GroupMessage{
		ID: "m-" + worker, Tenant: "acme", GroupID: gid, Kind: store.GroupMessageKindSession,
		Sender: coord, Body: "do the thing",
	}, nowRFC3339())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReplyObligations("acme", gid, coord, []string{worker}, posted.Seq, posted.ID, nowRFC3339()); err != nil {
		t.Fatal(err)
	}

	// THE TURN ENDS without an answer.
	srv.ChaseUnansweredReplies(ctx, "acme", worker)

	msgs, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: gid})
	if err != nil {
		t.Fatal(err)
	}
	var reminder *agentv1.GroupMessage
	for _, m := range msgs.Messages {
		if m.Kind == store.GroupMessageKindSystem {
			reminder = m
		}
	}
	if reminder == nil {
		t.Fatal("a turn ended owing a reply and nothing was said")
	}
	if reminder.Sender != "" {
		t.Errorf("the reminder is attributed to %q, want nobody — it is the platform's", reminder.Sender)
	}
	if !strings.Contains(reminder.Body, "1/3") {
		t.Errorf("the reminder does not say which attempt this is: %q", reminder.Body)
	}

	// AND IT IS BOUNDED.
	for i := 0; i < 10; i++ {
		srv.ChaseUnansweredReplies(ctx, "acme", worker)
	}
	msgs, _ = c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: gid})
	var reminders int
	for _, m := range msgs.Messages {
		if m.Kind == store.GroupMessageKindSystem {
			reminders++
		}
	}
	if reminders > 3 {
		t.Errorf("reminders = %d, want at most 3", reminders)
	}
}

/**
 * AN ANSWERED DEBT IS NEVER CHASED.
 *
 * The cheap way to get this wrong is to remind on "has an obligation row", which would also match rows already
 * settled. That failure is invisible in a transcript — the reminder and the answer would both be there — and would
 * make the system nag a session that had already complied.
 */
func TestASettledDebtIsNotReminded(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	gid := created.Group.Id
	posted, err := db.AppendGroupMessage(store.GroupMessage{
		ID: "m-" + worker, Tenant: "acme", GroupID: gid, Kind: store.GroupMessageKindSession,
		Sender: coord, Body: "do the thing",
	}, nowRFC3339())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReplyObligations("acme", gid, coord, []string{worker}, posted.Seq, posted.ID, nowRFC3339()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FulfilReplyObligations("acme", gid, worker, coord, nowRFC3339()); err != nil {
		t.Fatal(err)
	}

	srv.ChaseUnansweredReplies(ctx, "acme", worker)

	msgs, _ := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: gid})
	for _, m := range msgs.Messages {
		if m.Kind == store.GroupMessageKindSystem {
			t.Errorf("a settled debt was chased: %q", m.Body)
		}
	}
}

/**
 * A DEPARTED SESSION OWES NOTHING.
 *
 * An obligation outlives its ability to be discharged the moment the session is deleted or removed from the group. Kept,
 * it is a permanent "unanswered" mark nobody can clear.
 */
func TestLeavingTheGroupDropsTheDebt(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	coord := newSession(t, c, "coord")
	worker := newSession(t, c, "worker")
	created, err := c.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: "g", Coordinator: coord, Members: []string{worker}, CreatedBy: "user",
	})
	if err != nil {
		t.Fatal(err)
	}
	gid := created.Group.Id
	posted, err := db.AppendGroupMessage(store.GroupMessage{
		ID: "m1", Tenant: "acme", GroupID: gid, Kind: store.GroupMessageKindSession,
		Sender: coord, Body: "do it",
	}, nowRFC3339())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.OpenReplyObligations("acme", gid, coord, []string{worker}, posted.Seq, posted.ID, nowRFC3339()); err != nil {
		t.Fatal(err)
	}
	if open, _ := db.ObligationsForGroup("acme", gid); len(open) != 1 {
		t.Fatalf("setup: obligations = %d, want 1", len(open))
	}

	if _, err := c.RemoveGroupMember(ctx, &agentv1.RemoveGroupMemberRequest{
		GroupId: gid, Member: worker,
	}); err != nil {
		t.Fatal(err)
	}
	if open, _ := db.ObligationsForGroup("acme", gid); len(open) != 0 {
		t.Errorf("a removed member still owes %+v", open)
	}
}
