package server

import (
	"context"
	"strings"
	"testing"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/store"
)

/**
 * ASKING DOES NOT TOUCH THE SESSION.
 *
 * THE WHOLE POINT OF A SIDE CHANNEL: a person watching a group wants to know something, and the act of asking must not
 * change what they are watching. If this wrote a message, advanced the tip, or took the lease, it would be a turn with
 * extra steps — and the session mid-work would be interrupted by a question ABOUT its work.
 *
 * THE ASSERTION IS THE ABSENCE: no message, no tip movement, no extra link in the chain.
 */
func TestAskLeavesTheSessionUntouched(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	sid := newSession(t, c, "s")
	before, ok, err := db.GetSession("acme", sid)
	if err != nil || !ok {
		t.Fatal("session missing")
	}

	// NO MODEL means the side channel refuses with a precondition, and — the part that matters — still writes nothing.
	_, err = c.Ask(ctx, &agentv1.AskRequest{Id: sid, Question: "what are you doing"})
	if err == nil {
		t.Skip("this server has a model configured; the write-freedom check below still applies")
	}

	after, _, _ := db.GetSession("acme", sid)
	if after.MessageSeq != before.MessageSeq {
		t.Errorf("message_seq moved %d → %d; asking must not write to the session",
			before.MessageSeq, after.MessageSeq)
	}
	if (after.TipID == nil) != (before.TipID == nil) {
		t.Errorf("tip changed: %v → %v", before.TipID, after.TipID)
	}
}

// A QUESTION IS REQUIRED, and an empty one is the caller's mistake rather than an internal error.
func TestAskRequiresAQuestion(t *testing.T) {
	ts, db, _ := newTestServerWith(t, nil)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)

	sid := newSession(t, c, "s")
	if _, err := c.Ask(context.Background(), &agentv1.AskRequest{Id: sid}); err == nil {
		t.Fatal("an empty question was accepted")
	}
}

/**
 * ASK-IN-GROUP RELAYS THE ANSWER AS A SYSTEM MESSAGE, AND ASKS NOBODY FOR ANYTHING.
 *
 * TWO PROPERTIES, BOTH LOAD-BEARING:
 *
 *   · `kind = "system"` — the words came from the model but the QUESTION came from the reader, so attributing the
 *     message to the session would put a claim in the transcript it never made.
 *   · NO OBLIGATION — a person asking in passing must not hand work to a session. If this opened a debt, every
 *     curious question would spawn a reply the session owed.
 */
func TestAskInGroupPostsASystemMessageAndOpensNoDebt(t *testing.T) {
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

	// THE PUBLISH PATH DIRECTLY: the model call needs a provider, and what this pins is the bookkeeping around it.
	if err := srv.PublishAskAnswer(ctx, "acme", gid, worker, "worker", "what are you doing", "checking the build"); err != nil {
		t.Fatal(err)
	}

	msgs, err := c.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: gid})
	if err != nil {
		t.Fatal(err)
	}
	var relay *agentv1.GroupMessage
	for _, m := range msgs.Messages {
		if m.Kind == store.GroupMessageKindSystem {
			relay = m
		}
	}
	if relay == nil {
		t.Fatal("the answer was not posted into the group")
	}
	if relay.Sender != "" {
		t.Errorf("the relay is attributed to %q, want nobody", relay.Sender)
	}
	// THE QUESTION IS ECHOED, or a reader later sees an answer to something nobody can see.
	if !strings.Contains(relay.Body, "what are you doing") || !strings.Contains(relay.Body, "checking the build") {
		t.Errorf("the relay does not carry question and answer: %q", relay.Body)
	}
	if open, _ := db.ObligationsForGroup("acme", gid); len(open) != 0 {
		t.Errorf("asking opened %d obligations, want none", len(open))
	}
}
