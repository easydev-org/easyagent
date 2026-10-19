package server

import (
	"context"
	"testing"

	"connectrpc.com/connect/v2"
	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

// TestUndoReturnsPatchedSession: undo no longer re-reads the session row; it
// patches the row it already loaded. The RESPONSE must still carry the new
// tip_id and the timestamp SetTip actually wrote, and the STORED row must
// agree with it.
func TestUndoReturnsPatchedSession(t *testing.T) {
	ts, db, srv := newTestServerWithSrv(t)
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s1"}); err != nil {
		t.Fatal(err)
	}
	// u1 -> a1 -> u2 (tip)
	must := func(id, role string, prev *string) {
		if _, err := db.InsertMessage("acme", id, role, prev, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := db.AppendMessageTip("acme", "s1", id); err != nil {
			t.Fatal(err)
		}
	}
	must("u1", "user", nil)
	p := "u1"
	must("a1", "assistant", &p)
	p = "a1"
	must("u2", "user", &p)

	before, ok, err := db.GetSession("acme", "s1")
	if err != nil || !ok {
		t.Fatalf("get session: %v %v", ok, err)
	}
	if before.TipID == nil || *before.TipID != "u2" {
		t.Fatalf("precondition: tip = %v, want u2", before.TipID)
	}

	// Undo u2 => tip should move to a1.
	res, err := c.Undo(ctx, &agentv1.UndoRequest{Id: "s1", MessageId: "u2"})
	if err != nil {
		t.Fatalf("undo: %v", err)
	}
	if res.Session == nil {
		t.Fatal("undo returned no session")
	}
	if res.Session.TipId != "a1" {
		t.Errorf("response tip = %q, want a1", res.Session.TipId)
	}
	if res.Session.UpdatedAt == "" {
		t.Error("response updated_at is empty")
	}

	// The stored row must agree with what the response claimed.
	after, _, err := db.GetSession("acme", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if after.TipID == nil || *after.TipID != "a1" {
		t.Errorf("stored tip = %v, want a1", after.TipID)
	}
	if after.UpdatedAt != res.Session.UpdatedAt {
		t.Errorf("stored updated_at = %q, response = %q (must be identical)", after.UpdatedAt, res.Session.UpdatedAt)
	}
	// And the chain is now u1, a1.
	msgs, err := srv.db.Chain("acme", *after.TipID, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[0].ID != "u1" || msgs[1].ID != "a1" {
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		t.Errorf("chain after undo = %v, want [u1 a1]", ids)
	}
	_ = connect.CodeInvalidArgument
	_ = agent.CompactionRole
}
