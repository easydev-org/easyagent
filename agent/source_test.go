package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/easydev-org/easybus-sdk/mailbox"
)

/**
 * THE ROW'S ORIGIN IS THE MESSAGE'S ORIGIN.
 *
 * A trigger used to be written with `source: "user"` regardless of who sent it, which told the
 * console the reader had typed the message: a job's completion notice rendered right-aligned, with no
 * avatar, indistinguishable from something they said themselves. The console has always known how to
 * render `system:<name>` — nothing was sending it.
 *
 * THESE PIN THE MAPPING, because the failure is silent in every direction: the message appears, reads
 * correctly, and is merely attributed to the wrong author.
 */
func TestTriggerSourceIsCarriedThrough(t *testing.T) {
	for _, tc := range []struct {
		mailboxSource string
		want          string
		why           string
	}{
		{"system:sandbox", "system:sandbox", "a job's completion is automation, not a prompt"},
		{"system:gitdev", "system:gitdev", "the same for a CI run"},
		{"session:parent", "session:parent", "a subsession hand-off is another conversation's message"},
		{"", "user", "an API prompt names no sender, and the reader wrote it"},
		{"user", "user", "the explicit case the API uses"},
	} {
		if got := messageSource(tc.mailboxSource); got != tc.want {
			t.Errorf("%q -> %q, want %q (%s)", tc.mailboxSource, got, tc.want, tc.why)
		}
	}

	// AND THE PAYLOAD'S TEXT MUST STILL DECODE: the source is carried alongside it, not instead.
	raw, _ := json.Marshal(WakePayload{MessageID: "m1", Text: "<job ...>"})
	var p WakePayload
	if err := json.Unmarshal(raw, &p); err != nil || p.Text == "" {
		t.Fatalf("the wake payload still has to decode: %+v %v", p, err)
	}
}

/**
 * THE QUEUE KEEPS THE SOURCE VERBATIM.
 *
 * A wrapper that normalised it would silently undo the distinction the whole origin change is about — and
 * the queue is now a different store than the one this test was written against, so it is worth checking
 * that the value survives the move rather than assuming it.
 */
func TestQueueKeepsTheSourceVerbatim(t *testing.T) {
	q := mailbox.NewMemory()
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, "t", "s", "m1", "trigger", map[string]any{"text": "x"}, "system:sandbox"); err != nil {
		t.Fatal(err)
	}
	msgs, err := q.Pending(ctx, "t", "s")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Source != "system:sandbox" {
		t.Fatalf("source did not survive the queue: %+v", msgs)
	}
}
