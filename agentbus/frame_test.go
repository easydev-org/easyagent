package agentbus

import "testing"

/**
 * SINCE-SEQ MUST FILTER, NOT JUST SWITCH REPLAY ON.
 *
 * It used to mean only "replay something": the whole retained window went out and the client's
 * `eid` dedup cleaned up the overlap. A client echoing its newest `seq` expects the frames AFTER it,
 * and the annotation in the proto promises exactly that.
 *
 * A FRAME WITHOUT A SEQ IS KEPT — ephemeral deltas carry none, and dropping them would omit frames
 * the client has not seen, which is worse than sending one it has.
 */
func TestFrameAfterFiltersBySeq(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		sinceSeq uint64
		want     bool
	}{
		{"newer", `{"seq":11}`, 10, true},
		{"same", `{"seq":10}`, 10, false},
		{"older", `{"seq":9}`, 10, false},
		{"no-seq-is-kept", `{"event":"text-delta"}`, 10, true},
		{"unparseable-is-kept", `not json`, 10, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := frameAfter([]byte(c.raw), c.sinceSeq, 0); got != c.want {
				t.Fatalf("frameAfter(%s, %d) = %v, want %v", c.raw, c.sinceSeq, got, c.want)
			}
		})
	}
}

/**
 * THE TIME ANCHOR IS THE PAGE-REFRESH FALLBACK, and it must filter too.
 *
 * A refresh has no in-memory `seq`, only the newest message id it holds. The server resolves that to
 * a timestamp; without filtering on it the refresh replays the entire retained window.
 */
func TestFrameAfterFiltersByTime(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		sinceAt int64
		want    bool
	}{
		{"newer", `{"seq":3,"at":2000}`, 1500, true},
		{"same-instant-is-kept", `{"seq":3,"at":1500}`, 1500, true},
		{"older", `{"seq":3,"at":1000}`, 1500, false},
		{"no-timestamp-is-kept", `{"seq":3}`, 1500, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := frameAfter([]byte(c.raw), 0, c.sinceAt); got != c.want {
				t.Fatalf("frameAfter(%s, seq=0, at=%d) = %v, want %v", c.raw, c.sinceAt, got, c.want)
			}
		})
	}
}

// The SEQUENCE wins when both anchors are set, which is what the proto promises.
func TestSeqWinsOverTimeAnchor(t *testing.T) {
	// seq 11 > 10 says "keep", while at 1000 < 1500 says "drop". The seq must decide.
	if !frameAfter([]byte(`{"seq":11,"at":1000}`), 10, 1500) {
		t.Fatal("the time anchor overrode since_seq")
	}
}
