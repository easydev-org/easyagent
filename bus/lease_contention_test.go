package bus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/natsrun"
)

// startBusPair returns TWO independently-connected Buses against ONE broker.
//
// Two Bus values are what "two replicas" reduces to at this layer: each has its own client
// connection and its own in-memory `held` map, and they contend through the broker exactly as two
// pods would. A single Bus cannot reproduce the failure this file exists to cover, because the
// lease is enforced by KV, not by the map.
func startBusPair(t *testing.T) (*Bus, *Bus) {
	t.Helper()
	srv, err := natsrun.Start(natsrun.Config{Port: 0})
	if err != nil {
		t.Skipf("nats-server unavailable: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	a, err := Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := Connect(srv.URL())
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return a, b
}

// Only one of two racing replicas may claim a session.
//
// This is the invariant the whole lease exists for. If both win, both run turns for the same
// session: two writers on one message chain, duplicated tool calls, and a CAS war that shows up as
// `wrong last sequence` forever after.
func TestLeaseOnlyOneReplicaClaims(t *testing.T) {
	r1, r2 := startBusPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const tenant, session = "t1", "contended"

	// Start both claims as close together as possible.
	var wg sync.WaitGroup
	results := make([]bool, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	for i, b := range []*Bus{r1, r2} {
		wg.Add(1)
		go func(i int, b *Bus) {
			defer wg.Done()
			<-start
			results[i], errs[i] = b.Claim(ctx, tenant, session)
		}(i, b)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d claim error: %v", i, err)
		}
	}
	won := 0
	for _, ok := range results {
		if ok {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d replicas claimed the same session, want exactly 1 (results=%v)\n"+
			"two holders mean two concurrent turns on one message chain", won, results)
	}
}

// A replica that does NOT hold the lease must report "not held" rather than "lost".
//
// `Renew` distinguishes them: (false, nil) means another replica took over and the turn must stop;
// (true, err) means the call failed and should be retried. Reporting a bare loss for a replica that
// never had the lease would make it abort a turn it is not even running.
func TestLeaseRenewFromNonHolderIsNotALoss(t *testing.T) {
	r1, r2 := startBusPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const tenant, session = "t1", "renew-other"

	ok, err := r1.Claim(ctx, tenant, session)
	if err != nil || !ok {
		t.Fatalf("r1 claim: ok=%v err=%v", ok, err)
	}

	// r2 never claimed, so its held revision is 0. A CAS at revision 0 on an existing key loses.
	held, err := r2.Renew(ctx, tenant, session)
	if err != nil {
		t.Fatalf("a non-holder's renew reported an ERROR (%v); the caller treats that as a "+
			"transient failure and retries, when it should stop", err)
	}
	if held {
		t.Fatal("a non-holder's renew reported the lease as HELD: it would keep running a turn " +
			"another replica owns")
	}

	// And the real holder is unaffected — the non-holder's attempt must not steal or break it.
	held, err = r1.Renew(ctx, tenant, session)
	if err != nil {
		t.Fatalf("holder renew after a non-holder attempt: %v", err)
	}
	if !held {
		t.Fatal("a non-holder's renew STOLE the lease from the holder")
	}
}

// Release by one replica must let the other take over immediately.
func TestLeaseHandoffAfterRelease(t *testing.T) {
	r1, r2 := startBusPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const tenant, session = "t1", "handoff"

	if ok, err := r1.Claim(ctx, tenant, session); err != nil || !ok {
		t.Fatalf("r1 claim: ok=%v err=%v", ok, err)
	}
	if ok, err := r2.Claim(ctx, tenant, session); err != nil {
		t.Fatalf("r2 claim: %v", err)
	} else if ok {
		t.Fatal("r2 claimed while r1 held the lease")
	}

	if err := r1.Release(ctx, tenant, session); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, err := r2.Claim(ctx, tenant, session); err != nil {
		t.Fatalf("r2 re-claim: %v", err)
	} else if !ok {
		t.Fatal("r2 could not claim after r1 released: the lease is stuck")
	}
}

// A lost lease must be reported as LOST, not as a transport error.
//
// This is the case that used to kill healthy turns: the old code returned rev 0 for every error,
// so `Renew` could not tell "another replica took over" from "the broker hiccuped". Now the
// conflict path returns (false, nil) and only real failures return an error.
func TestLeaseRenewAfterTakeoverReportsLost(t *testing.T) {
	r1, r2 := startBusPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const tenant, session = "t1", "takeover"

	if ok, err := r1.Claim(ctx, tenant, session); err != nil || !ok {
		t.Fatalf("r1 claim: ok=%v err=%v", ok, err)
	}
	// Force a takeover: r1 drops it, r2 takes it. r1's cached revision is now stale.
	if err := r1.Release(ctx, tenant, session); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, err := r2.Claim(ctx, tenant, session); err != nil || !ok {
		t.Fatalf("r2 claim: ok=%v err=%v", ok, err)
	}

	// r1 still believes it holds revision 1; the CAS against r2's revision loses.
	held, err := r1.Renew(ctx, tenant, session)
	if err != nil {
		t.Fatalf("a stale renew reported an ERROR (%v) instead of a clean loss; the runtime "+
			"would retry forever instead of stopping", err)
	}
	if held {
		t.Fatal("a stale renew reported the lease as still held; both replicas would run a turn")
	}
}
