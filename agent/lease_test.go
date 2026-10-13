package agent

import (
	"context"
	"testing"
)

// The run lease guarantees at most one writer per session (session-lock.test.ts
// in the reference). Ported here for MemoryLease; the NATS-backed lease is
// exercised in the bus package against a live broker.

func TestMemoryLeaseExclusivePerSession(t *testing.T) {
	l := NewMemoryLease()
	ctx := context.Background()

	ok, err := l.Claim(ctx, "t", "s")
	if err != nil || !ok {
		t.Fatalf("first claim = %v,%v; want true", ok, err)
	}
	// A second claim on the same session fails while held.
	ok, err = l.Claim(ctx, "t", "s")
	if err != nil || ok {
		t.Fatalf("second claim = %v,%v; want false", ok, err)
	}
	// A DIFFERENT session is independent.
	ok, _ = l.Claim(ctx, "t", "s2")
	if !ok {
		t.Fatal("a different session should be claimable")
	}
	// A different TENANT with the same session name is independent.
	ok, _ = l.Claim(ctx, "t2", "s")
	if !ok {
		t.Fatal("a different tenant should be independent")
	}

	// Release frees the session for a new claim.
	if err := l.Release(ctx, "t", "s"); err != nil {
		t.Fatal(err)
	}
	ok, _ = l.Claim(ctx, "t", "s")
	if !ok {
		t.Fatal("released session should be claimable again")
	}
}

func TestMemoryLeaseIsHeld(t *testing.T) {
	l := NewMemoryLease()
	ctx := context.Background()
	if held, _ := l.IsHeld(ctx, "t", "s"); held {
		t.Fatal("unclaimed session reported held")
	}
	_, _ = l.Claim(ctx, "t", "s")
	if held, _ := l.IsHeld(ctx, "t", "s"); !held {
		t.Fatal("claimed session reported not held")
	}
	_ = l.Release(ctx, "t", "s")
	if held, _ := l.IsHeld(ctx, "t", "s"); held {
		t.Fatal("released session reported held")
	}
}

func TestMemoryLeaseRenew(t *testing.T) {
	l := NewMemoryLease()
	ctx := context.Background()
	// Renew before any claim -> not held.
	if held, _ := l.Renew(ctx, "t", "s"); held {
		t.Fatal("renew of an unheld lease should report not held")
	}
	_, _ = l.Claim(ctx, "t", "s")
	if held, _ := l.Renew(ctx, "t", "s"); !held {
		t.Fatal("renew of a held lease should report held")
	}
}
