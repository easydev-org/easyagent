package bus

import (
	"testing"

	"github.com/easydev-org/easybus-sdk/protocol"
)

// The KV key shape must survive the subscription id and session name it is built from.
//
// Written because a real deployment held correct subscriptions in the bucket that never matched
// anything. The id is derived from the pattern, and a pattern contains dots ("tag.acme/app.>"),
// which the key escapes — so the parser has to undo exactly that, and a parse that quietly
// returns the wrong session puts the subscription under a session nobody publishes to.
func TestParseSubscriptionKeyRoundTrips(t *testing.T) {
	cases := []struct {
		key     string
		tenant  string
		session string
		id      string
	}{
		// Plain session, escaped id (the id comes from the pattern, which has dots and ">").
		{
			key:     "t.alice.s.tag-watcher.sub-gitea-tag=2Eagentdev/mrflow=2E=3E",
			tenant:  "alice",
			session: "tag-watcher",
			id:      "sub-gitea-tag=2Eagentdev/mrflow=2E=3E",
		},
		// No escaping needed.
		{
			key:     "t.alice.s.chan-probe.sub-gitea",
			tenant:  "alice",
			session: "chan-probe",
			id:      "sub-gitea",
		},
		{
			key:     "t.alice.s.gh-e2e.system-default",
			tenant:  "alice",
			session: "gh-e2e",
			id:      "system-default",
		},
		// A session name that itself needed escaping must come back unescaped.
		{
			key:     "t.alice.s.ch-sub.sub-github-release=2Eeasydev-org%2Ftest=2E=3E",
			tenant:  "alice",
			session: "ch-sub",
			id:      "sub-github-release=2Eeasydev-org%2Ftest=2E=3E",
		},
	}

	for _, c := range cases {
		tenant, session, id, ok := parseSubscriptionKey(c.key)
		if !ok {
			t.Errorf("parseSubscriptionKey(%q) failed", c.key)
			continue
		}
		if tenant != c.tenant {
			t.Errorf("%q: tenant = %q, want %q", c.key, tenant, c.tenant)
		}
		if session != c.session {
			t.Errorf("%q: session = %q, want %q", c.key, session, c.session)
		}
		if id != c.id {
			t.Errorf("%q: id = %q, want %q", c.key, id, c.id)
		}
	}
}

// The key built by the store must parse back to the same three parts.
func TestSubscriptionKVKeyRoundTripsThroughParser(t *testing.T) {
	const (
		tenant  = "alice"
		session = "tag-watcher"
		id      = "sub-gitea-tag=2Eagentdev/mrflow=2E=3E"
	)
	key := protocol.SubscriptionKVKey(tenant, session, id)
	gotTenant, gotSession, gotID, ok := parseSubscriptionKey(key)
	if !ok {
		t.Fatalf("key %q did not parse", key)
	}
	if gotTenant != tenant || gotSession != session || gotID != id {
		t.Fatalf("round trip: (%q,%q,%q), want (%q,%q,%q)",
			gotTenant, gotSession, gotID, tenant, session, id)
	}
}
