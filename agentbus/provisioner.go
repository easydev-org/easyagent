package agentbus

import "context"

// DefaultChannelProvisioner writes each session's built-in subscription.
//
// Lives in the bus package because that is where the KV bucket is, and satisfies
// server.ChannelProvisioner so the server never has to know how subscriptions are stored.
type DefaultChannelProvisioner struct {
	kv kvPutter
}

// NewDefaultChannelProvisioner returns a provisioner over the given bus.
func NewDefaultChannelProvisioner(kv kvPutter) *DefaultChannelProvisioner {
	return &DefaultChannelProvisioner{kv: kv}
}

// EnsureDefault writes (or refreshes) the session's default subscription.
func (p *DefaultChannelProvisioner) EnsureDefault(ctx context.Context, tenant, session string) error {
	if p == nil || p.kv == nil {
		return nil
	}
	return EnsureDefaultSubscription(ctx, p.kv, tenant, session)
}

/**
 * SyncGroup brings every member's subscription in line with the member list.
 *
 * A FULL REWRITE, not a delta: the member list is the source of truth, and a rewrite is idempotent
 * whatever the process's history. `previous` exists only so a departure can be UNSUBSCRIBED — a stale
 * pattern that no longer has a member behind it would keep delivering to a session that left.
 */
func (p *DefaultChannelProvisioner) SyncGroup(ctx context.Context, tenant, group string, members map[string]bool, previous []string) error {
	if p == nil || p.kv == nil {
		return nil
	}
	return RewriteGroupSubscriptions(ctx, p.kv, tenant, group, members, previous)
}

// DropGroup removes every member's subscription to a group that no longer exists.
func (p *DefaultChannelProvisioner) DropGroup(ctx context.Context, tenant, group string, members []string) error {
	if p == nil || p.kv == nil {
		return nil
	}
	var firstErr error
	for _, m := range members {
		if err := RemoveGroupSubscription(ctx, p.kv, tenant, group, m); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
