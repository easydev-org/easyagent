package bus

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
