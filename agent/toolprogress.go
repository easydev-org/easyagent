package agent

import (
	"context"
	"time"

	easybus "github.com/easydev-org/easybus-sdk"
	abcagent "github.com/easydev-org/easybus-sdk/agent"
	"github.com/easydev-org/easybus-sdk/protocol"
)

// ToolProgressSource opens a stream of in-flight updates for one tool call.
//
// The cancel func must be callable multiple times and must release the
// subscription.
type ToolProgressSource func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error)

// NewToolProgressSource builds the bridge from the bus to the turn loop.
//
// Two things are deliberately NOT done here:
//
//   - Nothing is written to `messages`. Progress is a view of work in flight; the
//     model's context only ever receives the tool's final result. Keeping these
//     paths apart is what lets a job print thousands of lines for free.
//   - Nothing is persisted by this function. The frames become durable because the
//     turn loop re-emits them as ordinary session events (see relayProgress),
//     which is what lets a client that reconnects still see everything.
func NewToolProgressSource(a *abcagent.Agent) ToolProgressSource {
	return func(ctx context.Context, tenant, callID string) (<-chan ToolProgressUpdate, func(), error) {
		// SubscribeProgress is core NATS pub/sub on
		// easybus.<tenant>.tool.progress.<callID>: one hop from the extension to
		// here. It is deliberately not a JetStream subject — the durability the UI
		// needs is provided downstream, when the turn re-emits each frame onto the
		// session's event stream.
		sub, err := a.SubscribeProgress(ctx, tenant, callID)
		if err != nil {
			return nil, func() {}, err
		}

		out := make(chan ToolProgressUpdate, 64)
		go func() {
			defer close(out)
			for {
				env, ok := sub.Next(ctx)
				if !ok {
					return
				}
				var p easybus.ToolProgress
				if env == nil || !protocol.Coerce(env.Payload, &p) {
					// Unreadable frame: drop it rather than tearing down the relay.
					continue
				}
				upd := ToolProgressUpdate{CallID: p.GetCallId(), Phase: p.GetPhase(), Text: p.GetText()}
				if p.Progress != nil {
					upd.Progress = float64(*p.Progress)
				}
				if p.Metadata != nil {
					upd.Metadata = p.Metadata.AsInterface()
				}
				select {
				case out <- upd:
				case <-ctx.Done():
					return
				case <-time.After(30 * time.Second):
					// If the reader has been gone for this long, stop holding the
					// goroutine: progress is disposable, a leak is not.
					return
				}
			}
		}()
		return out, func() { sub.Close() }, nil
	}
}
