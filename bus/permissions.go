package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/easydev-org/easyagent/store"
	easybus "github.com/easydev-org/easybus-sdk"
)

// Permission plumbing, agent side.
//
// The agent is NOT the security boundary for permissions — the extension is, because the
// extension holds the credential and performs the action. What the agent does is:
//
//   - know what each tool requires (from the discovered manifest);
//   - refuse early, with a message naming the missing capability, when the session holds
//     no grant for it at all;
//   - hand the session's grants over so the extension can decide coverage itself.
//
// The last point is what keeps this generic. "Is agentdev/mrflow within agentdev/*" is a
// question about params the extension defined; answering it here would mean this file
// knowing what a repository is.

// PermissionDeniedError is returned when a session cannot possibly run a tool.
//
// It names the CAPABILITY, not a target: at this layer the target is not known — the
// arguments have not been interpreted. The extension's own refusal is what names the
// target, and its message is the one a user should read.
type PermissionDeniedError struct {
	Tool       string
	ExtID      string
	Capability string
}

func (e *PermissionDeniedError) Error() string {
	return fmt.Sprintf(
		"permission denied: tool %q requires capability %q from extension %q, which this session does not hold; "+
			"grant it to this session first (a forked session inherits its parent's grants)",
		e.Tool, e.Capability, e.ExtID)
}

// sessionGrants returns the session's grants for one extension.
//
// A missing source, or a store error, yields NO grants rather than an error. That is the
// fail-closed direction for a permission check: without a way to look up authorisation,
// the safe answer is "not authorised". It also keeps the failure legible — the caller sees
// "permission denied", not a database error leaking out of a tool call.
func (e *ToolExecutor) sessionGrants(ctx context.Context, tenant, session, extID string) []store.GrantSnapshot {
	if e.grants == nil {
		return nil
	}
	byExt, err := e.grants(ctx, tenant, session)
	if err != nil {
		return nil
	}
	return byExt[extID]
}

// requiresOf returns the permission keys a tool needs.
//
// Accepts the qualified name the model may use (`gitea-git-mr-merge`) as well as the bare
// one, so a caller's spelling cannot sidestep the check.
func (e *ToolExecutor) requiresOf(name string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if d, ok := e.defs[name]; ok {
		return d.Requires
	}
	if i := strings.IndexByte(name, '-'); i > 0 {
		if d, ok := e.defs[name[i+1:]]; ok {
			return d.Requires
		}
	}
	return nil
}

// grantsToWire converts the session's grants into the wire form the extension receives.
//
// Params are passed through as TEXT, not parsed: their structure is the extension's
// business. An unparseable params blob is passed as an empty object rather than dropped —
// the extension's own matching will simply not cover anything with it, which is the right
// outcome and leaves the grant visible in an audit.
func grantsToWire(grants []store.GrantSnapshot) []*easybus.Grant {
	if len(grants) == 0 {
		return nil
	}
	out := make([]*easybus.Grant, 0, len(grants))
	for _, g := range grants {
		out = append(out, &easybus.Grant{
			Key:    g.Capability,
			Params: easybus.Struct(parseParams(g.Params)),
		})
	}
	return out
}

// parseParams turns the stored params text into a struct payload.
//
// The agent does not validate the shape; it only has to round-trip it faithfully so the
// extension sees exactly what was granted.
func parseParams(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	return m
}
