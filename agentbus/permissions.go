package agentbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	agentpkg "github.com/easydev-org/easyagent/agent"
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
	d, ok := e.defOf(name)
	if !ok {
		return nil
	}
	return d.Requires
}

/**
 * defOf resolves a model-emitted name to its definition, accepting both spellings.
 *
 * THE QUALIFIED FORM IS SPLIT AT THE LAST '-' and matched against the known pairs, exactly as Execute resolves
 * names: a check that resolved differently from the call would guard a tool nobody invokes.
 */
func (e *ToolExecutor) defOf(name string) (agentpkg.ToolDef, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if d, ok := e.defs[name]; ok {
		return d, true
	}
	if i := strings.IndexByte(name, '-'); i > 0 {
		if d, ok := e.defs[name[i+1:]]; ok {
			return d, true
		}
	}
	return agentpkg.ToolDef{}, false
}

/**
 * missingCapability answers "what does this call lack" for one tool, given its arguments.
 *
 * TWO CHECKS, IN THIS ORDER:
 *
 *	1. does the session hold the capability AT ALL — unscoped or for any scope;
 *	2. if the tool names an argument that scopes it, does it hold that capability FOR THAT OBJECT.
 *
 * THE FIRST IS NOT REDUNDANT. It is what lets the refusal distinguish "you were never granted this" from "you were
 * granted it somewhere else", and those are different instructions to the person who can fix it: grant the
 * capability, versus grant it for the group this call names.
 *
 * THE OBJECT COMES FROM THE ARGUMENTS, read verbatim. `group` holds a group id, and the grant records `group:<id>`.
 * The prefix lives in the store, so a second scoped capability is a second case here rather than a second gate.
 */
func (e *ToolExecutor) missingCapability(def agentpkg.ToolDef, grants []store.GrantSnapshot, input map[string]any) (capability string, ok bool) {
	if len(def.Requires) == 0 {
		return "", true
	}
	if missing, held := store.HasAnyCapability(grants, def.Requires); !held {
		return missing, false
	}
	if def.RequiresScope == "" {
		return "", true
	}
	raw, _ := input[def.RequiresScope].(string)
	scope := strings.TrimSpace(raw)
	if scope == "" {
		// A CALL WITHOUT THE OBJECT IS LEFT TO THE TOOL. The gate cannot say which group was meant, and the tool
		// knows how to refuse a missing argument with a better message than "permission denied" — which would send
		// the reader looking for a grant they already hold.
		return "", true
	}
	for _, capability := range def.Requires {
		if !store.HasCapabilityInScope(grants, capability, store.GroupScope(scope)) {
			return capability, false
		}
	}
	return "", true
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
