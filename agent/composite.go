package agent

import (
	"context"
	"errors"
	"fmt"
)

// ErrUnknownTool marks a tool not owned by a given executor, so a composite can
// fall through to the next one. Executors should wrap it: fmt.Errorf("%w: …").
var ErrUnknownTool = errors.New("unknown tool")

// IsUnknownTool reports whether err means "this executor does not own the tool".
func IsUnknownTool(err error) bool { return errors.Is(err, ErrUnknownTool) }

// CompositeTools merges several tool sources behind the ToolRegistry +
// ToolExecutor seams: its tool list is the UNION, and a call is routed to the
// executor that owns the name. This is how the in-process bundled extension and
// the out-of-process NATS extensions coexist.
type CompositeTools struct {
	registries []ToolRegistry
	executors  []ToolExecutor
}

// NewCompositeTools builds a composite from ordered sources.
func NewCompositeTools(registries []ToolRegistry, executors []ToolExecutor) *CompositeTools {
	return &CompositeTools{registries: registries, executors: executors}
}

// Tools returns the union of every registry's tools (deduped by name; the first
// source wins a collision).
func (c *CompositeTools) Tools() []ToolDef {
	seen := map[string]bool{}
	var out []ToolDef
	for _, r := range c.registries {
		for _, t := range r.Tools() {
			name := QualifiedName(t)
			if seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, t)
		}
	}
	return out
}

// Execute routes a call to the owning executor (by name), falling through on an
// unknown-tool error.
func (c *CompositeTools) Execute(ctx context.Context, tenant, session, name, args string) (ToolOutcome, error) {
	owner := c.ownerOf(name)
	if owner != nil {
		return owner.Execute(ctx, tenant, session, name, args)
	}
	// No registry match (dynamic source): try each executor in order.
	var lastErr error
	for _, e := range c.executors {
		out, err := e.Execute(ctx, tenant, session, name, args)
		if err == nil || !IsUnknownTool(err) {
			return out, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: %s", ErrUnknownTool, name)
	}
	return ToolOutcome{}, lastErr
}

// ownerOf finds the executor whose registry declares name (bare or qualified).
func (c *CompositeTools) ownerOf(name string) ToolExecutor {
	for i, r := range c.registries {
		if i >= len(c.executors) {
			break
		}
		for _, t := range r.Tools() {
			if t.Name == name || QualifiedName(t) == name {
				return c.executors[i]
			}
		}
	}
	return nil
}

var _ ToolRegistry = (*CompositeTools)(nil)
var _ ToolExecutor = (*CompositeTools)(nil)
