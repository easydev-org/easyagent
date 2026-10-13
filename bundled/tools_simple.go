package bundled

import (
	"context"

	"github.com/easydev-org/easyagent/agent"
)

// localeOf resolves the session locale (session variable → tenant config → en).
func (e *Extension) localeOf(tenant, session string) string {
	if v, ok := e.deps.SessionVariable(tenant, session, "locale"); ok && v != "" {
		return v
	}
	if v := e.deps.ResolveConfig("locale", session, tenant); v != "" {
		return v
	}
	return "en"
}

// todoWrite replaces the session's todo list (stored by the agent, not a file).
func (e *Extension) todoWrite(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	raw, _ := args["todos"].([]any)
	rows := make([]TodoRow, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		rows = append(rows, TodoRow{
			Content: strArg(m, "content"), Status: strArg(m, "status"), Priority: strArg(m, "priority"),
		})
	}
	if err := e.deps.TodosReplace(tenant, session, rows); err != nil {
		return agent.ToolOutcome{}, err
	}
	return agent.ToolOutcome{
		Content:  tr(locale, "todosWritten", map[string]any{"n": len(rows)}),
		Metadata: map[string]any{"todos": rows},
	}, nil
}

// toolError is a tool that ran but reported an error (mapped to tool-error).
type toolError struct{ msg string }

func (e *toolError) Error() string { return e.msg }
