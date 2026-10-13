package agent

import (
	"strings"
	"time"
)

// RenderTemplate substitutes the built-in template variables the agent resolves
// without any extension: `{{date}}` and `{{datetime}}`. Extension variables
// (`{{ext.<id>.<name>}}`) and session-scoped variables are left as literal
// placeholders (the caller may resolve them later).
func RenderTemplate(tmpl string, now time.Time) string {
	if tmpl == "" {
		return ""
	}
	out := strings.ReplaceAll(tmpl, "{{date}}", now.UTC().Format("2006-01-02"))
	out = strings.ReplaceAll(out, "{{datetime}}", now.UTC().Format(time.RFC3339))
	return out
}
