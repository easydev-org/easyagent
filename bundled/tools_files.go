package bundled

import (
	"context"
	"fmt"
	"strings"

	"github.com/easydev-org/easyagent/agent"
)

// fileInfo returns metadata (name, mime, size, sha256) for a stored file.
func (e *Extension) fileInfo(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	code := strArg(args, "code")
	if code == "" {
		return agent.ToolOutcome{}, &toolError{msg: tr(locale, "codeRequired", nil)}
	}
	meta, _, ok := e.deps.BlobGet(tenant, code)
	if !ok {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf("file not found: file:%s", code)}
	}
	content := fmt.Sprintf("name: %s\nmime: %s\nsize: %v\nsha256: %s",
		meta["name"], meta["mime"], meta["size"], meta["sha256"])
	return agent.ToolOutcome{Content: content, Metadata: meta}, nil
}

// fileRead reads a text file's content, line-numbered, windowed by offset/limit.
func (e *Extension) fileRead(_ context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	code := strArg(args, "code")
	if code == "" {
		return agent.ToolOutcome{}, &toolError{msg: tr(locale, "codeRequired", nil)}
	}
	meta, data, ok := e.deps.BlobGet(tenant, code)
	if !ok {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf("file not found: file:%s", code)}
	}
	mime := fmt.Sprintf("%v", meta["mime"])
	if !isTextualMime(mime) {
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf("file %s is not text (mime: %s)", code, mime)}
	}
	offset, limit := 0, 200
	if f, ok := numArg(args, "offset"); ok {
		offset = int(f)
	}
	if f, ok := numArg(args, "limit"); ok && int(f) > 0 {
		limit = int(f)
	}
	if limit > 1000 {
		limit = 1000
	}
	lines := strings.Split(string(data), "\n")
	if offset > len(lines) {
		offset = len(lines)
	}
	end := offset + limit
	if end > len(lines) {
		end = len(lines)
	}
	var b strings.Builder
	for i := offset; i < end; i++ {
		fmt.Fprintf(&b, "%d\t%s\n", i+1, lines[i])
	}
	return agent.ToolOutcome{
		Content:  strings.TrimRight(b.String(), "\n"),
		Metadata: map[string]any{"code": code, "offset": offset, "lines": end - offset},
	}, nil
}
