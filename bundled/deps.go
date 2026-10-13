// Package bundled is the Go rewrite of the TS `@abc-protocol/bundled-extension`.
// It runs IN-PROCESS inside agent-go (no NATS extension server): it implements
// the agent's ToolExecutor + ToolRegistry seams directly, with the host
// capabilities the TS extension got via its injected `BundledDeps`.
package bundled

import (
	"github.com/easydev-org/easyagent/llm"
)

// Deps are the host capabilities the bundled tools need. agent-go supplies an
// implementation backed by the store + bus + blob + provider registry.
type Deps interface {
	// --- locale ---
	// SessionVariable reads a session-scoped variable the agent projects
	// (provider "agent", name "locale"). Returns ("", false) when unset.
	SessionVariable(tenant, session, name string) (string, bool)
	// ResolveConfig resolves an effective config knob (session > global).
	ResolveConfig(name, session, tenant string) string

	// --- generation (provider registry) ---
	// ResolveTextModel resolves a `provider/model` ref to a v4 client + model id.
	ResolveTextModel(tenant, ref string) (*llm.Client, string, error)
	// ResolveGenerative resolves a capability-specific model ref (image / video
	// / speech / transcription) to a v4 client + model id. It errors when the
	// provider is missing or serves a different capability.
	ResolveGenerative(capability, ref, tenant string) (*llm.Client, string, error)

	// --- files / blobs ---
	// BlobGet returns a stored file's metadata + bytes (tenant-scoped).
	BlobGet(tenant, code string) (meta map[string]any, data []byte, ok bool)
	// IngestBlob stores generated media bytes, returning the canonical code +
	// the agent-derived mime + display name.
	IngestBlob(tenant, session, name string, data []byte) (code, mime, displayName string, err error)

	// --- sessions ---
	SessionGroup(tenant, sid string) string
	SessionExists(tenant, sid string) bool
	SessionTip(tenant, sid string) string
	SessionsInGroup(tenant, group string) []string
	ForkSession(tenant, parent, child string) error
	DeleteSessionRow(tenant, sid string) error
	DeleteSessionMailbox(tenant, sid string) error
	PublishMailbox(tenant, session, typ string, payload any, source string) error

	// --- history ---
	// MessageChain walks a chain from tip, oldest-first with depth 0 at the tip.
	// limit <= 0 means NO cap (the whole chain).
	MessageChain(tenant, tip string, limit int) []ChainEntry
	// MessageParts returns the ordered parts for the given message ids.
	MessageParts(tenant string, ids []string) []MessagePartRow

	// --- todos ---
	TodosReplace(tenant, sid string, rows []TodoRow) error
}

// ChainEntry is one message in a chain walk.
type ChainEntry struct {
	ID        string
	Role      string
	CreatedAt string
	Depth     int
	Source    string
}

// MessagePartRow is one persisted message part.
type MessagePartRow struct {
	MessageID string
	Type      string
	Seq       int
	Data      string
}

// TodoRow is one todo item.
type TodoRow struct {
	Content  string
	Status   string
	Priority string
}
