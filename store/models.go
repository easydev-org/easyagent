// Package store is the agent's persistence layer: GORM models + repositories
// over postgres / mysql / sqlite.
//
// INITIAL SCHEMA ONLY. There is no migration path from the TS agent's schema
// (decided): this is a fresh, independently-designed schema created once via
// AutoMigrate. Do NOT add legacy-upgrade logic.
//
// The data layer uses a two-phase (claim-column) pattern so it never depends on
// `RETURNING` (which mysql lacks).
package store

// Session is one agent session (a message chain + settings), scoped by tenant.
// Composite PK (tenant, name).
type Session struct {
	Tenant string `gorm:"column:tenant;primaryKey;size:64"`
	/**
	 * ID is the session's identity AND its address: the mailbox subject, the wake subject, its default
	 * subscription pattern and how a group addresses it all name this value.
	 *
	 * RANDOM AND IMMUTABLE, which is what lets Name be a display label. While the two were the same
	 * column, renaming a session moved it to a different subject — its queue, its subscriptions and its
	 * group slots all pointed at a name that no longer existed. Identity cannot be something a user
	 * edits.
	 */
	ID string `gorm:"column:id;primaryKey;size:64"`
	/**
	 * Name is what a person calls this session: shown in lists, in the chat header, in group member
	 * lists. It is NOT unique and NOT an address.
	 *
	 * Not unique on purpose — two sessions may be called "check the build" because the name describes
	 * the work, and forcing a suffix would make the label serve the database. Nothing routes by it, so
	 * nothing breaks when it repeats.
	 */
	Name         string  `gorm:"column:name;size:128"`
	Model        string  `gorm:"column:model"`
	Variant      string  `gorm:"column:variant"`
	Preset       string  `gorm:"column:preset"`
	TipID        *string `gorm:"column:tip_id;size:64"`
	MaxTurns     int     `gorm:"column:max_turns"`
	SystemPrompt string  `gorm:"column:system_prompt"`
	// Single-step token counters (cumulative counters were removed upstream:
	// they overflowed int32 on long-lived sessions).
	LastInputTokens  int64 `gorm:"column:last_input_tokens"`
	LastOutputTokens int64 `gorm:"column:last_output_tokens"`
	// Monotonic per-session message counter, bumped on every chain append.
	MessageSeq int64 `gorm:"column:message_seq"`
	// Shared read watermark: unread = max(0, message_seq - read_seq).
	ReadSeq    int64   `gorm:"column:read_seq"`
	CreatedAt  string  `gorm:"column:created_at"`
	UpdatedAt  string  `gorm:"column:updated_at"`
	LastUsedAt *string `gorm:"column:last_used_at"`
	Locale     string  `gorm:"column:locale"`
	// THERE IS NO COLUMN FOR "WHICH SESSION THIS ONE FORKED FROM". There used to be (`group`, holding the
	// parent's name), and it was doing two jobs badly: it recorded fork lineage, and it was read as a
	// grouping key by the console's session list. Neither survived.
	//
	//   · GROUP MEMBERSHIP is now the `groups` table, with roles and a coordinator. A single string
	//     could not express "two sessions in the same group" without also meaning "first is the parent
	//     of the second", which conflated two unrelated ideas.
	//   · FORK LINEAGE is already recorded by `messages.prev_id`: a fork shares its ancestor chain, so
	//     "what did this come from" is answerable from the transcript itself — and it stays true after
	//     a rename, which a copied name did not.
	// List-row projection (derived from the chain, written on append).
	LastMessagePreview string  `gorm:"column:last_message_preview"`
	LastMessageAt      *string `gorm:"column:last_message_at"`
	/**
	 * AvatarFile is the session's picture, as a `file:<code>` from the agent's own blob store.
	 *
	 * EMPTY MEANS "DERIVE ONE", not "no picture": the console renders a honeycomb identicon seeded by
	 * the session name, which is what every session showed before this column existed. Storing a
	 * generated image instead would make every new session cost a blob for no visible difference.
	 *
	 * A CODE, NOT A URL: the bytes already live in the blob store behind `image-generate` and the
	 * upload RPC, and a session that pointed at an external URL would break when the remote disappeared
	 * — with nothing on this side able to tell a missing picture from a broken link.
	 */
	AvatarFile string `gorm:"column:avatar_file;size:64"`
	/**
	 * Intro is the session's one-line description of WHAT IT IS — "maintainer of repo acme/app",
	 * "CI triage". It rides in the turn's <env> block, so the model knows its own role without being
	 * told in every prompt.
	 *
	 * IT IS NOT A SYSTEM PROMPT. That field REPLACES the preset's prompt wholesale when set, so a user
	 * changing their session's role would have to restate the entire instruction set. This is one line
	 * that adds to whatever prompt is already in play.
	 *
	 * EMPTY MEANS "SAY NOTHING", and the <env> block omits the section rather than emitting an empty
	 * tag — a model reading "<intro></intro>" would reasonably ask what it is supposed to be.
	 */
	Intro string `gorm:"column:intro;size:512"`
	// Why the last turn ended: "stop" | "interrupted" | "locklost" (empty = none).
	LastTurnReason string  `gorm:"column:last_turn_reason"`
	LastTurnAt     *string `gorm:"column:last_turn_at"`
}

func (Session) TableName() string { return "sessions" }

// Message is one node of a session's chain. `PrevID` is the immutable back-link
// (append-only; forks share ancestors by pointing at the same row).
type Message struct {
	ID        string  `gorm:"column:id;primaryKey;size:64"`
	Tenant    string  `gorm:"column:tenant;size:64;index"`
	Role      string  `gorm:"column:role"`
	PrevID    *string `gorm:"column:prev_id;size:64;index"`
	Source    string  `gorm:"column:source"`
	CreatedAt string  `gorm:"column:created_at"`
	/**
	 * Seq is the message's POSITION FROM ITS SESSION'S TIP: the oldest reachable message is 1, and an
	 * append adds one to the end.
	 *
	 * THE VALUE DEPENDS ON WHICH SESSION YOU ASK FROM, and that is the whole design rather than an
	 * accident of storage.
	 *
	 * A conversation is a prev_id chain and `sessions.tip_id` is a POINTER into it. A fork copies the
	 * tip, so parent and child share every message up to the fork point and then DIVERGE: from the
	 * parent, position 3 is `p3`; from the child, position 3 is `c3`. Both are correct, because "the
	 * third message of this conversation" is a fact about the conversation, not about the row.
	 *
	 * SO THERE IS NO UNIQUENESS CONSTRAINT ON (prev_id, seq). Two children of one message legitimately
	 * both occupy the next position in their own chains — that is exactly what a fork IS, and a
	 * constraint there would break `subsession-create` on its first use.
	 *
	 * IT NEVER CHANGES FOR A GIVEN SESSION, unlike depth (`len(chain) - 1 - i`), which was recomputed
	 * from the chain length on every read and shifted for every message each time one was appended.
	 *
	 * AN UNDO moves the tip back and leaves the rows: the abandoned messages keep their positions and
	 * simply stop being reachable from that session, which is what an undo means.
	 */
	Seq int64 `gorm:"column:seq;index:idx_messages_tenant_seq,priority:2"`
}

func (Message) TableName() string { return "messages" }

// Part is one body piece of a message (text/reasoning/tool/tool_result/file/…).
type Part struct {
	ID        string `gorm:"column:id;primaryKey;size:64"`
	Tenant    string `gorm:"column:tenant;size:64"`
	MessageID string `gorm:"column:message_id;size:64;index:idx_parts_message,priority:1"`
	Type      string `gorm:"column:type"`
	Seq       int    `gorm:"column:seq;index:idx_parts_message,priority:2"`
	Data      string `gorm:"column:data"`
}

func (Part) TableName() string { return "parts" }

// Provider is a registered model provider. A provider serves EXACTLY ONE
// capability (modality); a host serving several registers one row per modality.
type Provider struct {
	Tenant     string `gorm:"column:tenant;primaryKey;size:64"`
	ProviderID string `gorm:"column:provider_id;primaryKey;size:128"`
	Capability string `gorm:"column:capability"`
	APIType    string `gorm:"column:api_type"`
	BaseURL    string `gorm:"column:base_url"`
	APIKey     string `gorm:"column:api_key"`
	Headers    string `gorm:"column:headers"`
	Models     string `gorm:"column:models"`
	CreatedAt  string `gorm:"column:created_at"`
	UpdatedAt  string `gorm:"column:updated_at"`
}

func (Provider) TableName() string { return "providers" }

// Tenant is one isolation domain.
type Tenant struct {
	ID       string `gorm:"column:id;primaryKey;size:64"`
	Name     string `gorm:"column:name"`
	Disabled bool   `gorm:"column:disabled"`
	// ParentID is the delegating parent tenant (NULL = a platform root).
	ParentID *string `gorm:"column:parent_id;size:64;index"`
	// ScopeID is the delegation-subtree root this tenant belongs to (self when
	// the tenant is a root). Denormalized so listing a subtree is one indexed
	// query with no recursive walk.
	ScopeID   string `gorm:"column:scope_id;size:64;index"`
	CreatedAt string `gorm:"column:created_at"`
	UpdatedAt string `gorm:"column:updated_at"`
}

func (Tenant) TableName() string { return "tenants" }

// AdminToken is a DELEGATED administrator credential. It carries a scope: the
// subtree root it may administer. The static platform admin token is NOT stored
// here (it is config); this table holds only delegated (sub)admins.
type AdminToken struct {
	TokenID     string  `gorm:"column:token_id;primaryKey;size:64"`
	ScopeTenant string  `gorm:"column:scope_tenant_id;size:64;index"`
	TokenSHA256 string  `gorm:"column:token_sha256;size:64;uniqueIndex"`
	Label       string  `gorm:"column:label"`
	CreatedAt   string  `gorm:"column:created_at"`
	LastUsedAt  *string `gorm:"column:last_used_at"`
	RevokedAt   *string `gorm:"column:revoked_at"`
}

func (AdminToken) TableName() string { return "admin_tokens" }

// SessionToken is the credential an API call actually presents.
//
// Login exchanges a PAIR (username + secret) for one of these. Why a second
// layer exists:
//
//   - The PAIR is what a human types, and its secret may be a short, guessable
//     label. Presenting it on every RPC would mean the guessable string travels
//     constantly AND that two tenants choosing the same label collide in the
//     lookup.
//   - This token is globally unique and random, so the per-RPC middleware can
//     resolve a caller from it alone (no username, no ambiguity).
//
// Only the sha256 of the random token is stored; the plaintext is returned once
// by Login and kept by the client.
type SessionToken struct {
	TokenID     string `gorm:"column:token_id;primaryKey;size:64"`
	TokenSHA256 string `gorm:"column:token_sha256;size:64;uniqueIndex"`
	// Role is "tenant" or "admin" — the identity the pair resolved to.
	Role string `gorm:"column:role;size:16"`
	// TenantID is the tenant the caller acts as ("" for a platform admin).
	TenantID string `gorm:"column:tenant_id;size:64;index"`
	// Scope is the admin scope ("*" or a subtree root); "" for a tenant.
	Scope string `gorm:"column:scope;size:64"`
	// Label is a human note (e.g. the username that logged in), never a secret.
	Label      string  `gorm:"column:label"`
	CreatedAt  string  `gorm:"column:created_at"`
	LastUsedAt *string `gorm:"column:last_used_at"`
}

func (SessionToken) TableName() string { return "session_tokens" }

// TenantToken is a bearer credential minted for one tenant. Only its sha256 is
// stored (a DB compromise yields no usable credential).
type TenantToken struct {
	TokenID     string  `gorm:"column:token_id;primaryKey;size:64"`
	TenantID    string  `gorm:"column:tenant_id;size:64;index"`
	TokenSHA256 string  `gorm:"column:token_sha256;size:64;uniqueIndex"`
	Label       string  `gorm:"column:label"`
	CreatedAt   string  `gorm:"column:created_at"`
	LastUsedAt  *string `gorm:"column:last_used_at"`
	RevokedAt   *string `gorm:"column:revoked_at"`
}

func (TenantToken) TableName() string { return "tenant_tokens" }

// AgentFile is a stored file's metadata + per-tenant sha256 dedup index.
type AgentFile struct {
	Code            string  `gorm:"column:code;primaryKey;size:64"`
	Tenant          string  `gorm:"column:tenant;size:64;uniqueIndex:uq_agent_files_tenant_sha,priority:1"`
	SHA256          string  `gorm:"column:sha256;size:64;uniqueIndex:uq_agent_files_tenant_sha,priority:2"`
	Name            string  `gorm:"column:name"`
	Mime            string  `gorm:"column:mime"`
	Size            int64   `gorm:"column:size"`
	UploaderSession string  `gorm:"column:uploader_session"`
	CreatedAt       string  `gorm:"column:created_at"`
	Width           *int    `gorm:"column:width"`
	Height          *int    `gorm:"column:height"`
	DurationMS      *int64  `gorm:"column:duration_ms"`
	ThumbCode       *string `gorm:"column:thumb_code"`
	Thumbhash       *string `gorm:"column:thumbhash"`
}

func (AgentFile) TableName() string { return "agent_files" }

// BundledTodo is one row of a session's todo list (written by todo-write).
type BundledTodo struct {
	ID        int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Tenant    string `gorm:"column:tenant;size:64;index:idx_bundled_todos_session,priority:1"`
	SessionID string `gorm:"column:session_id;size:128;index:idx_bundled_todos_session,priority:2"`
	Content   string `gorm:"column:content"`
	Status    string `gorm:"column:status"`
	Priority  string `gorm:"column:priority"`
	CreatedAt int64  `gorm:"column:created_unix"`
}

func (BundledTodo) TableName() string { return "bundled_todos" }
