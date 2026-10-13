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
	Tenant       string  `gorm:"column:tenant;primaryKey;size:64"`
	Name         string  `gorm:"column:name;primaryKey;size:128"`
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
	// Generic grouping key (a subsession records its parent's name here).
	Group string `gorm:"column:group"`
	// List-row projection (derived from the chain, written on append).
	LastMessagePreview string  `gorm:"column:last_message_preview"`
	LastMessageAt      *string `gorm:"column:last_message_at"`
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

// Mailbox is a session's durable delivery queue. `ConsumedBy` is the CLAIM
// column that makes the atomic drain a portable two-phase operation.
type Mailbox struct {
	ID          string  `gorm:"column:id;primaryKey;size:64"`
	Tenant      string  `gorm:"column:tenant;size:64;index:idx_mb_session,priority:1"`
	SessionName string  `gorm:"column:session_name;size:128;index:idx_mb_session,priority:2"`
	MsgType     string  `gorm:"column:msg_type"`
	Source      string  `gorm:"column:source"`
	Payload     string  `gorm:"column:payload"`
	EffectiveAt *string `gorm:"column:effective_at"`
	Status      string  `gorm:"column:status"`
	CreatedAt   string  `gorm:"column:created_at"`
	ConsumedAt  *string `gorm:"column:consumed_at"`
	ConsumedBy  *string `gorm:"column:consumed_by;size:64;index"`
	Seq         *int64  `gorm:"column:seq"`
}

func (Mailbox) TableName() string { return "mailbox" }

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
