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
	SessionExists(tenant, sid string) bool
	SessionTip(tenant, sid string) string
	/**
	 * ForkSession copies a session, optionally withholding capabilities from the copy.
	 *
	 * `exclude` holds `extID/capability` pairs. THE EXCLUSION IS APPLIED DURING THE COPY rather than by
	 * revoking afterwards: a revoking loop leaves a window where the fork holds the grant, and a tool
	 * call in that window would be authorised by a permission the user never intended the fork to have.
	 *
	 * `name` is the DISPLAY label only. THE RETURNED STRING IS THE NEW SESSION'S ID, because that is
	 * what every later call needs — the name identifies nothing.
	 */
	ForkSession(tenant, parent, name string, exclude []string) (string, error)
	DeleteSessionRow(tenant, sid string) error

	// --- history ---
	// MessageChain walks a chain from tip, OLDEST FIRST, each entry carrying its position (`Seq`).
	// limit <= 0 means NO cap (the whole chain); a positive limit keeps the NEWEST `limit` messages.
	MessageChain(tenant, tip string, limit int) []ChainEntry
	// MessageParts returns the ordered parts for the given message ids.
	MessageParts(tenant string, ids []string) []MessagePartRow

	// --- groups ---
	//
	// READ ONLY. A group is created and edited by a person through the console; the bundled tools can
	// only ask what a session is part of and who else is in it. There is deliberately no
	// CreateGroup/AddGroupMember here — an extension that could assemble a set of sessions would be
	// choosing its own collaborators.
	// GroupsOfSession returns the groups a session is in. It reports an error rather than an empty slice:
	// "you are in no groups" and "the lookup failed" are different answers, and a tool that says the first
	// when the second happened tells the model its group does not exist.
	GroupsOfSession(tenant, session string) ([]GroupRow, error)
	IsGroupMember(tenant, group, session string) bool
	/**
	 * GroupMemberProfiles returns a group's members as READERS see them: id, display name, one-line intro and
	 * role.
	 *
	 * IT REPLACED A BARE []string OF IDS. A coordinator could see the members but not tell which one it was,
	 * what each of them does, or who was answerable for the group — and it said so, in its answer to a live
	 * group. The name and the intro exist so a peer can decide whom to address and with what.
	 */
	GroupMemberProfiles(tenant, group string) ([]GroupMemberRow, error)
	GroupCoordinator(tenant, group string) (string, error)
	// CoordinatedGroup returns the group this session coordinates ("" when none). At most one, by the
	// store's constraint.
	CoordinatedGroup(tenant, session string) (string, error)
	// AddGroupMemberOnly enrolls a session as a plain member (never coordinator).
	AddGroupMemberOnly(tenant, group, session string) error
	/**
	 * CreateGroupForSession makes `group` with `coordinator` as its coordinator.
	 *
	 * THE ONLY group-creating capability an extension has, and it is narrow on purpose: it cannot add
	 * anyone beyond the two sessions named, and it cannot touch a group it did not create. Permission
	 * `group.create` gates it further — without the grant the call never reaches this method.
	 */
	CreateGroupForSession(tenant, group, coordinator string) error
	/**
	 * PostGroupMessage STORES a message in the group's transcript and then delivers it to the named members.
	 *
	 * STORE FIRST, THEN DELIVER. A recipient that receives a message can always find it in the transcript;
	 * the reverse order would let a message be heard and never recorded, which is the failure that cannot be
	 * repaired afterwards.
	 *
	 * DELIVERY STILL GOES THROUGH THE GROUP'S CHANNEL, so the subscriber's pattern decides who hears it and
	 * a session outside the group cannot receive it whoever calls this.
	 */
	PostGroupMessage(tenant, group, from string, to []string, text string) error

	// --- todos ---
	TodosReplace(tenant, sid string, rows []TodoRow) error
}

/**
 * GroupRoleCoordinator is the role value the store records for a group's coordinator.
 *
 * THE TOOL AND THE STORE MUST AGREE ON THE SPELLING, and a bare string literal in the renderer would be a
 * second copy that drifts the moment the store renames one. `GroupMemberRow.Role` carries the store's value
 * verbatim, so the comparison is against this one constant.
 */
const GroupRoleCoordinator = "coordinator"

/**
 * GroupMemberRow is one member as the tools see it.
 *
 * NAME AND INTRO MAY BE EMPTY: a session is not required to have either, and a renderer that dropped the row
 * for lack of a label would hide a member other sessions are told exists. The ID is always present, and it is
 * what an addressed message must use.
 */
type GroupMemberRow struct {
	Session string
	Name    string
	Intro   string
	// Role is "coordinator" or "member" — a routing fact, not a permission tier.
	Role string
}

// GroupRow is one group as the tools see it: a name and how many sessions are in it.
type GroupRow struct {
	Name    string
	Members int
}

// ChainEntry is one message in a chain walk.
type ChainEntry struct {
	ID        string
	Role      string
	CreatedAt string
	// Seq is the message's POSITION IN THE CHAIN: oldest is 1 and it never changes.
	//
	// THERE IS NO "DEPTH" HERE ANY MORE. It counted back from the tip and was recomputed from the
	// chain's LENGTH, so appending one message shifted every depth in the conversation — a number
	// that looks like an address and is not one. `Seq` is that address, and `Chain(limit)` still
	// answers "the newest N" for the one question depth was actually good at.
	Seq    int64
	Source string
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
