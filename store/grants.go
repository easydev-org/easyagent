package store

import (
	"errors"
	"sort"
	"time"

	"gorm.io/gorm"
)

// SessionGrant is one authorisation held by a session.
//
// A permission is a (key, params) pair, not a bare name: "merge pull requests in THIS
// repository" is a different authorisation from "merge pull requests in that one", so a
// session may hold several rows with the same Key and different Params.
//
// Params is stored as TEXT and never parsed here. Its shape is whatever the extension
// declared in its manifest; reading it would mean teaching the store what a "repo" is,
// which is exactly the coupling this design avoids. The extension receives the text,
// parses it against its own declaration, and decides whether it covers the call.
type SessionGrant struct {
	Tenant  string `gorm:"column:tenant;primaryKey;size:64"`
	Session string `gorm:"column:session;primaryKey;size:128"`
	// ID is a stable handle for revoking ONE instance. Without it, revoking "mr.merge on
	// agentdev/mrflow" would have to match on params, and a params edit would orphan the
	// grant the user meant to remove.
	ID string `gorm:"column:id;primaryKey;size:64"`

	ExtID string `gorm:"column:ext_id;size:64;index"`
	// Capability is the permission key the extension declared ("mr.merge").
	Capability string `gorm:"column:capability;size:128;index"`
	// Params is opaque JSON text. Empty for a boolean permission.
	Params string `gorm:"column:params;type:text"`
	/**
	 * Scope NARROWS a grant to one object, when the capability is about a particular one.
	 *
	 * EMPTY MEANS "EVERYWHERE", which is what almost every grant is: "may merge in this repository" is already
	 * narrowed by Params, and "may read files" is not about an object at all.
	 *
	 * THE GROUP CAPABILITIES ARE THE CASE THIS EXISTS FOR. `group.spawn` means "may put sessions to work" — and the
	 * question a gate actually has to answer is "may it do that IN THIS GROUP". A session that may spawn inside the
	 * group a person created for it is not thereby allowed to spawn inside a group it created for itself, and the
	 * only way to say that is to record which group the permission was given for.
	 *
	 * `group:<id>` is the shape. It is OPAQUE here, like Params: this field records a string the caller chose, and
	 * the caller that reads it is the one that knows the prefix.
	 */
	Scope string `gorm:"column:scope;size:128;index"`
	// Risk is copied from the declaration so a client can render a list without
	// fetching every manifest: read | write | destructive.
	Risk string `gorm:"column:risk;size:32"`

	GrantedAt string `gorm:"column:granted_at"`
	// GrantedBy records provenance: "user", "auto:fork:<parent>", "auto:preset:<name>".
	// A user who sees a permission they did not add needs to know where it came from.
	GrantedBy string `gorm:"column:granted_by;size:128"`
	// RevokedAt is a soft delete: revoking keeps the audit trail, and a later question
	// ("who could merge last week") is still answerable.
	RevokedAt *string `gorm:"column:revoked_at"`
}

func (SessionGrant) TableName() string { return "session_grants" }

// ErrGrantNotFound is returned when revoking a grant that does not exist or is already
// revoked.
var ErrGrantNotFound = errors.New("grant not found")

// newGrantID mints an opaque id for one grant.
func newGrantID() string {
	return "gr-" + newID()
}

// AddGrant records one authorisation and returns it.
//
// Not deduplicated: granting the same (key, params) twice creates two rows, which is
// intentional — the second may have a different GrantedBy (one inherited, one given), and
// revoking one must not revoke the other.
func (d *DB) AddGrant(tenant, session string, g SessionGrant) (SessionGrant, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if g.Risk == "" {
		g.Risk = "write"
	}
	row := SessionGrant{
		Tenant: tenant, Session: session, ID: newGrantID(),
		ExtID: g.ExtID, Capability: g.Capability, Params: g.Params, Risk: g.Risk,
		Scope: g.Scope, GrantedAt: now, GrantedBy: g.GrantedBy,
	}
	if row.GrantedBy == "" {
		row.GrantedBy = "user"
	}
	if err := d.g.Create(&row).Error; err != nil {
		return SessionGrant{}, err
	}
	return row, nil
}

/**
 * EnsureGrant records one authorisation unless an identical live one already exists.
 *
 * DIFFERENT FROM AddGrant, WHICH ALWAYS INSERTS. Two grants for the same (capability, scope) are not two
 * permissions — unlike Params, where the same capability on two repositories is genuinely two authorisations. A
 * duplicate row here would show the user the same permission twice and make a revoke look half-done.
 *
 * IT MATTERS BECAUSE THE CALLERS ARE RETRIED PATHS: session creation, group creation and fork all re-run on a
 * client retry, and the grant they write is derived from something that has not changed.
 *
 * Returns (grant, created, error); `created` is false when an identical live grant was already there.
 */
func (d *DB) EnsureGrant(tenant, session string, g SessionGrant) (SessionGrant, bool, error) {
	var existing SessionGrant
	err := d.g.Where(
		"tenant = ? AND session = ? AND ext_id = ? AND capability = ? AND scope = ? AND revoked_at IS NULL",
		tenant, session, g.ExtID, g.Capability, g.Scope,
	).First(&existing).Error
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return SessionGrant{}, false, err
	}
	row, err := d.AddGrant(tenant, session, g)
	if err != nil {
		return SessionGrant{}, false, err
	}
	return row, true, nil
}

// ListGrants returns a session's live grants, ordered for display.
func (d *DB) ListGrants(tenant, session string) ([]SessionGrant, error) {
	var rows []SessionGrant
	if err := d.g.
		Where("tenant = ? AND session = ? AND revoked_at IS NULL", tenant, session).
		Order("ext_id, capability, granted_at").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// RevokeGrant soft-deletes one grant by id.
//
// Idempotent-ish: revoking an already-revoked grant reports ErrGrantNotFound rather than
// silently succeeding, so a client can tell "I revoked it" from "there was nothing there".
func (d *DB) RevokeGrant(tenant, session, grantID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	res := d.g.Model(&SessionGrant{}).
		Where("tenant = ? AND session = ? AND id = ? AND revoked_at IS NULL", tenant, session, grantID).
		Update("revoked_at", now)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrGrantNotFound
	}
	return nil
}

// CopyGrantsFrom duplicates every live grant of one session onto another.
//
// Used by fork. The copies carry GrantedBy = "auto:fork:<parent>" so a user looking at a
// forked session can see which permissions they inherited rather than granted.
//
// The copies are INDEPENDENT rows: revoking one in the child must not affect the parent.
func (d *DB) CopyGrantsFrom(tenant, from, to string) (int, error) {
	return d.CopyGrantsFromExcept(tenant, from, to, nil, false)
}

/**
 * CopyGrantsFromExcept duplicates a session's live grants onto another, minus some capabilities.
 *
 * TWO QUESTIONS, TWO ARGUMENTS. "Should a fork inherit permissions at all" and "which ones must it not"
 * are decided by different callers for different reasons: the API lets a user choose the first, and a
 * tool like `spawn` needs the second to stop a worker from recruiting. Folding both into one flag would
 * force the caller that only wants an exception to also state a policy it has no opinion about.
 *
 * `except` is keyed by `ExtID + "/" + Capability`. The pair matters: two extensions may both declare a
 * `merge`, and excluding one must not exclude the other.
 */
func (d *DB) CopyGrantsFromExcept(tenant, from, to string, except map[string]bool, none bool) (int, error) {
	if none {
		// INHERIT_NONE is answered without reading the parent: copying nothing is the whole operation,
		// and a caller that asked for it does not care which grants existed.
		return 0, nil
	}
	rows, err := d.ListGrants(tenant, from)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	out := make([]SessionGrant, 0, len(rows))
	for _, r := range rows {
		if except[GrantKey(r.ExtID, r.Capability)] {
			continue
		}
		out = append(out, SessionGrant{
			Tenant: tenant, Session: to, ID: newGrantID(),
			ExtID: r.ExtID, Capability: r.Capability, Params: r.Params, Risk: r.Risk,
			// THE SCOPE TRAVELS WITH THE GRANT. A fork that inherited "may spawn in group X" but not the scope would
			// hold the capability for EVERY group — a wider permission than the parent had, granted by an operation
			// nobody described as an escalation.
			Scope:     r.Scope,
			GrantedAt: now, GrantedBy: "auto:fork:" + from,
		})
	}
	if len(out) == 0 {
		return 0, nil
	}
	if err := d.g.Create(&out).Error; err != nil {
		return 0, err
	}
	return len(out), nil
}

// GrantKey identifies one capability within one extension, for exclusion sets.
func GrantKey(extID, capability string) string { return extID + "/" + capability }

// GrantSnapshot is one extension's grants, ready to hand to a tool call.
type GrantSnapshot struct {
	ExtID      string
	Capability string
	Params     string
	// Scope is the object a grant was narrowed to, or "" for everywhere. See SessionGrant.Scope.
	Scope string
}

// GrantsByExt groups a session's live grants by extension.
//
// The agent uses this to decide, per call: does this session hold ANY grant for the keys
// the tool requires? It deliberately does not look inside Params — that decision belongs
// to the extension, which is the only side that knows what a param means.
func (d *DB) GrantsByExt(tenant, session string) (map[string][]GrantSnapshot, error) {
	rows, err := d.ListGrants(tenant, session)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]GrantSnapshot, 4)
	for _, r := range rows {
		out[r.ExtID] = append(out[r.ExtID], GrantSnapshot{
			ExtID: r.ExtID, Capability: r.Capability, Params: r.Params, Scope: r.Scope,
		})
	}
	return out, nil
}

// HasAnyCapability reports whether the session holds at least one grant for every key in
// `keys`, within one extension.
//
// This is the agent's whole check, and it is intentionally shallow: it answers "could this
// possibly be allowed", never "is this call covered". A false positive costs one call the
// extension then refuses; a false negative would block work the user authorised.
/**
 * GroupScope is the scope value that narrows a grant to one group.
 *
 * THE PREFIX IS PART OF THE VALUE, so a scope is self-describing: `group:<id>` says which kind of object it names
 * without the reader having to consult the capability it was granted for. A bare id would be ambiguous the moment a
 * second object kind needs scoping, and the column stores one string.
 */
func GroupScope(groupID string) string { return "group:" + groupID }

/**
 * HasCapabilityInScope reports whether a set of grants covers one capability for one object.
 *
 * A GRANT WITH NO SCOPE COVERS EVERYTHING. That is what almost every permission is, and it is what makes this a
 * narrowing rather than a new requirement: an unscoped `group.spawn` still means "anywhere", which is how the
 * capability behaved before scopes existed.
 *
 * THE SCOPED FORM IS AN ADDITION, NOT A REPLACEMENT. A session that was granted `group.spawn` for one group and
 * nothing else holds it there, fails the check for a group it was not granted, and fails the unscoped check too —
 * which is the point: the narrow grant must not read as the broad one.
 */
func HasCapabilityInScope(grants []GrantSnapshot, capability, scope string) bool {
	for _, g := range grants {
		if g.Capability != capability {
			continue
		}
		if g.Scope == "" || g.Scope == scope {
			return true
		}
	}
	return false
}

func HasAnyCapability(grants []GrantSnapshot, keys []string) (missing string, ok bool) {
	if len(keys) == 0 {
		return "", true
	}
	held := make(map[string]bool, len(grants))
	for _, g := range grants {
		held[g.Capability] = true
	}
	for _, k := range keys {
		if !held[k] {
			return k, false
		}
	}
	return "", true
}

// SortedCapabilities lists the distinct capabilities in a set, for stable output.
func SortedCapabilities(grants []GrantSnapshot) []string {
	seen := make(map[string]bool, len(grants))
	out := make([]string, 0, len(grants))
	for _, g := range grants {
		if seen[g.Capability] {
			continue
		}
		seen[g.Capability] = true
		out = append(out, g.Capability)
	}
	sort.Strings(out)
	return out
}

var _ = gorm.ErrRecordNotFound
