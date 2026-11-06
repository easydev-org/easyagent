package store

import (
	"errors"
	"sort"

	"gorm.io/gorm"
)

/**
 * Group is a named set of sessions that can see each other.
 *
 * CREATED BY A USER, NEVER BY A SESSION OR AN EXTENSION, and that is a security boundary rather than
 * a convention. A group is a view of the tenant — who is working with whom. An extension that could
 * create one could assemble a set of sessions and then read the membership back, which is
 * information nobody granted it. Keeping group creation on the console side means the only thing that
 * can name a set of sessions is the person directing the work.
 *
 * WHAT IT ANSWERS: "who else is here". That is the whole contract. Messages still travel over
 * channels, so a group is a DIRECTORY plus a fan-out address, not a second message store — a group
 * that owned a transcript would need its own read watermarks, unread counts and "what did I miss"
 * answer, all of which the session already has.
 *
 * NO DESCRIPTION FIELD, but there IS a one-line INTRO — the same field a session has, for the same reason.
 * It is not a description of the group's purpose written once and forgotten; it is what a member reads to
 * remember what this set of sessions is FOR, and it rides in the turn's <env> so a woken coordinator does not
 * have to infer its job from the member names. A full description would be a document nobody keeps current.
 */
type Group struct {
	Tenant string `gorm:"column:tenant;primaryKey;size:64"`
	// Name is the group's identity AND its address segment: messages to the group use it, so it is
	// constrained to a subject segment exactly like a session name.
	Name string `gorm:"column:name;primaryKey;size:128"`

	/**
	 * AvatarFile is the group's picture, in the same `file:<code>` form a session uses.
	 *
	 * EMPTY MEANS "DERIVE ONE": the console seeds the same honeycomb identicon from the group name,
	 * so a group with no picture still has a stable, recognisable face rather than a blank circle.
	 */
	AvatarFile string `gorm:"column:avatar_file;size:64"`

	/**
	 * Intro is one line saying what this group is FOR — "release coordination for acme/app", "incident 42".
	 *
	 * IT IS THE SAME IDEA AS A SESSION'S INTRO, and it is deliberately one line: a group with a paragraph of
	 * description would be a document that goes stale, and the member list is what a reader actually needs.
	 * EMPTY MEANS "SAY NOTHING" — nothing renders an empty tag.
	 */
	Intro string `gorm:"column:intro;size:512"`

	CreatedBy string `gorm:"column:created_by;size:128"`
	CreatedAt string `gorm:"column:created_at"`
	UpdatedAt string `gorm:"column:updated_at"`
}

func (Group) TableName() string { return "groups" }

/**
 * GroupMember is one session's membership in one group.
 *
 * THE ROLE IS ABOUT MESSAGE ROUTING, NOT MANAGEMENT. It decides who a message the user sends to the
 * group reaches:
 *
 *   coordinator  RECEIVES what the user says to the group. It reads the instruction, decides how to
 *                split the work, and hands pieces to individual members with `group-send`. Exactly one
 *                per group, because two would both answer the same user message and their answers
 *                would talk past each other.
 *   member       receives NOTHING by default. It is addressed when the coordinator names it — that
 *                is what "@" means here: the coordinator writes to that session's mailbox.
 *
 * SO THE ROLE LIVES ON THE MEMBERSHIP, NOT THE SESSION: the same session can coordinate one group
 * and be an ordinary member of another, and a column on `sessions` could only express one of them.
 */
type GroupMember struct {
	Tenant  string `gorm:"column:tenant;primaryKey;size:64"`
	Group   string `gorm:"column:group;primaryKey;size:128"`
	Session string `gorm:"column:session;primaryKey;size:128"`

	// Role is GroupRoleCoordinator or GroupRoleMember.
	Role string `gorm:"column:role;size:32"`

	JoinedAt string `gorm:"column:joined_at"`
	// JoinedBy records provenance: "user" or "user:<name>". A member nobody remembers adding needs
	// an answer, and it can never be an extension, because extensions cannot create or edit groups.
	JoinedBy string `gorm:"column:joined_by;size:128"`
}

// Group roles: who the group's user messages reach, and who waits to be addressed.
const (
	GroupRoleCoordinator = "coordinator"
	GroupRoleMember      = "member"
)

func (GroupMember) TableName() string { return "group_members" }

// ErrGroupNotFound is returned when a group does not exist for the tenant.
var ErrGroupNotFound = errors.New("group not found")

// ErrGroupExists is returned when creating a group whose name is taken.
var ErrGroupExists = errors.New("group already exists")

// ErrNoCoordinator is returned when a group is created or edited without exactly one coordinator.
var ErrNoCoordinator = errors.New("a group needs exactly one coordinator")

/**
 * A SESSION MAY COORDINATE ANY NUMBER OF GROUPS.
 *
 * There used to be a one-coordinator-per-session rule (`ErrAlreadyCoordinating`), on the argument that a
 * coordinator receiving two groups' work could not keep them apart. The argument was right about the RISK and
 * wrong about the remedy: a session that cannot tell which group a message came from is a DEFECT IN THE MESSAGE,
 * not a reason to forbid the arrangement. Every group message already carries its group name, and the prompt now
 * lists every group the session coordinates, so the separation is stated rather than assumed.
 *
 * WHAT IT COST TO KEEP THE RULE: an operator running several groups had to dedicate a session to each, and those
 * sessions shared nothing — no context, no history, no learned procedure. The work is the same work; only the
 * conversation was duplicated.
 *
 * THE CONSTRAINT THAT REMAINS is the one a group cannot survive without: exactly ONE COORDINATOR PER GROUP. None
 * routes its messages nowhere; two would both answer the same message.
 */

// ErrNotGroupMember is returned when a session asks about a group it does not belong to.
//
// A SESSION THAT IS NOT A MEMBER MUST NOT LEARN THE GROUP EXISTS, so callers turn this into
// NOT_FOUND. Distinguishing "no such group" from "not in it" would let any session enumerate the
// tenant's groups by watching which names return the more specific error.
var ErrNotGroupMember = errors.New("session is not a member of this group")

// CreateGroup inserts a group and then its memberships.
//
// THE MEMBERS ARE INSIDE THE SAME TRANSACTION as the group. A group whose membership write failed
// would exist and be visible to nobody — unreachable, with nothing in the API that could repair it.
func (d *DB) CreateGroup(tenant string, g Group, coordinator string, members []string, now string) error {
	// EXACTLY ONE COORDINATOR. None means a group whose messages reach nobody; two means both answer
	// the same user message. Refusing here is better than either failure, which would only show up
	// when someone tried to use the group.
	if coordinator == "" {
		return ErrNoCoordinator
	}
	return d.g.Transaction(func(tx *gorm.DB) error {
		var n int64
		if err := tx.Model(&Group{}).Where("tenant = ? AND name = ?", tenant, g.Name).
			Count(&n).Error; err != nil {
			return err
		}
		if n > 0 {
			return ErrGroupExists
		}
		g.Tenant = tenant
		if err := tx.Create(&g).Error; err != nil {
			return err
		}
		seen := map[string]bool{coordinator: true}
		if err := tx.Create(&GroupMember{
			Tenant: tenant, Group: g.Name, Session: coordinator,
			Role: GroupRoleCoordinator, JoinedAt: now, JoinedBy: g.CreatedBy,
		}).Error; err != nil {
			return err
		}
		for _, m := range members {
			if m == "" || seen[m] {
				continue
			}
			seen[m] = true
			if err := tx.Create(&GroupMember{
				Tenant: tenant, Group: g.Name, Session: m,
				Role: GroupRoleMember, JoinedAt: now, JoinedBy: g.CreatedBy,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// GetGroup returns (group, true) or (nil, false).
func (d *DB) GetGroup(tenant, name string) (*Group, bool, error) {
	var g Group
	err := d.g.Where("tenant = ? AND name = ?", tenant, name).First(&g).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &g, true, nil
}

// UpdateGroupAvatar sets (or clears, with "") the group's picture.
func (d *DB) UpdateGroupAvatar(tenant, name, avatarFile, now string) error {
	return d.g.Model(&Group{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{"avatar_file": avatarFile, "updated_at": now}).Error
}

/**
 * UpdateGroupProfile writes the group's picture and one-line intro in one statement.
 *
 * `avatarFile` AND `intro` ARE PLAIN STRINGS, not pointers: the caller decides whether to include them, and
 * both callers here always do — a profile save sends the whole card, so "absent = leave it alone" is handled
 * once, at the wire, where an `optional` field can express it. Converting that to two nullable arguments here
 * would duplicate the rule in a second place.
 */
func (d *DB) UpdateGroupProfile(tenant, name, avatarFile, intro, now string) error {
	return d.g.Model(&Group{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{
			"avatar_file": avatarFile, "intro": intro, "updated_at": now,
		}).Error
}

// DeleteGroup removes a group and its memberships.
func (d *DB) DeleteGroup(tenant, name string) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant = ? AND \"group\" = ?", tenant, name).
			Delete(&GroupMember{}).Error; err != nil {
			return err
		}
		return tx.Where("tenant = ? AND name = ?", tenant, name).Delete(&Group{}).Error
	})
}

/**
 * AddGroupMembers enrolls sessions, idempotently.
 *
 * RE-ADDING AN EXISTING MEMBER IS A NO-OP RATHER THAN AN ERROR. "Add" from a console that cannot
 * tell whether a member is already present should just succeed; failing here would surface as a
 * conflict the user has to interpret from a list they are looking at anyway.
 *
 * THE FIRST MEMBER ADDED TO AN EMPTY GROUP DOES NOT BECOME COORDINATOR. Silently promoting someone
 * would answer "who receives the group's messages" differently depending on the order rows were
 * written, which is not an answer a user can predict. `SetGroupCoordinator` is the only way to choose.
 */
func (d *DB) AddGroupMembers(tenant, group string, sessions []string, now, addedBy string) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		var existing []GroupMember
		if err := tx.Where("tenant = ? AND \"group\" = ?", tenant, group).
			Find(&existing).Error; err != nil {
			return err
		}
		have := make(map[string]bool, len(existing))
		for _, m := range existing {
			have[m.Session] = true
		}
		for _, s := range sessions {
			if s == "" || have[s] {
				continue
			}
			have[s] = true
			if err := tx.Create(&GroupMember{
				Tenant: tenant, Group: group, Session: s,
				Role: GroupRoleMember, JoinedAt: now, JoinedBy: addedBy,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

/**
 * CoordinatedGroups returns every group this session coordinates, oldest first.
 *
 * IT USED TO RETURN ONE, because a session could coordinate only one. A plural answer is what the caller needs
 * now, and it is deliberately a LIST rather than "the first one":
 *
 *   · the prompt has to tell a session about EVERY group it coordinates, or a message from the second one wakes
 *     it with the wrong context — the defect the old rule was working around;
 *   · `spawn` has to know which groups it may add a worker to, and picking "the first" would put the worker in a
 *     group nobody named.
 *
 * SORTED BY NAME rather than by row order: the prompt is built from this on every turn, and an unstable order
 * would rewrite the system prompt for no reason.
 */
func (d *DB) CoordinatedGroups(tenant, session string) ([]string, error) {
	var rows []GroupMember
	if err := d.g.Where("tenant = ? AND session = ? AND role = ?",
		tenant, session, GroupRoleCoordinator).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Group)
	}
	sort.Strings(out)
	return out, nil
}

/**
 * GroupCoordinator returns the session that receives the group's user messages.
 *
 * THE OTHER DIRECTION OF THE SAME RULE as CoordinatedGroup — "who coordinates this group" versus "which
 * group does this session coordinate". Both are needed: a coordinator picks its group when it spawns, and
 * a group picks its coordinator when a message arrives. They are kept as two explicit queries rather than
 * one parameterised by column name: a column name threaded through a query string is the kind of
 * convenience that becomes a footgun the first time it carries a caller's value.
 *
 * Empty means the group has no coordinator — a state this code tries not to produce (creation requires
 * one, and removing one is refused) but which a direct database edit can still create. The caller treats
 * it as "this group routes nowhere" rather than guessing a recipient.
 */
func (d *DB) GroupCoordinator(tenant, group string) (string, error) {
	var m GroupMember
	err := d.g.Where("tenant = ? AND \"group\" = ? AND role = ?", tenant, group, GroupRoleCoordinator).
		First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return m.Session, nil
}

/**
 * SetGroupCoordinator moves the coordinator role to another member.
 *
 * ONE TRANSACTION, because the two writes are a single fact: demote the old and promote the new. A
 * half-applied change leaves either two coordinators (both answering the user's message) or none
 * (the group routes nowhere), and both are worse than the change failing outright.
 *
 * THE NEW COORDINATOR MUST ALREADY BE A MEMBER. Promoting a stranger would add a session to the group
 * as a side effect of naming it, which is the opposite of what an explicit member list is for.
 */
func (d *DB) SetGroupCoordinator(tenant, group, session string) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		var n int64
		if err := tx.Model(&GroupMember{}).
			Where("tenant = ? AND \"group\" = ? AND session = ?", tenant, group, session).
			Count(&n).Error; err != nil {
			return err
		}
		if n == 0 {
			return ErrNotGroupMember
		}
		if err := tx.Model(&GroupMember{}).
			Where("tenant = ? AND \"group\" = ? AND role = ?", tenant, group, GroupRoleCoordinator).
			Update("role", GroupRoleMember).Error; err != nil {
			return err
		}
		return tx.Model(&GroupMember{}).
			Where("tenant = ? AND \"group\" = ? AND session = ?", tenant, group, session).
			Update("role", GroupRoleCoordinator).Error
	})
}

/**
 * RemoveGroupMember drops one membership.
 *
 * THE COORDINATOR CANNOT BE REMOVED WITHOUT A REPLACEMENT, because a group with none routes the
 * user's messages nowhere and nothing in the API would say so. The caller either nominates someone
 * else first or sees this refusal — the dead end is reported at the moment it would be created.
 */
func (d *DB) RemoveGroupMember(tenant, group, session string) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		var m GroupMember
		err := tx.Where("tenant = ? AND \"group\" = ? AND session = ?", tenant, group, session).
			First(&m).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotGroupMember
		}
		if err != nil {
			return err
		}
		if m.Role == GroupRoleCoordinator {
			return ErrCoordinatorRequired
		}
		return tx.Where("tenant = ? AND \"group\" = ? AND session = ?", tenant, group, session).
			Delete(&GroupMember{}).Error
	})
}

// ErrCoordinatorRequired is returned when removing the coordinator would leave the group unroutable.
var ErrCoordinatorRequired = errors.New("the coordinator cannot be removed; move the role first")

// ListGroupMembers returns a group's members, oldest join first.
func (d *DB) ListGroupMembers(tenant, group string) ([]GroupMember, error) {
	var out []GroupMember
	err := d.g.Where("tenant = ? AND \"group\" = ?", tenant, group).
		Order("CASE WHEN role = 'coordinator' THEN 0 ELSE 1 END, joined_at, session").
		Find(&out).Error
	return out, err
}

// IsGroupMember reports whether a session belongs to a group.
func (d *DB) IsGroupMember(tenant, group, session string) (bool, error) {
	var n int64
	err := d.g.Model(&GroupMember{}).
		Where("tenant = ? AND \"group\" = ? AND session = ?", tenant, group, session).
		Count(&n).Error
	return n > 0, err
}

/**
 * GroupsOfSession lists the groups a session belongs to.
 *
 * SORTED BY NAME at the store rather than left to the caller: this is rendered as a list, and an
 * unstable order makes the same session's groups appear to shuffle between refreshes.
 */
func (d *DB) GroupsOfSession(tenant, session string) ([]Group, error) {
	var memberships []GroupMember
	if err := d.g.Where("tenant = ? AND session = ?", tenant, session).
		Find(&memberships).Error; err != nil {
		return nil, err
	}
	if len(memberships) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(memberships))
	for _, m := range memberships {
		names = append(names, m.Group)
	}
	var groups []Group
	if err := d.g.Where("tenant = ? AND name IN ?", tenant, names).
		Order("name").Find(&groups).Error; err != nil {
		return nil, err
	}
	return groups, nil
}

// ListGroups returns every group in the tenant, sorted by name (the console's view).
func (d *DB) ListGroups(tenant string) ([]Group, error) {
	var out []Group
	err := d.g.Where("tenant = ?", tenant).Order("name").Find(&out).Error
	return out, err
}

// GroupMemberCounts returns member counts for the given group names.
//
// ONE QUERY FOR THE WHOLE LIST, not one per group: a list of twenty groups would otherwise cost
// twenty round trips to render a number nobody would wait for.
func (d *DB) GroupMemberCounts(tenant string, groups []string) (map[string]int, error) {
	out := make(map[string]int, len(groups))
	if len(groups) == 0 {
		return out, nil
	}
	type row struct {
		Group string
		N     int
	}
	var rows []row
	if err := d.g.Model(&GroupMember{}).
		Select("\"group\" as group, count(*) as n").
		Where("tenant = ? AND \"group\" IN ?", tenant, groups).
		Group("\"group\"").Scan(&rows).Error; err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.Group] = r.N
	}
	return out, nil
}

/**
 * GroupMemberProfile is one member as a READER sees it: who it is, what it is, and what role it holds.
 *
 * THE ID ALONE IS NOT ENOUGH, and this is not a presentation nicety. A coordinator reading a bare list of ids
 * could see the members but not which one it was, not what each of them does, and not who was answerable for
 * the group — and it said so, in its answer to a live group. The display name and the one-line intro exist
 * precisely so a peer can decide whom to address and with what.
 *
 * ROLE IS INCLUDED because it is a ROUTING fact: reports go to the coordinator, and a member that cannot tell
 * which row that is has to guess.
 */
type GroupMemberProfile struct {
	Session  string
	Name     string
	Intro    string
	Role     string
	JoinedAt string
}

/**
 * GroupMemberProfiles returns a group's members with their profiles, coordinator first then oldest join.
 *
 * A MEMBER WHOSE SESSION ROW IS GONE KEEPS ITS PLACE. The membership row is removed when a session is deleted,
 * so in practice this does not arise; when it does — a partially applied delete — the member still appears,
 * by id, rather than silently vanishing from a list that other sessions rely on to know who is present.
 */
func (d *DB) GroupMemberProfiles(tenant, group string) ([]GroupMemberProfile, error) {
	rows, err := d.ListGroupMembers(tenant, group)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.Session)
	}
	var sessions []Session
	if err := d.g.Where("tenant = ? AND id IN ?", tenant, ids).Find(&sessions).Error; err != nil {
		return nil, err
	}
	byID := make(map[string]Session, len(sessions))
	for _, s := range sessions {
		byID[s.ID] = s
	}
	out := make([]GroupMemberProfile, 0, len(rows))
	for _, r := range rows {
		p := GroupMemberProfile{Session: r.Session, Role: r.Role, JoinedAt: r.JoinedAt}
		if s, ok := byID[r.Session]; ok {
			p.Name = s.Name
			p.Intro = s.Intro
		}
		out = append(out, p)
	}
	return out, nil
}

// SessionsInGroupIDs returns the session ids in a group, sorted, optionally excluding one.
//
// IT IS ListGroupMembers REDUCED TO THE ID, not a second query: the two read the same table with the
// same predicate, and a duplicate query is a second place for the membership rule to drift. The
// exclusion exists because "who else is here" is the question, and a caller that receives itself in the
// list has to remember to filter it out — which every caller then does differently, and one forgets.
func (d *DB) SessionsInGroupIDs(tenant, group, exclude string) ([]string, error) {
	rows, err := d.ListGroupMembers(tenant, group)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if exclude != "" && r.Session == exclude {
			continue
		}
		out = append(out, r.Session)
	}
	sort.Strings(out)
	return out, nil
}

// DeleteGroupMemberships drops every membership of a session (called when the session is deleted).
//
// A DELETED SESSION MUST NOT STAY IN A MEMBER LIST. The group would keep advertising a session that
// cannot answer, and nothing would ever remove it — the row's owner is gone.
func (d *DB) DeleteGroupMemberships(tenant, session string) error {
	return d.g.Where("tenant = ? AND session = ?", tenant, session).
		Delete(&GroupMember{}).Error
}
