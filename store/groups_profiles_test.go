package store

import "testing"

/**
 * A MEMBER LIST IS A LIST OF SESSIONS, NOT OF IDS.
 *
 * The ids alone answered "who is here" and nothing else. A live coordinator read the list, could not tell which
 * row was itself, could not tell what anyone did, and said so in its answer to the group — because the display
 * name and the intro are how a peer decides whom to address and with what.
 *
 * ROLE COMES WITH IT because it is a routing fact: reports go to the coordinator, and a member that cannot tell
 * which row that is has to guess. The coordinator sorts first so the answer is visible without reading every
 * intro.
 */
func TestGroupMemberProfilesCarryNameIntroAndRole(t *testing.T) {
	d := newTestDB(t)

	coord := &SessionInput{Name: "发布协调", Model: "p/m", Locale: "zh", MaxTurns: 10}
	if err := d.CreateSession("t1", coord); err != nil {
		t.Fatal(err)
	}
	member := &SessionInput{Name: "CI 巡检", Model: "p/m", MaxTurns: 10}
	if err := d.CreateSession("t1", member); err != nil {
		t.Fatal(err)
	}
	// Intro is set through the settings patch, which is the only writer that touches it.
	if err := d.UpdateSessionSettings("t1", coord.ID, map[string]any{"intro": "协调发布流程"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateSessionSettings("t1", member.ID, map[string]any{"intro": "看着 main 的构建"}); err != nil {
		t.Fatal(err)
	}
	// A session with neither a name nor an intro: it must still appear, by id.
	bare := &SessionInput{Model: "p/m", MaxTurns: 10}
	if err := d.CreateSession("t1", bare); err != nil {
		t.Fatal(err)
	}

	grp := &Group{Name: "release"}
	if err := d.CreateGroup("t1", grp, coord.ID,
		[]string{member.ID, bare.ID}, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}

	// THE PROFILES ARE READ BY GROUP ID, which is what CreateGroup wrote back into `grp`.
	got, err := d.GroupMemberProfiles("t1", grp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d members, want 3: %+v", len(got), got)
	}

	// THE COORDINATOR COMES FIRST, so "who is answerable" is answered before any intro is read.
	if got[0].Session != coord.ID || got[0].Role != GroupRoleCoordinator {
		t.Errorf("first row = %+v, want the coordinator", got[0])
	}
	if got[0].Name != "发布协调" || got[0].Intro != "协调发布流程" {
		t.Errorf("the coordinator's profile was not returned: %+v", got[0])
	}

	byID := map[string]GroupMemberProfile{}
	for _, m := range got {
		byID[m.Session] = m
	}
	if m := byID[member.ID]; m.Name != "CI 巡检" || m.Intro != "看着 main 的构建" || m.Role != GroupRoleMember {
		t.Errorf("member row = %+v", m)
	}
	// AN EMPTY PROFILE KEEPS ITS ROW: dropping it would hide a member the group is told exists.
	if m, ok := byID[bare.ID]; !ok {
		t.Error("a member with no name and no intro was dropped")
	} else if m.Name != "" || m.Intro != "" || m.Role != GroupRoleMember {
		t.Errorf("bare member row = %+v", m)
	}
}

// A group with nobody in it returns an empty list rather than an error: "no members" is an answer.
func TestGroupMemberProfilesOnAnEmptyGroup(t *testing.T) {
	d := newTestDB(t)
	got, err := d.GroupMemberProfiles("t1", "nobody")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("got %d members of a group that does not exist: %+v", len(got), got)
	}
}

/**
 * A DATABASE BUILT WITH THE OLD GROUP SCHEMA IS RECREATED, NOT MIGRATED.
 *
 * Groups used to be keyed by their NAME, which made a rename a different group. The new shape keys them by an id.
 * Reconciling that in place would mean rewriting membership rows, swapping primary keys and dropping columns whose
 * invariants the old schema still enforces — a migration whose only purpose is preserving data from a build that was
 * never released.
 *
 * SO THE TABLES ARE DROPPED AND AutoMigrate RECREATES THEM. This test proves the drop happens and that the result is
 * usable: a group can be created and read back immediately afterwards, with an id.
 */
func TestMigrateRecreatesLegacyGroupTables(t *testing.T) {
	d := newTestDB(t)

	// A DATABASE AS THE PREVIOUS BUILD LEFT IT: keyed by (tenant, name), members keyed by the group's name.
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS group_members`,
		`DROP TABLE IF EXISTS group_messages`,
		`DROP TABLE IF EXISTS groups`,
		`CREATE TABLE groups (tenant TEXT, name TEXT, avatar_file TEXT, intro TEXT,
			created_by TEXT, created_at TEXT, updated_at TEXT, PRIMARY KEY (tenant, name))`,
		`CREATE TABLE group_members (tenant TEXT, "group" TEXT, session TEXT, role TEXT,
			joined_at TEXT, joined_by TEXT, PRIMARY KEY (tenant, "group", session))`,
		`CREATE TABLE group_messages (id TEXT PRIMARY KEY, tenant TEXT, "group" TEXT, sender TEXT,
			sender_name TEXT, body TEXT, seq INTEGER, created_at TEXT, attachments TEXT)`,
	} {
		if err := d.g.Exec(stmt).Error; err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	if err := d.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// THE OLD COLUMNS ARE GONE, and the new shape works.
	if d.g.Migrator().HasColumn("group_members", "group") {
		t.Fatal("the legacy group column survived; the table was not recreated")
	}
	if !d.g.Migrator().HasColumn("groups", "id") {
		t.Fatal("the recreated groups table has no id")
	}

	in := &SessionInput{Name: "coord", Model: "p/m"}
	if err := d.CreateSession("t1", in); err != nil {
		t.Fatal(err)
	}
	g := &Group{Name: "release"}
	if err := d.CreateGroup("t1", g, in.ID, nil, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if g.ID == "" {
		t.Fatal("a group created after the recreate has no id")
	}
	got, ok, err := d.GetGroup("t1", g.ID)
	if err != nil || !ok || got.Name != "release" {
		t.Fatalf("read back: %v %v %+v", ok, err, got)
	}
}

/**
 * A DATABASE THAT STILL CARRIES THE UNIQUE GROUP-NAME INDEX IS REPAIRED AT BOOT.
 *
 * THE INDEX IS A LEFTOVER, and it is the kind that hides: AutoMigrate adds and alters but never drops, so a
 * database created while the name WAS the identity kept `uq_groups_tenant_name` forever. The model's change to a
 * plain index is invisible to GORM — the column is still indexed, and the extra uniqueness is not something it looks
 * for. The failure would surface as a second group called "check the build" being refused by a constraint whose
 * reason no longer exists.
 */
func TestMigrateDropsTheStaleUniqueGroupNameIndex(t *testing.T) {
	d := newTestDB(t)

	// A DATABASE FROM BEFORE THE CHANGE: the name unique, the way it was when the name was the identity.
	if err := d.g.Exec(`DROP INDEX IF EXISTS uq_groups_tenant_name`).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.g.Exec(`CREATE UNIQUE INDEX uq_groups_tenant_name ON groups (tenant, name)`).Error; err != nil {
		t.Fatal(err)
	}

	if err := d.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	in := &SessionInput{Name: "c", Model: "p/m"}
	if err := d.CreateSession("t1", in); err != nil {
		t.Fatal(err)
	}
	first := &Group{Name: "check the build"}
	if err := d.CreateGroup("t1", first, in.ID, nil, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	// THE SAME LABEL AGAIN, which is the whole point: two sessions doing similar work get two rooms with one name.
	second := &Group{Name: "check the build"}
	if err := d.CreateGroup("t1", second, in.ID, nil, "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("a repeated group name was refused: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("the two groups collapsed into one")
	}
}
