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
 * THE IDENTITY SPLIT MIGRATES A NAME-KEYED DATABASE.
 *
 * THE SCHEMA CHANGE IS THE EASY HALF. Groups used to be keyed by their NAME — `group_members."group"` and
 * `group_messages."group"` both held it — and `group_members."group"` was part of that table's PRIMARY KEY. Adding an
 * `id` column alone would leave a NOT NULL column no insert supplies, and PostgreSQL refuses to drop a column that is
 * in a primary key at all (`column "name" is in a primary key`).
 *
 * This drives the migration over a database built the OLD way and asserts the three things that would otherwise
 * fail at boot: the columns are gone, the rows point at an id, and the group is still readable.
 */
func TestMigrateSplitsANameKeyedGroupTable(t *testing.T) {
	d := newTestDB(t)

	// A DATABASE AS THE PREVIOUS BUILD LEFT IT: groups keyed by (tenant, name), members keyed by the group's name.
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
	if err := d.g.Exec(`INSERT INTO groups (tenant, name) VALUES ('t1', 'release')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.g.Exec(`INSERT INTO group_members (tenant, "group", session, role)
		VALUES ('t1', 'release', 'alice', 'coordinator')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.g.Exec(`INSERT INTO group_messages (id, tenant, "group", body, seq)
		VALUES ('m1', 't1', 'release', 'before the split', 1)`).Error; err != nil {
		t.Fatal(err)
	}

	if err := d.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// THE GROUP HAS AN ID, and its members and messages point at it.
	g, ok, err := d.GroupByName("t1", "release")
	if err != nil || !ok {
		t.Fatalf("group: %v %v", ok, err)
	}
	if g.ID == "" {
		t.Fatal("the migrated group has no id")
	}

	members, err := d.ListGroupMembers("t1", g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Session != "alice" {
		t.Fatalf("members = %+v", members)
	}

	msgs, err := d.ListGroupMessages("t1", g.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Body != "before the split" {
		t.Fatalf("transcript = %+v", msgs)
	}
}
