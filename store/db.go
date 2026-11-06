package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Backend selects the SQL dialect.
type Backend string

const (
	Postgres Backend = "pg"
	MySQL    Backend = "mysql"
	SQLite   Backend = "sqlite"
)

// DB wraps a GORM handle with the resolved backend.
type DB struct {
	g       *gorm.DB
	backend Backend
}

// Open connects. dsn is the driver-specific connection string; for SQLite it is
// a file path (or ":memory:").
func Open(backend Backend, dsn string) (*DB, error) {
	var (
		g   *gorm.DB
		err error
	)
	cfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	switch backend {
	case Postgres:
		g, err = gorm.Open(postgres.Open(dsn), cfg)
	case MySQL:
		g, err = gorm.Open(mysql.Open(dsn), cfg)
	case SQLite:
		g, err = gorm.Open(sqlite.Open(dsn), cfg)
	default:
		return nil, fmt.Errorf("store: unknown backend %q", backend)
	}
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", backend, err)
	}
	// SQLite is a single-writer engine: a multi-connection pool hits
	// "database is locked" under concurrency. Serialize to one connection.
	if backend == SQLite {
		if sqlDB, e := g.DB(); e == nil {
			sqlDB.SetMaxOpenConns(1)
		}
	}
	return &DB{g: g, backend: backend}, nil
}

// G exposes the underlying GORM handle (advanced callers/tests).
func (d *DB) G() *gorm.DB { return d.g }

// Backend reports the dialect.
func (d *DB) Backend() Backend { return d.backend }

/**
 * Migrate creates the INITIAL schema (create-if-absent). It is not a migration
 * path: there is no legacy schema to upgrade.
 *
 * IT IS SAFE TO RUN ON SEVERAL REPLICAS AT ONCE, which is not what it used to be. `AutoMigrate` is
 * check-then-create: two processes starting together both see "table does not exist" and both issue CREATE
 * TABLE, so the loser gets `table sessions already exists` and EXITS. A two-replica deployment therefore came
 * up as one replica, and the failure looked like a configuration problem rather than a race.
 *
 * THE RETRY IS THE FIX, and it is honest about why: the error says another replica created exactly the object
 * this one was about to create. Waiting for it to finish and trying again converges — the second attempt sees
 * the object and moves on — while a lock would need a different mechanism per backend (advisory locks on
 * Postgres, GET_LOCK on MySQL, flock on the SQLite file) and would fail in new ways when one of them is
 * unavailable.
 *
 * THE ATTEMPTS ARE BOUNDED AND SPACED. A schema that genuinely cannot be created (a permissions problem, a
 * conflicting definition) still fails, and it fails with the last real error rather than with a timeout.
 */
func (d *DB) Migrate() error {
	const attempts = 10
	var err error
	for i := 0; i < attempts; i++ {
		err = d.g.AutoMigrate(
			&Session{}, &Message{}, &Part{},
			&Provider{}, &Tenant{}, &TenantToken{}, &AgentFile{},
			&BundledTodo{}, &AdminToken{}, &SessionToken{},
			&SessionGrant{}, &Group{}, &GroupMember{}, &GroupMessage{},
		)
		if err == nil {
			break
		}
		if !isConcurrentSchemaChange(err) {
			return err
		}
		// A LOST RACE, NOT A BROKEN SCHEMA. Space the retries so the winner has time to commit; the delay grows
		// because the loser is waiting on a DDL statement it cannot see.
		time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
	}
	if err != nil {
		return err
	}
	// Backfill: every pre-existing tenant becomes its own subtree root
	// (scope_id = id, parent_id = NULL). Idempotent.
	if err := d.g.Model(&Tenant{}).Where("scope_id = ? OR scope_id IS NULL", "").
		Update("scope_id", gorm.Expr("id")).Error; err != nil {
		return err
	}
	if err := d.backfillMessageSeq(); err != nil {
		return err
	}
	return d.backfillGroupIDs()
}

/**
 * backfillGroupIDs gives every pre-existing group an identity and re-keys its rows.
 *
 * THE SCHEMA CHANGE IS THE EASY HALF. Groups used to be keyed by their NAME — `group_members."group"` and
 * `group_messages."group"` both held it — so adding an `id` column alone would leave every membership and every
 * message pointing at a name that is no longer an identity. This walks the groups that still lack one and rewrites
 * the references.
 *
 * IDEMPOTENT, and gated on "any group with an empty id", so a normal boot does one cheap count. It runs INSIDE A
 * TRANSACTION because a half-rewritten group is one whose transcript and memberships disagree about which group
 * they belong to.
 *
 * THE OLD COLUMNS ARE DROPPED, and they have to be: `group_members."group"` was part of that table's PRIMARY KEY, so
 * it is NOT NULL, and an insert that no longer supplies it fails outright. Keeping them "for a rollback" would mean
 * keeping a database the new code cannot write to — the opposite of what was intended.
 *
 * A ROLLBACK IS STILL POSSIBLE, one version back: the previous build reads the name-keyed columns, which no longer
 * exist. That is the cost of the split, and it is why this ships as a version of its own rather than inside a
 * feature release.
 */
func (d *DB) backfillGroupIDs() error {
	var groups []Group
	if err := d.g.Where("id IS NULL OR id = ?", "").Find(&groups).Error; err != nil {
		return err
	}
	if len(groups) == 0 {
		return nil
	}
	for _, g := range groups {
		id, err := NewSessionID()
		if err != nil {
			return err
		}
		if err := d.g.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&Group{}).Where("tenant = ? AND name = ?", g.Tenant, g.Name).
				Update("id", id).Error; err != nil {
				return err
			}
			// THE REFERENCES ARE REWRITTEN BY NAME, which is the only identity those rows ever had. Doing it in the
			// same transaction as the id is what keeps a crash from leaving rows nobody can attribute.
			if err := tx.Model(&GroupMember{}).
				Where("tenant = ? AND \"group\" = ?", g.Tenant, g.Name).
				Updates(map[string]any{"group_id": id}).Error; err != nil {
				return err
			}
			return tx.Model(&GroupMessage{}).
				Where("tenant = ? AND \"group\" = ?", g.Tenant, g.Name).
				Updates(map[string]any{"group_id": id}).Error
		}); err != nil {
			return err
		}
	}
	/**
	 * ORDER MATTERS AND IT IS NOT OBVIOUS: rewrite, THEN re-key, THEN drop.
	 *
	 *   · the rewrite needs the OLD column to find the rows, so it cannot come after the drop;
	 *   · the primary key cannot move to `id` while any row still has a NULL id — PostgreSQL says
	 *     `column "id" ... contains null values` — so the re-key cannot come before the rewrite;
	 *   · the old column cannot be dropped while it is in the key, so the drop comes last.
	 *
	 * Getting this wrong does not corrupt anything: it fails at boot, loudly, which is how the first version of
	 * this migration was caught.
	 */
	return d.rekeyAndDropLegacyGroupColumns()
}

/**
 * dropLegacyGroupColumns removes the name-keyed columns the identity split replaced.
 *
 * EACH DROP IS BEST-EFFORT AND IDEMPOTENT: the column may already be gone, or the backend may refuse while an index
 * still references it. A failure here is not fatal to boot — the app writes only the new columns — but it leaves a
 * NOT NULL column unsatisfied, which is why the drops are attempted before the first insert rather than lazily.
 */
func (d *DB) rekeyAndDropLegacyGroupColumns() error {
	/**
	 * ORDER MATTERS AND IT IS NOT OBVIOUS: rewrite, THEN re-key, THEN drop.
	 *
	 *   · the rewrite needs the OLD column to find the rows, so it cannot come after the drop;
	 *   · the primary key cannot move to `id` while any row still has a NULL id — PostgreSQL says
	 *     `column "id" ... contains null values` — so the re-key cannot come before the rewrite;
	 *   · the old column cannot be dropped while it is in the key, so the drop comes last.
	 *
	 * EVERY STATEMENT IS IDEMPOTENT, because the migration may be interrupted between any two of them and a
	 * restart must pick up where it stopped rather than fail on "already done".
	 *
	 * THE BACKEND IS NOT DISPATCHED ON. `IF EXISTS` / `IF NOT EXISTS` make the same statements work on PostgreSQL and
	 * SQLite, which are the two this project runs on; MySQL's `DROP COLUMN IF EXISTS` also accepts this form since
	 * 8.0.
	 */
	stmts := d.groupIdentityMigrationStatements()
	for _, stmt := range stmts {
		if err := d.g.Exec(stmt).Error; err != nil {
			// REPORTED, NOT SWALLOWED. A half-applied migration leaves a database the new code cannot write to, and
			// the failure says which statement — a silent continue would surface later as "insert violates NOT NULL"
			// in a request handler.
			return fmt.Errorf("group identity migration: %w", err)
		}
	}
	return nil
}

/**
 * groupIdentityMigrationStatements is the DDL, per backend.
 *
 * THE TWO DIALECTS DIFFER IN WAYS THAT MATTER HERE and nowhere else in this project: SQLite has no
 * `ADD COLUMN IF NOT EXISTS` and no `md5()`, while PostgreSQL's `DROP CONSTRAINT IF EXISTS` is what makes a re-run
 * safe. Writing SQL that pretended they were the same would work on exactly one of them — and the one it did not
 * work on is the one that runs first in CI.
 */
func (d *DB) groupIdentityMigrationStatements() []string {
	if d.backend == SQLite {
		return []string{
			// SQLite assigns the id from the row's own rowid: unique, stable, and expressible without a function
			// PostgreSQL does not have.
			`UPDATE groups SET id = lower(hex(randomblob(16))) WHERE id IS NULL OR id = ''`,
			`UPDATE group_members SET group_id = (SELECT g.id FROM groups g
				WHERE g.tenant = group_members.tenant AND g.name = group_members."group")
				WHERE group_id = ''`,
			`UPDATE group_messages SET group_id = (SELECT g.id FROM groups g
				WHERE g.tenant = group_messages.tenant AND g.name = group_messages."group")
				WHERE group_id = ''`,
			// SQLite CANNOT ALTER A PRIMARY KEY, so the table is rebuilt. The columns being replaced are simply not
			// carried over — which is how SQLite expresses "drop a column in a key".
			`CREATE TABLE IF NOT EXISTS groups_new (tenant TEXT, id TEXT, name TEXT, avatar_file TEXT,
				intro TEXT, created_by TEXT, created_at TEXT, updated_at TEXT, PRIMARY KEY (tenant, id))`,
			`INSERT OR REPLACE INTO groups_new (tenant, id, name, avatar_file, intro, created_by, created_at, updated_at)
				SELECT tenant, id, name, avatar_file, intro, created_by, created_at, updated_at FROM groups`,
			`DROP TABLE groups`,
			`ALTER TABLE groups_new RENAME TO groups`,
			`CREATE TABLE IF NOT EXISTS group_members_new (tenant TEXT, group_id TEXT, session TEXT, role TEXT,
				joined_at TEXT, joined_by TEXT, PRIMARY KEY (tenant, group_id, session))`,
			`INSERT OR REPLACE INTO group_members_new (tenant, group_id, session, role, joined_at, joined_by)
				SELECT tenant, group_id, session, role, joined_at, joined_by FROM group_members`,
			`DROP TABLE group_members`,
			`ALTER TABLE group_members_new RENAME TO group_members`,
			`CREATE UNIQUE INDEX IF NOT EXISTS uq_groups_tenant_name ON groups (tenant, name)`,
		}
	}
	return []string{
		// AutoMigrate adds the new columns but cannot fill them, and on a table that already exists it also cannot
		// change the key. Both are this migration's job.
		`ALTER TABLE group_members ADD COLUMN IF NOT EXISTS group_id VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE group_messages ADD COLUMN IF NOT EXISTS group_id VARCHAR(64) NOT NULL DEFAULT ''`,

		// THE REWRITE. Random per row, which is all an identity needs; `md5(random())` is portable and does not
		// depend on a UUID extension being installed.
		`UPDATE groups SET id = md5(random()::text) WHERE id IS NULL OR id = ''`,
		`UPDATE group_members SET group_id = (SELECT g.id FROM groups g
			WHERE g.tenant = group_members.tenant AND g.name = group_members."group")
			WHERE group_id = ''`,
		`UPDATE group_messages SET group_id = (SELECT g.id FROM groups g
			WHERE g.tenant = group_messages.tenant AND g.name = group_messages."group")
			WHERE group_id = ''`,

		// THE KEY MOVES TO THE ID, which is what the old column was standing in for.
		`ALTER TABLE groups DROP CONSTRAINT IF EXISTS groups_pkey`,
		`ALTER TABLE groups ADD PRIMARY KEY (tenant, id)`,
		`ALTER TABLE group_members DROP CONSTRAINT IF EXISTS group_members_pkey`,
		`ALTER TABLE group_members ADD PRIMARY KEY (tenant, group_id, session)`,

		// A UNIQUE LABEL, so two groups cannot look the same. Not the identity — that is the id.
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_groups_tenant_name ON groups (tenant, name)`,

		// THE OLD COLUMNS GO LAST, once nothing needs them.
		`DROP INDEX IF EXISTS idx_group_messages_group`,
		`ALTER TABLE group_messages DROP COLUMN IF EXISTS "group"`,
		`ALTER TABLE group_members DROP COLUMN IF EXISTS "group"`,
	}
}

/**
 * isConcurrentSchemaChange reports whether an AutoMigrate error means "another replica got there first".
 *
 * THE THREE PHRASINGS ARE THE THREE BACKENDS, and they are matched rather than parsed because the drivers do not
 * expose typed errors for DDL races: SQLite says "already exists", Postgres says "already exists" with a
 * different prefix, and MySQL says "Duplicate column name" or "already exists". Matching the common tail keeps
 * this from depending on which database a deployment happens to use.
 *
 * EVERYTHING ELSE IS A REAL FAILURE and is returned immediately: a malformed definition or a missing privilege
 * must not be retried, because ten attempts will not change the answer.
 */
func isConcurrentSchemaChange(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"already exists",
		"duplicate column name",
		"duplicate key name",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

/**
 * backfillMessageSeq assigns positions to messages written before `seq` existed.
 *
 * AutoMigrate ADDS THE COLUMN AND NOTHING ELSE, so every existing row starts at seq 0 — and a
 * positional read that filters on seq would then see those messages as "before the beginning" and
 * skip them entirely. That is worse than an error: history would look SHORTER than it is.
 *
 * IT WALKS EACH CHAIN, because a position is a property of the chain and there is nowhere else to
 * read it from. Chains are found by their tips, so messages abandoned by an undo (reachable from no
 * tip) get no number — deliberately: they are not part of any conversation any more, and numbering
 * them would make the range include messages nothing can reach.
 *
 * It is a ONE-TIME cost gated on "any row still at seq 0", so a normal boot does one cheap count.
 */
func (d *DB) backfillMessageSeq() error {
	// NULL **OR** 0, and both are real: AutoMigrate adds the column as NULLABLE on PostgreSQL and
	// MySQL, so pre-existing rows arrive as NULL; SQLite fills a numeric default, so the same rows
	// arrive as 0. Checking only 0 skipped every row on the deployed database — the migration ran,
	// reported success, and changed nothing.
	var unnumbered int64
	if err := d.g.Model(&Message{}).Where("seq IS NULL OR seq = 0").Count(&unnumbered).Error; err != nil {
		return err
	}
	if unnumbered == 0 {
		return nil
	}

	var sessions []Session
	if err := d.g.Where("tip_id IS NOT NULL").Find(&sessions).Error; err != nil {
		return err
	}
	numbered := map[string]bool{}
	for _, s := range sessions {
		if s.TipID == nil || *s.TipID == "" {
			continue
		}
		// Walk the chain newest→oldest, collecting ids; Chain already returns oldest-first.
		msgs, err := d.Chain(s.Tenant, *s.TipID, maxChainDepth, "")
		if err != nil {
			return err
		}
		for i, m := range msgs {
			seq := int64(i + 1)
			if numbered[m.ID] || m.Seq == seq {
				numbered[m.ID] = true
				continue
			}
			if err := d.g.Model(&Message{}).Where("tenant = ? AND id = ?", m.Tenant, m.ID).
				Update("seq", seq).Error; err != nil {
				return err
			}
			numbered[m.ID] = true
		}
	}
	return nil
}

// nowStr is the canonical timestamp form (RFC3339, UTC) used by every column.
func nowStr() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// newID mints a random UUID-v4-shaped id.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	s := hex.EncodeToString(b[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}
