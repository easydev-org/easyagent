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
	/**
	 * THE OLD GROUP TABLES ARE DROPPED, NOT MIGRATED.
	 *
	 * Groups used to be keyed by their name, and the new shape keys them by an id. Reconciling that in place means
	 * rewriting membership rows, swapping primary keys and dropping columns whose invariants the old schema still
	 * enforces — a migration whose only purpose is to preserve data from a build that was NEVER RELEASED.
	 *
	 * So the tables are recreated. It costs a few test groups and buys a schema that says what it means. Once this
	 * ships to anyone, the next change to these tables needs a real migration; this one does not, because there is
	 * nothing to preserve.
	 */
	if err := d.dropLegacyGroupTables(); err != nil {
		return err
	}
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
	return d.backfillMessageSeq()
}

/**
 * dropLegacyGroupTables removes group tables built with the name-as-identity schema.
 *
 * THE DETECTOR IS THE OLD COLUMN, not a version number: this project keeps no migration ledger, and
 * `group_members."group"` exists only in the old shape. A fresh database has no such table, so the drop is skipped.
 *
 * AutoMigrate recreates all three tables from the models afterwards, which is the whole point — the models ARE the
 * new schema.
 */
func (d *DB) dropLegacyGroupTables() error {
	if !d.g.Migrator().HasTable("group_members") {
		return nil
	}
	if !d.g.Migrator().HasColumn("group_members", "group") {
		return nil
	}
	// MEMBERSHIPS FIRST: they reference groups, and a backend with foreign keys would refuse the other order.
	for _, t := range []string{"group_messages", "group_members", "groups"} {
		if err := d.g.Migrator().DropTable(t); err != nil {
			return fmt.Errorf("drop legacy %s: %w", t, err)
		}
	}
	return nil
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
