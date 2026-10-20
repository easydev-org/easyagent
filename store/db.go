package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
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

// Migrate creates the INITIAL schema (create-if-absent). It is not a migration
// path: there is no legacy schema to upgrade.
func (d *DB) Migrate() error {
	if err := d.g.AutoMigrate(
		&Session{}, &Message{}, &Part{}, &Mailbox{},
		&Provider{}, &Tenant{}, &TenantToken{}, &AgentFile{},
		&BundledTodo{}, &AdminToken{}, &SessionToken{},
	); err != nil {
		return err
	}
	// Backfill: every pre-existing tenant becomes its own subtree root
	// (scope_id = id, parent_id = NULL). Idempotent.
	return d.g.Model(&Tenant{}).Where("scope_id = ? OR scope_id IS NULL", "").
		Update("scope_id", gorm.Expr("id")).Error
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
