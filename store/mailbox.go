package store

import "gorm.io/gorm"

// Enqueue appends a mailbox row (mints an id).
func (d *DB) Enqueue(tenant, session, msgType string, payload any, source string) (string, error) {
	id := newID()
	if err := d.EnqueueWithID(tenant, id, session, msgType, payload, source); err != nil {
		return "", err
	}
	return id, nil
}

// EnqueueWithID inserts with a producer-supplied id, ignoring a duplicate (a
// redelivery carries the same id → the second insert is a no-op).
func (d *DB) EnqueueWithID(tenant, id, session, msgType string, payload any, source string) error {
	raw, err := jsonBytes(payload)
	if err != nil {
		return err
	}
	m := Mailbox{
		ID: id, Tenant: tenant, SessionName: session, MsgType: msgType,
		Source: source, Payload: raw, Status: "pending", CreatedAt: nowStr(),
	}
	return d.g.Clauses(onConflictDoNothing()).Create(&m).Error
}

// DrainAll atomically pops EVERY deliverable pending item for one tenant+session
// in a TWO-PHASE operation (no `RETURNING`):
//
//  1. UPDATE ... SET status='consumed', consumed_by=<claim> WHERE pending
//  2. SELECT ... WHERE consumed_by=<claim>
//
// The per-session run lease guarantees a single writer, so no SKIP LOCKED is
// needed. Rows are returned oldest-first.
func (d *DB) DrainAll(tenant, session string) ([]Mailbox, error) {
	claim := newID()
	now := nowStr()
	var rows []Mailbox
	err := d.g.Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&Mailbox{}).
			Where("tenant = ? AND session_name = ? AND status = 'pending' AND (effective_at IS NULL OR effective_at <= ?)",
				tenant, session, now).
			Updates(map[string]any{"status": "consumed", "consumed_at": now, "consumed_by": claim})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil
		}
		return tx.Where("tenant = ? AND session_name = ? AND consumed_by = ?", tenant, session, claim).
			Order("created_at ASC").Find(&rows).Error
	})
	return rows, err
}

// HasPending reports whether any deliverable pending row exists (non-consuming).
func (d *DB) HasPending(tenant, session string) (bool, error) {
	now := nowStr()
	var n int64
	err := d.g.Model(&Mailbox{}).
		Where("tenant = ? AND session_name = ? AND status = 'pending' AND (effective_at IS NULL OR effective_at <= ?)",
			tenant, session, now).
		Limit(1).Count(&n).Error
	return n > 0, err
}

// HasPendingInterrupt reports whether an `interrupt` row is pending.
func (d *DB) HasPendingInterrupt(tenant, session string) (bool, error) {
	var n int64
	err := d.g.Model(&Mailbox{}).
		Where("tenant = ? AND session_name = ? AND msg_type = 'interrupt' AND status = 'pending'", tenant, session).
		Limit(1).Count(&n).Error
	return n > 0, err
}

// PendingSessions lists sessions with pending items, optionally narrowed to the
// message types that actually WAKE a turn (trigger/compact).
func (d *DB) PendingSessions(types []string) ([]struct{ Tenant, SessionName string }, error) {
	q := d.g.Model(&Mailbox{}).Distinct("tenant, session_name").Where("status = 'pending'")
	if len(types) > 0 {
		q = q.Where("msg_type IN ?", types)
	}
	var out []struct{ Tenant, SessionName string }
	err := q.Scan(&out).Error
	return out, err
}

// ListMailboxPage returns a NEWEST-FIRST page (older than `before`) plus
// hasMore. Ordering is (created_at DESC, id DESC) — a stable total order.
func (d *DB) ListMailboxPage(tenant, session string, limit int, before string) ([]Mailbox, bool, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	take := limit + 1
	q := d.g.Where("tenant = ? AND session_name = ?", tenant, session)
	if before != "" {
		var anchor Mailbox
		if err := d.g.Where("tenant = ? AND id = ?", tenant, before).First(&anchor).Error; err == nil {
			q = q.Where("(created_at < ? OR (created_at = ? AND id < ?))",
				anchor.CreatedAt, anchor.CreatedAt, anchor.ID)
		}
	}
	var rows []Mailbox
	if err := q.Order("created_at DESC, id DESC").Limit(take).Find(&rows).Error; err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

// PurgeConsumed deletes consumed rows older than the retention window (days).
func (d *DB) PurgeConsumed(retentionDays int) (int64, error) {
	cutoff := cutoffStr(retentionDays)
	res := d.g.Where("status = 'consumed' AND consumed_at IS NOT NULL AND consumed_at < ?", cutoff).
		Delete(&Mailbox{})
	return res.RowsAffected, res.Error
}
