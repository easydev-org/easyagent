package store

import (
	"errors"
	"log/slog"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SessionInput are the fields accepted at creation.
type SessionInput struct {
	Name         string
	Model        string
	Variant      string
	Preset       string
	TipID        *string
	SystemPrompt string
	MaxTurns     int
	Locale       string
	Group        string
}

// CreateSession inserts a session row.
func (d *DB) CreateSession(tenant string, in SessionInput) error {
	// EVERY session carries a preset: the UI resolves the system prompt from it,
	// so an empty preset means "no system prompt". This is the single choke
	// point for session creation (create / fork / rename / subsession), so
	// defaulting here guarantees the invariant for all of them — including a
	// fork of a legacy row that was itself stored without a preset.
	if in.Preset == "" {
		in.Preset = DefaultPreset
	}
	now := nowStr()
	s := Session{
		Tenant: tenant, Name: in.Name, Model: in.Model, Variant: in.Variant,
		Preset: in.Preset, TipID: in.TipID, MaxTurns: in.MaxTurns,
		SystemPrompt: in.SystemPrompt, Locale: in.Locale, Group: in.Group,
		CreatedAt: now, UpdatedAt: now,
	}
	return d.g.Create(&s).Error
}

// GetSession returns (session, true) or (nil, false).
func (d *DB) GetSession(tenant, name string) (*Session, bool, error) {
	var s Session
	err := d.g.Where("tenant = ? AND name = ?", tenant, name).First(&s).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &s, true, nil
}

// ListSessions returns a tenant's sessions, most recently updated first.
func (d *DB) ListSessions(tenant string) ([]Session, error) {
	var out []Session
	err := d.g.Where("tenant = ?", tenant).Order("updated_at DESC").Find(&out).Error
	return out, err
}

// SessionExists reports whether (tenant, name) exists.
func (d *DB) SessionExists(tenant, name string) (bool, error) {
	var n int64
	err := d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).Count(&n).Error
	return n > 0, err
}

// DeleteSession removes a session and its private mailbox queue. Message
// history is NOT touched (messages are append-only and shared by forks).
func (d *DB) DeleteSession(tenant, name string) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant = ? AND session_name = ?", tenant, name).Delete(&Mailbox{}).Error; err != nil {
			return err
		}
		return tx.Where("tenant = ? AND name = ?", tenant, name).Delete(&Session{}).Error
	})
}

// SessionTip returns the session's chain tip id ("" when none).
func (d *DB) SessionTip(tenant, name string) (string, error) {
	var s struct{ TipID *string }
	err := d.g.Model(&Session{}).Select("tip_id").
		Where("tenant = ? AND name = ?", tenant, name).Take(&s).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if s.TipID == nil {
		return "", nil
	}
	return *s.TipID, nil
}

// SetTip moves the chain tip WITHOUT bumping message_seq (an undo/withdraw is
// not a new message).
//
// It returns the `updated_at` it wrote, so a caller that already holds the row
// can patch it in memory instead of re-reading the row (undo used to issue a
// second GetSession purely to return the refreshed session to the client).
func (d *DB) SetTip(tenant, name string, tipID *string) (updatedAt string, err error) {
	updatedAt = nowStr()
	err = d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{"tip_id": tipID, "updated_at": updatedAt}).Error
	return updatedAt, err
}

// AppendMessageTip atomically sets tip_id AND bumps message_seq, returning the
// NEW counter. Two-phase (read-modify-write under a row lock) so it needs no
// `RETURNING`.
func (d *DB) AppendMessageTip(tenant, name, messageID string) (int64, error) {
	var seq int64
	err := d.g.Transaction(func(tx *gorm.DB) error {
		var s Session
		q := tx.Where("tenant = ? AND name = ?", tenant, name)
		if tx.Dialector.Name() != "sqlite" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := q.First(&s).Error; err != nil {
			return err
		}
		// Trace the tip transitions: "the tip is stuck on an old message" has several
		// possible causes and this says which one it is.
		prev := ""
		if s.TipID != nil {
			prev = *s.TipID
		}
		slog.Debug("append message tip", "tenant", tenant, "session", name,
			"from", prev, "to", messageID, "seq", s.MessageSeq)
		// message_seq is the CHAIN LENGTH, not a call counter.
		//
		// It is compared against read_seq to make the unread badge, so it has to
		// track how many messages the session actually holds. Incrementing per call
		// drifted in both directions: one step emits several writes while adding a
		// single message (so the counter ran ahead), and re-setting the same tip
		// added nothing (so two runs disagreed about the same session). Counting the
		// chain is the definition the badge needs.
		n, cerr := d.chainLength(tx, tenant, messageID)
		if cerr != nil {
			return cerr
		}
		seq = n
		return tx.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
			Updates(map[string]any{"tip_id": messageID, "message_seq": seq, "updated_at": nowStr()}).Error
	})
	return seq, err
}

// chainLength counts the messages reachable from tipID by following prev_id.
//
// Walks the same links the reader does, so the number it returns is exactly what
// the client will be shown — no separate bookkeeping to drift out of step.
func (d *DB) chainLength(tx *gorm.DB, tenant, tipID string) (int64, error) {
	if tipID == "" {
		return 0, nil
	}
	var n int64
	cur := tipID
	seen := map[string]bool{}
	for cur != "" && !seen[cur] && int64(len(seen)) < 10000 {
		seen[cur] = true
		var row struct{ PrevID *string }
		if err := tx.Model(&Message{}).Select("prev_id").
			Where("tenant = ? AND id = ?", tenant, cur).Take(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				break
			}
			return 0, err
		}
		n++
		if row.PrevID == nil {
			break
		}
		cur = *row.PrevID
	}
	return n, nil
}

// MarkRead advances the shared read watermark to at least seq, clamped to the
// current message_seq. Monotonic: an older value never lowers it.
func (d *DB) MarkRead(tenant, name string, seq int64) (int64, error) {
	var out int64
	err := d.g.Transaction(func(tx *gorm.DB) error {
		var s Session
		q := tx.Where("tenant = ? AND name = ?", tenant, name)
		if tx.Dialector.Name() != "sqlite" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := q.First(&s).Error; err != nil {
			return err
		}
		v := s.ReadSeq
		// seq == 0 means "mark everything read", not "do nothing".
		//
		// The console calls MarkRead with no sequence, and the old code treated 0 as
		// a value to compare: 0 > ReadSeq is never true, so read_seq never moved and
		// the unread badge could not be cleared by opening a session. The API
		// documents "seq omitted => the current tip", so honour it.
		if seq > v {
			v = seq
		} else if seq <= 0 {
			v = s.MessageSeq
		}
		if v > s.MessageSeq {
			v = s.MessageSeq
		}
		out = v
		return tx.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
			Updates(map[string]any{"read_seq": v, "updated_at": nowStr()}).Error
	})
	return out, err
}

// AddUsage records the LAST step's token usage (overwritten, not accumulated).
func (d *DB) AddUsage(tenant, name string, in, out int64) error {
	return d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{
			"last_input_tokens": in, "last_output_tokens": out, "last_used_at": nowStr(),
		}).Error
}

// UpdateSessionSettings patches the mutable settings (nil = leave unchanged).
func (d *DB) UpdateSessionSettings(tenant, name string, patch map[string]any) error {
	if len(patch) == 0 {
		return nil
	}
	patch["updated_at"] = nowStr()
	return d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(patch).Error
}

// Children returns sessions sharing a `group` value (a parent's subsessions).
func (d *DB) Children(tenant, group string) ([]Session, error) {
	var out []Session
	err := d.g.Where("tenant = ? AND \"group\" = ?", tenant, group).Order("created_at").Find(&out).Error
	return out, err
}

// SetSessionPreview updates the list-row preview projection.
func (d *DB) SetSessionPreview(tenant, name, preview string) error {
	now := nowStr()
	return d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{"last_message_preview": preview, "last_message_at": now}).Error
}

// SetTurnEnd records why (and when) the last turn ended.
func (d *DB) SetTurnEnd(tenant, name, reason string) error {
	now := nowStr()
	return d.g.Model(&Session{}).Where("tenant = ? AND name = ?", tenant, name).
		Updates(map[string]any{"last_turn_reason": reason, "last_turn_at": now}).Error
}

// KnownTenants lists every tenant id present in the DB (sessions ∪ providers ∪
// tenants). There is NO implicit/bootstrap tenant: every tenant is created
// explicitly via AdminService.CreateTenant.
func (d *DB) KnownTenants() ([]string, error) {
	set := map[string]bool{}
	collect := func(table any, col string) error {
		var rows []map[string]any
		if err := d.g.Model(table).Select("DISTINCT " + col).Find(&rows).Error; err != nil {
			return err
		}
		for _, r := range rows {
			if v, ok := r[col].(string); ok && v != "" {
				set[v] = true
			}
		}
		return nil
	}
	if err := collect(&Session{}, "tenant"); err != nil {
		return nil, err
	}
	if err := collect(&Provider{}, "tenant"); err != nil {
		return nil, err
	}
	if err := collect(&Tenant{}, "id"); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out, nil
}
