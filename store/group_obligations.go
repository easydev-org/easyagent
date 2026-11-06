package store

import (
	"errors"
	"time"

	"gorm.io/gorm"
)

/**
 * GroupReplyObligation is a session's OUTSTANDING DEBT: one reply, in one group, to the session that addressed it.
 *
 * THE OBLIGATION IS THE FEATURE, not the reminder. `group-send` with a `to` list hands someone work, and until this
 * existed nothing recorded that the work had been acknowledged — the sender could not tell "it is thinking" from "it
 * finished and forgot". The reminder machinery below is a consequence of having this row; without the row there is
 * nothing to remind about.
 *
 * ONE ROW PER (group, addressee, message). Being named twice in one message is one debt; being named in two messages
 * is two, because the sender asked twice.
 *
 * FULFILLED IS A TIMESTAMP RATHER THAN A DELETION. A group page answering "did it reply" is asking a question about
 * the past, and a deleted row cannot answer it. It also keeps the reminder count honest for an obligation that was
 * chased and then satisfied.
 */
type GroupReplyObligation struct {
	ID      string `gorm:"column:id;primaryKey;size:64"`
	Tenant  string `gorm:"column:tenant;primaryKey;size:64"`
	GroupID string `gorm:"column:group_id;size:64;index:idx_group_obligations_group,priority:2"`
	// Session is the session that OWES the reply — the one named in `to`.
	Session string `gorm:"column:session;size:64;index:idx_group_obligations_session,priority:2"`
	// From is the session that asked, and the one a reply must be addressed to.
	From string `gorm:"column:from_session;size:64"`
	// Seq is the group message that created the debt. It identifies WHICH ask is outstanding when there are several.
	Seq int64 `gorm:"column:seq"`
	// MessageID is that message's id, for a reader that wants to link to it.
	MessageID string `gorm:"column:message_id;size:64"`

	CreatedAt string `gorm:"column:created_at"`
	// RemindedAt is the last time it was chased. Empty = never.
	RemindedAt string `gorm:"column:reminded_at"`
	// RemindCount is how many times. Bounded by the caller, which decides when to stop.
	RemindCount int `gorm:"column:remind_count"`
	/**
	 * FulfilledAt is when a reply landed. EMPTY MEANS STILL OUTSTANDING.
	 *
	 * A POINTER WOULD ALSO WORK, but an empty string keeps this column comparable in SQL without NULL semantics —
	 * "fulfilled_at = ''" is the outstanding predicate, and it reads the same on every backend.
	 */
	FulfilledAt string `gorm:"column:fulfilled_at"`
}

func (GroupReplyObligation) TableName() string { return "group_reply_obligations" }

// ErrNoObligation is returned when marking one that does not exist.
var ErrNoObligation = errors.New("no such reply obligation")

// newObligationID mints an opaque id.
func newObligationID() string { return "ob-" + newID() }

/**
 * OpenReplyObligations records that each session owes a reply to `from` for one message.
 *
 * IDEMPOTENT ON (group, session, message): re-sending the same ask must not open a second debt. That matters because
 * the caller is a tool call, and a model that retries after a timeout would otherwise double the debt it is chasing.
 *
 * IT DOES NOT LOOK FOR AN EXISTING FULFILMENT. A session named again in a NEW message owes a NEW reply, even if it
 * answered the previous one — the sender asked again.
 */
func (d *DB) OpenReplyObligations(tenant, groupID, from string, sessions []string, seq int64, messageID, now string) error {
	if len(sessions) == 0 {
		return nil
	}
	return d.g.Transaction(func(tx *gorm.DB) error {
		for _, s := range sessions {
			if s == "" || s == from {
				// A SESSION NEVER OWES ITSELF A REPLY. `group-send` already refuses self-addressing; this is the same
				// rule at the storage layer, so a caller that skipped the tool cannot create a permanent debt.
				continue
			}
			var n int64
			if err := tx.Model(&GroupReplyObligation{}).
				Where("tenant = ? AND group_id = ? AND session = ? AND seq = ?", tenant, groupID, s, seq).
				Count(&n).Error; err != nil {
				return err
			}
			if n > 0 {
				continue
			}
			if err := tx.Create(&GroupReplyObligation{
				ID: newObligationID(), Tenant: tenant, GroupID: groupID, Session: s,
				From: from, Seq: seq, MessageID: messageID, CreatedAt: now,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

/**
 * FulfilReplyObligations settles every outstanding debt one session owed to one sender in one group.
 *
 * ALL OF THEM, not the oldest: a session that answers "here is the result" has answered the asks that preceded it, and
 * leaving older rows open would make the group page show a debt the reply plainly settled.
 *
 * THE ADDRESSING IS THE RULE. `to` must name the session that asked — the answer to "must reply once" is only
 * meaningful if a reply is distinguishable from chatter, and the distinction is who it is addressed to.
 */
func (d *DB) FulfilReplyObligations(tenant, groupID, session, to, now string) (int64, error) {
	res := d.g.Model(&GroupReplyObligation{}).
		Where("tenant = ? AND group_id = ? AND session = ? AND from_session = ? AND fulfilled_at = ?",
			tenant, groupID, session, to, "").
		Update("fulfilled_at", now)
	return res.RowsAffected, res.Error
}

/**
 * OutstandingObligations returns the debts a session still owes, oldest first.
 *
 * USED AT THE MOMENT A TURN ENDS, which is the only point where "it finished and did not answer" is knowable.
 */
func (d *DB) OutstandingObligations(tenant, session string) ([]GroupReplyObligation, error) {
	var rows []GroupReplyObligation
	err := d.g.Where("tenant = ? AND session = ? AND fulfilled_at = ?", tenant, session, "").
		Order("created_at").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

/**
 * ObligationsDueForReminder returns the outstanding debts a session still owes that are worth chasing.
 *
 * `maxReminders` BOUNDS IT. An obligation at the limit is not returned, so a session that will not answer stops being
 * chased — the row stays open and visible to a reader, which is the honest end state: "it was asked, it did not reply,
 * and the system said so three times".
 */
func (d *DB) ObligationsDueForReminder(tenant, session string, maxReminders int) ([]GroupReplyObligation, error) {
	var rows []GroupReplyObligation
	err := d.g.Where("tenant = ? AND session = ? AND fulfilled_at = ? AND remind_count < ?",
		tenant, session, "", maxReminders).
		Order("created_at").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// BumpReminder records one chase, returning the new count.
func (d *DB) BumpReminder(tenant, id, now string) (int, error) {
	res := d.g.Model(&GroupReplyObligation{}).
		Where("tenant = ? AND id = ?", tenant, id).
		Updates(map[string]any{
			"reminded_at":  now,
			"remind_count": gorm.Expr("remind_count + 1"),
		})
	if res.Error != nil {
		return 0, res.Error
	}
	if res.RowsAffected == 0 {
		return 0, ErrNoObligation
	}
	var row GroupReplyObligation
	if err := d.g.Where("tenant = ? AND id = ?", tenant, id).First(&row).Error; err != nil {
		return 0, err
	}
	return row.RemindCount, nil
}

/**
 * DropObligationsOf removes every debt a session owes, in every group.
 *
 * CALLED WHEN THE SESSION LEAVES. A deleted session cannot answer, so keeping its debts would leave a group forever
 * showing a debt nobody can settle — the same failure as a member list advertising a session that is gone.
 */
func (d *DB) DropObligationsOf(tenant, session string) error {
	return d.g.Where("tenant = ? AND session = ? AND fulfilled_at = ?", tenant, session, "").
		Delete(&GroupReplyObligation{}).Error
}

/**
 * DropObligationsInGroup removes the debts one session owes in one group.
 *
 * ON REMOVAL FROM THE GROUP, for the same reason: leaving the group ends every ask inside it.
 */
func (d *DB) DropObligationsInGroup(tenant, groupID, session string) error {
	return d.g.Where("tenant = ? AND group_id = ? AND session = ? AND fulfilled_at = ?",
		tenant, groupID, session, "").
		Delete(&GroupReplyObligation{}).Error
}

/**
 * ObligationsForGroup returns a group's outstanding debts, for the page that shows them.
 *
 * OUTSTANDING ONLY: a reader asking "who still owes a reply" is not asking about the history of settled ones.
 */
func (d *DB) ObligationsForGroup(tenant, groupID string) ([]GroupReplyObligation, error) {
	var rows []GroupReplyObligation
	err := d.g.Where("tenant = ? AND group_id = ? AND fulfilled_at = ?", tenant, groupID, "").
		Order("created_at").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

var _ = time.Now
