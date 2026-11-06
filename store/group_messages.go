package store

import (
	"errors"
	"strings"

	"gorm.io/gorm"
)

/**
 * GroupMessage is one message posted INTO a group.
 *
 * A GROUP HAS ITS OWN TRANSCRIPT, separate from any member's chain. This is the difference between "a
 * group is a directory" and "a group is a place where things are said": with its own transcript, a member
 * that joins later can read what was said before it arrived, a reader can see the conversation without
 * opening ten sessions, and a message survives the departure of every session that saw it.
 *
 * WHY NOT REUSE `messages`. That table is a prev_id CHAIN with a per-session `seq`, because a session's
 * history is a tree of forks. A group is not a chain: messages arrive from several senders, there is no
 * "before" to fork from, and the natural order is arrival. Reusing the chain would mean two different
 * meanings for the same columns, and every reader would have to know which one it was looking at.
 *
 * `Seq` IS A MONOTONIC COUNTER PER GROUP, assigned on append. It is the resume cursor for a watch and the
 * sort key for a page; unlike a session's `seq` (a position in a chain that forks), a group's sequence is
 * strictly increasing and never reused.
 */
type GroupMessage struct {
	ID     string `gorm:"column:id;primaryKey;size:64"`
	Tenant string `gorm:"column:tenant;size:64;index:idx_group_messages_group,priority:1"`
	// GroupID is the group's immutable identity. It used to be the group's NAME, which meant a rename orphaned the
	// transcript: the rows would still exist, under a name no group has any more.
	GroupID string `gorm:"column:group_id;size:64;index:idx_group_messages_group,priority:2"`

	// Sender is the originating session's id, or empty when a USER posted it.
	//
	// EMPTY MEANS THE USER, and that is a real distinction rather than a missing value: the console renders
	// a user's message differently from a coordinator's. `SenderName` is the label AT THE TIME OF SENDING,
	// kept because a session may be renamed later and the transcript should read as it did.
	Sender     string `gorm:"column:sender;size:64"`
	SenderName string `gorm:"column:sender_name;size:128"`

	/**
	 * Kind is "user" | "session" | "system", and it says WHO WROTE THIS.
	 *
	 * EXPLICIT RATHER THAN DERIVED FROM `Sender`. "Empty sender means the user" was true while there were only two
	 * authors; a system reminder is a third, and reusing the empty convention would have rendered the platform's
	 * notice as the person's own words — the reader chasing their own worker.
	 */
	Kind string `gorm:"column:kind;size:16;index"`

	// Body is the message text.
	/**
	 * Kind is WHO WROTE IT, and Intent is WHAT IT IS: a request or a reply.
	 *
	 * TWO AXES, NOT ONE. "who" is about attribution (the console draws it differently), "what" is about the work: a
	 * request EXPECTS AN ANSWER and a reply IS one. Folding them together was the mistake — a reply is written by the
	 * same session that can write a request, so "who" cannot tell you whether anyone is waiting.
	 */
	Intent string `gorm:"column:intent;size:16;index"`

	Body string `gorm:"column:body;type:text"`

	/**
	 * Attachments is the message's files, as a JSON array of `{code,name,size}`.
	 *
	 * AN ATTACHMENT IS A `file:<code>` REFERENCE, nothing more: the bytes live once in the tenant's blob
	 * store and the message carries the reference, exactly as a session message's `file` part does. Copying
	 * bytes per message would duplicate a recording for every member who reads it.
	 *
	 * A JSON COLUMN RATHER THAN A SECOND TABLE, because a reference belongs to exactly one message and is
	 * never queried on its own — a join table would add a migration, a foreign key and a load step to answer
	 * a question no caller asks ("which messages mention this blob").
	 *
	 * EMPTY STRING MEANS NO ATTACHMENTS, which is also what every row written before this column existed
	 * holds — so no backfill is needed and an old message reads exactly as it did.
	 */
	Attachments string `gorm:"column:attachments;type:text"`

	/**
	 * Seq orders messages within one group, starting at 1.
	 *
	 * ASSIGNED INSIDE THE APPEND TRANSACTION, so two concurrent posts cannot share a number. It is the
	 * resume cursor for WatchGroup (a client passes the newest it has) and the sort key for a page.
	 */
	Seq       int64  `gorm:"column:seq;index:idx_group_messages_group,priority:3"`
	CreatedAt string `gorm:"column:created_at"`
}

func (GroupMessage) TableName() string { return "group_messages" }

/**
 * The three authors a group message can have.
 *
 * USER IS THE ONE WITH NO SESSION BEHIND IT — a person typing into the console. SESSION is one member addressing
 * another. SYSTEM is the platform itself, which today means a reminder that a reply is still owed.
 *
 * THEY ARE CONSTANTS RATHER THAN LITERALS because the console branches on them, and a typo would render the platform's
 * notice as somebody's words.
 */
const (
	GroupMessageKindUser    = "user"
	GroupMessageKindSession = "session"
	GroupMessageKindSystem  = "system"
)

/**
 * The two things a session's group message can BE.
 *
 * A REQUEST OPENS A DEBT: the addressee owes an answer, and the group page can say who has not given one yet.
 * A REPLY CLOSES ONE: it is the answer, and by definition asks nothing further.
 *
 * THE SENDER DECLARES IT. Inferring it — "this one settled something, so it must be a reply" — cannot work for the
 * first request, and made a reply that happened to answer nothing indistinguishable from a new ask. The ping-pong
 * that produced on the first live run is what this parameter exists to prevent.
 *
 * EMPTY MEANS "NO OPINION", which is every message a person posts and every system notice: neither owes nor settles
 * anything.
 */
const (
	GroupIntentRequest = "request"
	GroupIntentReply   = "reply"
)

/**
 * KindOf derives the author kind for a message written without one.
 *
 * FOR ROWS THAT PREDATE THE COLUMN, and for callers that set only a sender. An empty sender is a user, because that is
 * what it meant before there were three authors.
 */
func KindOf(kind, sender string) string {
	if kind != "" {
		return kind
	}
	if sender == "" {
		return GroupMessageKindUser
	}
	return GroupMessageKindSession
}

// ErrGroupMessageNotFound is returned when a message does not exist in the given group.
var ErrGroupMessageNotFound = errors.New("group message not found")

/**
 * AppendGroupMessage stores one message and assigns its sequence.
 *
 * IT IS A TRANSACTION BECAUSE THE SEQUENCE DEPENDS ON WHAT IS ALREADY THERE. Two concurrent appends that
 * both read "highest is 7" would both write 8, and a watch resuming from 8 would then miss one of them.
 * The transaction reads the maximum and inserts within the same statement group, so the database decides
 * the order.
 */
func (d *DB) AppendGroupMessage(m GroupMessage, now string) (GroupMessage, error) {
	err := d.g.Transaction(func(tx *gorm.DB) error {
		var highest int64
		if err := tx.Model(&GroupMessage{}).
			Where("tenant = ? AND group_id = ?", m.Tenant, m.GroupID).
			Select("COALESCE(MAX(seq), 0)").Scan(&highest).Error; err != nil {
			return err
		}
		m.Seq = highest + 1
		m.CreatedAt = now
		return tx.Create(&m).Error
	})
	if err != nil {
		return GroupMessage{}, err
	}
	return m, nil
}

// ListGroupMessages returns a page of a group's messages in ASCENDING sequence order.
//
// `afterSeq` is EXCLUSIVE, so a caller that holds message 12 asks for messages after 12 and receives 13
// onward. `limit <= 0` means the server's default page; a caller wanting everything should page rather than
// ask for an unbounded scan, because a group's transcript has no natural cap.
func (d *DB) ListGroupMessages(tenant, groupID string, afterSeq int64, limit int) ([]GroupMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []GroupMessage
	err := d.g.Where("tenant = ? AND group_id = ? AND seq > ?", tenant, groupID, afterSeq).
		Order("seq").Limit(limit).Find(&out).Error
	return out, err
}

/**
 * GroupTranscriptPreview answers the two things a list row shows: the newest message's words and when.
 *
 * A GROUP ROW HAD NO PREVIEW because a group's messages are not a session's chain, so the projection a session row
 * uses (`last_message_preview`) never applied. The row showed a member count and nothing else, while a session
 * row beside it showed what was last said — the one piece of information a reader scans a list for.
 *
 * ONE ROW, not a page: the list is rendered for every group at once, and reading a hundred messages to show the
 * last one would cost a hundred times what it answers.
 *
 * PREFERS A MESSAGE WITH WORDS. A group whose last message is a picture would otherwise render an empty
 * preview, which reads as "nothing was said" rather than "something was sent that has no words".
 */
func (d *DB) GroupTranscriptPreview(tenant, groupID string) (text string, at string, err error) {
	var rows []GroupMessage
	if e := d.g.Where("tenant = ? AND group_id = ?", tenant, groupID).
		Order("seq DESC").Limit(5).Find(&rows).Error; e != nil {
		return "", "", e
	}
	for _, m := range rows {
		if strings.TrimSpace(m.Body) != "" {
			return m.Body, m.CreatedAt, nil
		}
	}
	if len(rows) > 0 {
		// Every recent message is attachment-only: report the newest anyway, with an empty body. The caller
		// decides how to word "something was sent" — that is a presentation question, not a storage one.
		return "", rows[0].CreatedAt, nil
	}
	return "", "", nil
}

// LatestGroupSeq returns the highest sequence in a group (0 when empty).
//
// Used by a watch that starts without a cursor: the client is told where "now" is, so it does not receive
// the whole transcript before it can begin.
func (d *DB) LatestGroupSeq(tenant, groupID string) (int64, error) {
	var highest int64
	err := d.g.Model(&GroupMessage{}).
		Where("tenant = ? AND group_id = ?", tenant, groupID).
		Select("COALESCE(MAX(seq), 0)").Scan(&highest).Error
	return highest, err
}

// GroupMessageCount returns how many messages a group holds.
func (d *DB) GroupMessageCount(tenant, groupID string) (int64, error) {
	var n int64
	err := d.g.Model(&GroupMessage{}).
		Where("tenant = ? AND group_id = ?", tenant, groupID).Count(&n).Error
	return n, err
}

// DeleteGroupMessages removes a group's transcript (called when the group is deleted).
//
// A DELETED GROUP'S MESSAGES MUST NOT OUTLIVE IT: the rows would be unreachable through any RPC and would
// accumulate forever, and a later group with the same name would inherit a stranger's conversation.
func (d *DB) DeleteGroupMessages(tenant, groupID string) error {
	return d.g.Where("tenant = ? AND group_id = ?", tenant, groupID).
		Delete(&GroupMessage{}).Error
}
