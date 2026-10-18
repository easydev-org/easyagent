package store

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// InsertMessage inserts a message with a CLIENT-supplied id, idempotently.
// Returns true when a row was created, false when the id already existed.
// Uses RowsAffected (portable) instead of `INSERT ... RETURNING`.
func (d *DB) InsertMessage(tenant, id, role string, prevID *string, source string) (bool, error) {
	m := Message{ID: id, Tenant: tenant, Role: role, PrevID: prevID, Source: source, CreatedAt: nowStr()}
	res := d.g.Clauses(clause.OnConflict{DoNothing: true}).Create(&m)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// GetMessage returns (message, true) or (nil, false).
func (d *DB) GetMessage(tenant, id string) (*Message, bool, error) {
	var m Message
	err := d.g.Where("tenant = ? AND id = ?", tenant, id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &m, true, nil
}

// ForkBase resolves the chain anchor a FORK should start from: when the tip is
// a `user` message it is the prompt that started the current turn, so the fork
// anchors at its PARENT (the child must not inherit that instruction).
func (d *DB) ForkBase(tenant, tipID string) (*string, error) {
	if tipID == "" {
		return nil, nil
	}
	m, ok, err := d.GetMessage(tenant, tipID)
	if err != nil || !ok {
		return nil, err
	}
	if m.Role == "user" {
		return m.PrevID, nil
	}
	return &tipID, nil
}

// IsInChain reports whether target is reachable by walking prev_id back from
// tip (i.e. target belongs to this session's chain). Bounded to guard against a
// corrupted (cyclic) link.
func (d *DB) IsInChain(tenant, tipID, targetID string) (bool, error) {
	if tipID == targetID {
		return true, nil
	}
	// One recursive query (never one SELECT per hop).
	return d.chainContains(tenant, tipID, targetID, maxChainDepth)
}

// maxChainDepth bounds a recursive walk (guards against a cyclic link).
const maxChainDepth = 100_000

// Chain walks the prev_id chain backwards from tip (or from before's
// predecessor), newest→oldest, up to limit, then returns oldest-first.
func (d *DB) Chain(tenant, tipID string, limit int, before string) ([]Message, error) {
	cursor := tipID
	if before != "" {
		m, ok, err := d.GetMessage(tenant, before)
		if err != nil {
			return nil, err
		}
		if !ok || m.PrevID == nil {
			return nil, nil
		}
		cursor = *m.PrevID
	}
	if cursor == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}
	// One recursive query (never one SELECT per hop).
	nodes, err := d.chainNodes(tenant, cursor, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.message())
		if len(out) >= limit {
			break
		}
	}
	reverse(out)
	return out, nil
}

// Delta is the result of an incremental chain read.
type Delta struct {
	Messages      []Message // oldest-first
	AnchorReached bool
	ReachedRoot   bool
}

// DeltaSince walks from tip back to (excluding) anchorID, returning the segment
// appended since the anchor. anchorReached=false + reachedRoot=true means the
// anchor is NOT on the current chain → the client must resync.
func (d *DB) DeltaSince(tenant, tipID, anchorID string, limit int) (Delta, error) {
	if tipID == "" {
		return Delta{ReachedRoot: true}, nil
	}
	if anchorID == tipID {
		return Delta{AnchorReached: true}, nil
	}
	if limit <= 0 {
		limit = 50
	}
	// One recursive query (never one SELECT per hop).
	nodes, err := d.chainNodes(tenant, tipID, limit+1)
	if err != nil {
		return Delta{}, err
	}
	out := make([]Message, 0, len(nodes))
	for _, n := range nodes {
		if n.ID == anchorID {
			reverse(out)
			return Delta{Messages: out, AnchorReached: true}, nil
		}
		out = append(out, n.message())
		if len(out) > limit {
			break
		}
	}
	reverse(out)
	return Delta{Messages: out, ReachedRoot: true}, nil
}

func reverse(ms []Message) {
	for i, j := 0, len(ms)-1; i < j; i, j = i+1, j-1 {
		ms[i], ms[j] = ms[j], ms[i]
	}
}
