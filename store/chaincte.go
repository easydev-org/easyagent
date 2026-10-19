package store

import (
	"fmt"

	"gorm.io/gorm"
)

// chainNode is one row of a recursive prev_id walk.
type chainNode struct {
	ID        string  `gorm:"column:id"`
	Tenant    string  `gorm:"column:tenant"`
	Role      string  `gorm:"column:role"`
	PrevID    *string `gorm:"column:prev_id"`
	Source    string  `gorm:"column:source"`
	CreatedAt string  `gorm:"column:created_at"`
	Depth     int     `gorm:"column:depth"`
}

// message converts a walked row back to the canonical Message.
func (n chainNode) message() Message {
	return Message{ID: n.ID, Tenant: n.Tenant, Role: n.Role, PrevID: n.PrevID, Source: n.Source, CreatedAt: n.CreatedAt}
}

// chainCTE walks the prev_id chain from one message toward the root in a SINGLE
// recursive query, newest-first, bounded by maxDepth. It replaces the previous
// one-SELECT-per-hop loop, which cost O(n) round-trips (the cause of a long
// pause after an undo: the chain had to be re-walked hop by hop).
//
// The statement is standard SQL: PostgreSQL, MySQL 8+, and SQLite all accept
// `WITH RECURSIVE`. `depth` is bounded so a corrupted (cyclic) link cannot loop
// forever.
const chainCTE = `WITH RECURSIVE chain(id, depth) AS (
  SELECT id, 0 FROM messages WHERE tenant = ? AND id = ?
  UNION ALL
  SELECT m.prev_id, c.depth + 1
  FROM chain c JOIN messages m ON m.id = c.id
  WHERE c.depth < ? AND m.prev_id IS NOT NULL
)
SELECT m.id, m.tenant, m.role, m.prev_id, m.source, m.created_at, c.depth
FROM chain c JOIN messages m ON m.id = c.id
ORDER BY c.depth ASC`

// chainNodes runs chainCTE. It uses a transaction so that, when MySQL needs its
// recursion depth raised, the SET applies to the same connection as the query.
func (d *DB) chainNodes(tenant, startID string, maxDepth int) ([]chainNode, error) {
	if startID == "" || maxDepth <= 0 {
		return nil, nil
	}
	var nodes []chainNode
	err := d.g.Transaction(func(tx *gorm.DB) error {
		// MySQL caps recursive CTEs at cte_max_recursion_depth (default 1000);
		// raise it for this session when the walk may go deeper.
		if d.backend == MySQL && maxDepth > 1000 {
			if e := tx.Exec(fmt.Sprintf("SET SESSION cte_max_recursion_depth = %d", maxDepth+1)).Error; e != nil {
				return e
			}
		}
		return tx.Raw(chainCTE, tenant, startID, maxDepth).Scan(&nodes).Error
	})
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// chainContains reports whether target is reachable from startID by following
// prev_id (one recursive query).
//
// Undo calls this to validate "is this message an ancestor of the tip?". It
// used to be `SELECT COUNT(*) FROM chain`, which forces the recursion to walk
// the WHOLE chain: a 1000-message session took ~300ms per undo. The outer
// `SELECT 1 ... LIMIT 1` lets the recursive CTE stop as soon as a produced row
// matches, which is the common case (an undo usually targets a recent message,
// i.e. the target sits near the tip). `maxDepth` still bounds a cyclic link.
func (d *DB) chainContains(tenant, startID, targetID string, maxDepth int) (bool, error) {
	const sqlText = `WITH RECURSIVE chain(id, depth) AS (
  SELECT id, 0 FROM messages WHERE tenant = ? AND id = ?
  UNION ALL
  SELECT m.prev_id, c.depth + 1
  FROM chain c JOIN messages m ON m.id = c.id
  WHERE c.depth < ? AND m.prev_id IS NOT NULL
)
SELECT 1 AS found FROM chain WHERE id = ? LIMIT 1`
	var found []int64
	err := d.g.Transaction(func(tx *gorm.DB) error {
		if d.backend == MySQL && maxDepth > 1000 {
			if e := tx.Exec(fmt.Sprintf("SET SESSION cte_max_recursion_depth = %d", maxDepth+1)).Error; e != nil {
				return e
			}
		}
		return tx.Raw(sqlText, tenant, startID, maxDepth, targetID).Scan(&found).Error
	})
	if err != nil {
		return false, err
	}
	return len(found) > 0, nil
}
