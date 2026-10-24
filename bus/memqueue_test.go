package bus

import (
	"sync"
	"sync/atomic"
)

// memQueue is a MailboxQueue that records what was enqueued, for tests that care about delivery
// rather than storage.
//
// Deliberately NOT a no-op: a PublishMailbox that signalled without enqueueing is the defect the
// queue interface exists to prevent, so a test double that silently accepts everything would let
// the same mistake back in.
type memQueue struct {
	mu    sync.Mutex
	rows  []string // "tenant/session/type"
	seq   int64
	fails error
}

func newMemQueue() *memQueue { return &memQueue{} }

func (q *memQueue) Enqueue(tenant, session, typ string, _ any, _ string) (string, error) {
	if q.fails != nil {
		return "", q.fails
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.rows = append(q.rows, tenant+"/"+session+"/"+typ)
	n := atomic.AddInt64(&q.seq, 1)
	return "row-" + itoa(n), nil
}

// count reports how many messages were enqueued.
func (q *memQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.rows)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
