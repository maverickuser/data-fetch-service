package acquisition

import (
	"context"
	"fmt"
	"sync"
)

// Budget limits aggregate live temporary-file bytes across concurrent jobs.
type Budget struct {
	mu             sync.Mutex
	capacity, used int64
	changed        chan struct{}
}

// NewBudget constructs a byte reservation pool; invalid capacity rejects reservations.
func NewBudget(capacity int64) *Budget {
	return &Budget{capacity: capacity, changed: make(chan struct{})}
}

// Acquire waits cancellably for a fixed reservation and returns an idempotent release.
func (b *Budget) Acquire(ctx context.Context, size int64) (func(), error) {
	if size < 1 || size > b.capacity {
		return nil, fmt.Errorf("temporary reservation exceeds capacity")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.mu.Lock()
		if size <= b.capacity-b.used {
			b.used += size
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { b.mu.Lock(); b.used -= size; close(b.changed); b.changed = make(chan struct{}); b.mu.Unlock() })
			}, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
