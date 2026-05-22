// Package keylock provides per-key mutual-exclusion locks that honor
// context cancellation. A KeyMap[K] is a process-wide map of locks
// keyed by K; Acquire returns once the slot is free or ctx fires.
package keylock

import (
	"context"
	"sync"
)

// KeyMap holds one lock per key value, allocated on first use.
type KeyMap[K comparable] struct {
	mu    sync.Mutex
	locks map[K]chan struct{}
}

// New constructs an empty KeyMap.
func New[K comparable]() *KeyMap[K] {
	return &KeyMap[K]{locks: map[K]chan struct{}{}}
}

// Acquire blocks until the lock for key is held by the caller, or ctx
// is done. Returns ctx.Err on cancellation (lock not held). On success
// returns a release func — call it (typically deferred) to free the
// lock.
func (m *KeyMap[K]) Acquire(ctx context.Context, key K) (release func(), err error) {
	m.mu.Lock()
	lock, ok := m.locks[key]
	if !ok {
		lock = make(chan struct{}, 1)
		m.locks[key] = lock
	}
	m.mu.Unlock()

	select {
	case lock <- struct{}{}:
		return func() { <-lock }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}
