// Package diskcache holds the persistent disk render-cache Store and the
// single-flight, mtime-LRU sweep that bounds it. Store (store.go) owns the
// sharded layout, gzip framing, atomic writes, and eviction; the helm and
// kustomize render caches sit on it and add only their value codec. The sweep
// pieces here — the single-flight gate that keeps a burst of writes from forking
// one sweep per write, and the oldest-first eviction loop — are unexported; the
// only entry point is Store.
package diskcache

import (
	"slices"
	"sync/atomic"
)

// gate is a single-flight gate for an asynchronous sweep: at most one sweep runs
// at a time, so a write storm past the cap schedules exactly one eviction pass
// instead of N. The zero value is ready to use.
type gate struct {
	busy atomic.Bool
}

// TryAcquire returns true and marks the gate busy when no sweep is in flight,
// false when one already is. The acquirer owns the gate until it calls Release.
func (g *gate) TryAcquire() bool { return g.busy.CompareAndSwap(false, true) }

// Release clears the busy flag so the next over-cap write can re-trigger. Pair
// every successful TryAcquire with a Release (defer it in the sweep goroutine).
func (g *gate) Release() { g.busy.Store(false) }

// entry is one candidate for eviction: an absolute path, its byte size, and the
// mtime the LRU order is computed from (unix nanos for a stable total order).
type entry struct {
	Path  string
	Size  int64
	MTime int64
}

// evictOldest removes entries oldest-first until the running total is at or
// below limit. total is the caller's pre-summed byte usage of entries; when it's
// already within limit nothing is removed. less defines the eviction order (the
// sweep pins mtime then path). remove deletes one entry's path and returns an
// error on failure; a failed remove is skipped (its bytes stay counted) and the
// sweep continues, matching the cache's best-effort semantics.
func evictOldest(entries []entry, total, limit int64, less func(a, b entry) int, remove func(e entry) error) {
	if total <= limit {
		return
	}
	slices.SortFunc(entries, less)
	for _, e := range entries {
		if total <= limit {
			break
		}
		if err := remove(e); err != nil {
			continue
		}
		total -= e.Size
	}
}
