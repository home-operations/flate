// Package diskcache holds the pieces of the single-flight, mtime-LRU
// disk-cache sweep used by the helm render cache (pkg/helm). Entry
// collection (a recursive file walk) stays in the caller; what's shared
// here is the single-flight gate that keeps a burst of writes from
// forking one sweep per write, and the oldest-first eviction loop that
// deletes entries until total bytes fall under a cap.
package diskcache

import (
	"slices"
	"sync/atomic"
)

// Gate is a single-flight gate for an asynchronous sweep: at most one
// sweep runs at a time, so a write storm past the cap schedules exactly
// one eviction pass instead of N. The zero value is ready to use.
type Gate struct {
	busy atomic.Bool
}

// TryAcquire returns true and marks the gate busy when no sweep is in
// flight, false when one already is. The acquirer owns the gate until
// it calls Release.
func (g *Gate) TryAcquire() bool { return g.busy.CompareAndSwap(false, true) }

// Release clears the busy flag so the next over-cap write can
// re-trigger. Pair every successful TryAcquire with a Release (defer it
// in the sweep goroutine).
func (g *Gate) Release() { g.busy.Store(false) }

// Entry is one candidate for eviction: an absolute path, its byte size,
// and the mtime the LRU order is computed from (unix nanos for a stable
// total order across the two callers' clock representations).
type Entry struct {
	Path  string
	Size  int64
	MTime int64
}

// EvictOldest removes entries oldest-first until the running total is at
// or below limit. total is the caller's pre-summed byte usage of
// entries; when it's already within limit nothing is removed. less
// defines the eviction order (the helm render cache pins mtime then
// path). remove deletes one entry's path and returns an error on
// failure; a failed remove is skipped (its bytes stay counted) and the
// sweep continues, matching the caller's best-effort semantics.
func EvictOldest(entries []Entry, total, limit int64, less func(a, b Entry) int, remove func(e Entry) error) {
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
