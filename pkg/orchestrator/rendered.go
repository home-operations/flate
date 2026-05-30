package orchestrator

import (
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// renderedSet tracks IDs emitted by a parent Kustomization's render
// output AND records which parent emitted each one. The parent
// linkage is the foundation for render-driven discovery — consumers
// that today rely on `sourceFiles[id]` for file-loaded resources
// (parent index, change filter, orphan detection, RS extension
// attribution) can query ParentOf for resources that arrive via a
// KS render instead.
//
// detectOrphans reads `Has` to distinguish "loaded but never wired
// into any parent" from "loaded and emitted" — orphans get demoted
// from Failed to Ready/orphan so a stale on-disk KS doesn't fail
// the run.
//
// Lives here rather than on Store because nothing else cares: only
// the KS controller writes, only orchestrator-side consumers read.
// Keeping it on Store was a layering smell flagged in iter-15 —
// the Store became a dumping ground for orchestrator-internal
// bookkeeping just because it keyed by NamedResource.
type renderedSet struct {
	mu      sync.RWMutex
	parents map[manifest.NamedResource]manifest.NamedResource
}

func newRenderedSet() *renderedSet {
	return &renderedSet{parents: make(map[manifest.NamedResource]manifest.NamedResource)}
}

// MarkRenderedBatch records multiple parent→child edges atomically
// under a single lock acquisition. Used by the KS controller's
// emitRenderedChildren loop, which would otherwise pay N full Lock
// acquisitions on r.mu for the N children of a single render. Self-
// edges are dropped (a KS whose spec.path covers its own definition
// file — the Zariel/home-ops cluster KS pattern — emits itself during
// render; the file-loaded path-prefix index already excludes self in
// loader.LongestParent), first-write-wins on duplicates. No-op for an
// empty children slice.
func (r *renderedSet) MarkRenderedBatch(parent manifest.NamedResource, children []manifest.NamedResource) {
	if len(children) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, child := range children {
		if parent == child {
			continue
		}
		r.markLocked(parent, child)
	}
}

// markLocked is the lock-held body of MarkRenderedBatch. Caller MUST
// hold r.mu (write lock). Applies first-write-wins on duplicate
// children.
//
// First-write-wins: a child emitted by multiple parents (rare —
// kustomize replacements/components patterns) keeps its initial
// attribution. Without this guard, PR #361's fingerprint-dedup
// replay re-runs the emit on every reconcile of every parent
// and silently swaps attribution to whichever parent reconciled
// most recently — breaking detectOrphans / ParentOf / RS-extension
// queries that subsequently read the wrong parent.
func (r *renderedSet) markLocked(parent, child manifest.NamedResource) {
	if _, exists := r.parents[child]; exists {
		return
	}
	r.parents[child] = parent
}

// has reports whether id was ever marked rendered.
func (r *renderedSet) has(id manifest.NamedResource) bool {
	r.mu.RLock()
	_, ok := r.parents[id]
	r.mu.RUnlock()
	return ok
}

// ParentOf returns the parent KS that emitted id, or (zero, false)
// when id was never marked rendered (i.e. it was file-loaded, or
// it's the bootstrap-injected GitRepository).
func (r *renderedSet) ParentOf(id manifest.NamedResource) (manifest.NamedResource, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	parent, ok := r.parents[id]
	return parent, ok
}
