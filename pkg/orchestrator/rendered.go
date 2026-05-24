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

// MarkRendered satisfies kustomization.RenderTracker. Called by the
// KS controller for every reconcilable child it emits from a render,
// passing the parent KS's id alongside the child's. The parent
// linkage replaces the previous file-path prefix-match used by
// loader.BuildParentIndexForKind — render-emitted children have no
// source file, but they DO have a known parent.
func (r *renderedSet) MarkRendered(parent, child manifest.NamedResource) {
	r.mu.Lock()
	r.parents[child] = parent
	r.mu.Unlock()
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
