package orchestrator

import (
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// renderedSet tracks IDs emitted by a parent Kustomization's render
// output (in addition to or instead of the file walker's static load).
// detectOrphans reads it to distinguish "loaded but never wired into
// any parent" from "loaded and emitted" — orphans get demoted from
// Failed to Ready/orphan so a stale on-disk KS doesn't fail the run.
//
// Lives here rather than on Store because nothing else cares: only the
// KS controller writes, only detectOrphans reads. Keeping it on
// Store was a layering smell flagged in iter-15 — the Store became a
// dumping ground for orchestrator-internal bookkeeping just because
// it keyed by NamedResource.
type renderedSet struct {
	mu  sync.RWMutex
	ids map[manifest.NamedResource]struct{}
}

func newRenderedSet() *renderedSet {
	return &renderedSet{ids: make(map[manifest.NamedResource]struct{})}
}

// MarkRendered satisfies kustomization.RenderTracker. Called by the
// KS controller for every reconcilable child it emits from a render.
func (r *renderedSet) MarkRendered(id manifest.NamedResource) {
	r.mu.Lock()
	r.ids[id] = struct{}{}
	r.mu.Unlock()
}

// has reports whether id was ever marked rendered.
func (r *renderedSet) has(id manifest.NamedResource) bool {
	r.mu.RLock()
	_, ok := r.ids[id]
	r.mu.RUnlock()
	return ok
}
