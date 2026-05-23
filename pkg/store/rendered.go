package store

import "github.com/home-operations/flate/pkg/manifest"

// MarkRendered records that id was emitted by a parent Kustomization's
// render output (as opposed to coming only from the static file walk).
// The orchestrator's post-run phase uses this to distinguish real
// failures from orphaned-and-loaded-anyway resources.
//
// This is orphan-detection bookkeeping rather than a state mutation
// observers care about — no event fires. Co-locating the set with the
// rest of the Store is a convenience: every key is a NamedResource and
// the Store is the canonical place for per-id metadata.
func (s *Store) MarkRendered(id manifest.NamedResource) {
	s.mu.Lock()
	s.rendered[id] = struct{}{}
	s.mu.Unlock()
}

// WasRendered reports whether id was ever emitted by a parent's render.
func (s *Store) WasRendered(id manifest.NamedResource) bool {
	s.mu.RLock()
	_, ok := s.rendered[id]
	s.mu.RUnlock()
	return ok
}
