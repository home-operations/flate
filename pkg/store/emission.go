package store

import "github.com/home-operations/flate/pkg/manifest"

// RecordEmitted records a complete ownership batch before children become
// visible to listeners. Self-edges are excluded and the first emitter wins.
// Ownership lasts for the run, including after DeleteObject.
func (s *Store) RecordEmitted(parent manifest.NamedResource, children []manifest.NamedResource) {
	if len(children) == 0 {
		return
	}
	s.emittedMu.Lock()
	defer s.emittedMu.Unlock()
	for _, child := range children {
		if parent == child {
			continue
		}
		if _, exists := s.emittedParents[child]; !exists {
			s.emittedParents[child] = parent
		}
	}
}

// EmittedParent returns the first render emitter of child.
func (s *Store) EmittedParent(child manifest.NamedResource) (manifest.NamedResource, bool) {
	s.emittedMu.RLock()
	defer s.emittedMu.RUnlock()
	parent, ok := s.emittedParents[child]
	return parent, ok
}

// EmittedChildrenByParent returns an independent snapshot of render ownership.
func (s *Store) EmittedChildrenByParent() map[manifest.NamedResource][]manifest.NamedResource {
	s.emittedMu.RLock()
	defer s.emittedMu.RUnlock()
	children := map[manifest.NamedResource][]manifest.NamedResource{}
	for child, parent := range s.emittedParents {
		children[parent] = append(children[parent], child)
	}
	return children
}
