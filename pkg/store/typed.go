package store

import "github.com/home-operations/flate/pkg/manifest"

// Get fetches the object at id and asserts it to T in one step.
// Returns (zero, false) when the object is absent or stored under a
// different type. Eliminates the two-line GetObject + type-assert
// pattern that otherwise repeats across controllers, discovery, and
// the orchestrator:
//
//	// before
//	ks, ok := s.GetObject(id).(*manifest.Kustomization)
//
//	// after
//	ks, ok := s.Get[*manifest.Kustomization](id)
//
// The constraint on T is manifest.BaseManifest — any stored manifest
// type — so Get and GetByName share one uniform, type-safe lookup.
func (s *Store) Get[T manifest.BaseManifest](id manifest.NamedResource) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	obj, ok := s.GetObject(id).(T)
	return obj, ok
}

// GetByName is the (kind, namespace, name) sibling of Get — performs
// the same store lookup as Store.GetObjectByName and asserts the
// result to T. Returns (zero, false) on miss or type mismatch.
//
// Useful when the caller has the human-readable triple (e.g. from a
// SecretRef or valuesFrom) rather than a NamedResource.
func (s *Store) GetByName[T manifest.BaseManifest](kind, namespace, name string) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	obj, ok := s.GetObjectByName(kind, namespace, name).(T)
	return obj, ok
}

// ListAs lists every stored object of kind and returns those that
// type-assert to T. Mirrors ListObjects(kind) but returns the
// concrete-typed slice the caller almost always needs:
//
//	// before
//	for _, obj := range s.ListObjects(manifest.KindKustomization) {
//	    ks, ok := obj.(*manifest.Kustomization)
//	    if !ok { continue }
//	    // use ks
//	}
//
//	// after
//	for _, ks := range s.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
//	    // use ks
//	}
//
// The store invariant guarantees that ListObjects(K) returns only
// objects whose Named().Kind == K, so the inner type-assert is a
// defensive net rather than a real filter — but keeping it cheap
// means a future invariant relaxation degrades safely rather than
// panicking.
func (s *Store) ListAs[T manifest.BaseManifest](kind string) []T {
	if s == nil {
		return nil
	}
	raw := s.ListObjects(kind)
	out := make([]T, 0, len(raw))
	for _, obj := range raw {
		if typed, ok := obj.(T); ok {
			out = append(out, typed)
		}
	}
	return out
}
