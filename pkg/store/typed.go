package store

import "github.com/home-operations/flate/pkg/manifest"

// Get fetches the object at id and asserts it to T.
// Returns (zero, false) when absent, stored under another type, or s is nil.
func (s *Store) Get[T manifest.BaseManifest](id manifest.NamedResource) (T, bool) {
	if s == nil {
		var zero T
		return zero, false
	}
	obj, ok := s.GetObject(id).(T)
	return obj, ok
}

// ListAs returns objects in the exact group/kind bucket that assert to T.
// Empty group and kind selects the complete inventory, as in ListObjects.
func (s *Store) ListAs[T manifest.BaseManifest](group, kind string) []T {
	if s == nil {
		return nil
	}
	raw := s.ListObjects(group, kind)
	out := make([]T, 0, len(raw))
	for _, obj := range raw {
		if typed, ok := obj.(T); ok {
			out = append(out, typed)
		}
	}
	return out
}
