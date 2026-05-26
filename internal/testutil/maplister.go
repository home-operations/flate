package testutil

import "github.com/home-operations/flate/pkg/manifest"

// MapLister is a test fixture that implements change.ObjectLister via a
// plain map. Used by controller tests that need a minimal ObjectLister
// (typically an empty one) to construct a change.Filter.
type MapLister map[manifest.NamedResource]manifest.BaseManifest

// GetObject returns the manifest stored under id, or nil if absent.
func (m MapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }

// ListObjects returns all manifests whose kind matches the given string.
func (m MapLister) ListObjects(kind string) []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for id, obj := range m {
		if id.Kind == kind {
			out = append(out, obj)
		}
	}
	return out
}
