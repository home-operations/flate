// Package filtertest supplies in-memory relation fixtures for selection tests.
package filtertest

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// MapLister supplies immutable objects and discovery productions for filter fixtures.
type MapLister map[manifest.NamedResource]manifest.BaseManifest

// GetObject returns the manifest stored under id, or nil if absent.
func (m MapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }

// Objects returns the complete unordered fixture inventory.
func (m MapLister) Objects() []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for _, obj := range m {
		out = append(out, obj)
	}
	return out
}

// EmptyLister returns an empty object fixture.
func EmptyLister() MapLister { return MapLister{} }

// Productions exposes frozen fixture evidence without runtime contributions.
func (m MapLister) Productions(id manifest.NamedResource, index *manifest.DiscoveryIndex) []manifest.Production {
	r, _ := index.Lookup(id)
	return r.Productions
}

// GetArtifact returns no acquired artifact for map-only fixtures.
func (m MapLister) GetArtifact(manifest.NamedResource) store.Artifact { return nil }

// IncomingProductions returns only the contribution evidence supplied by the fixture.
func (m MapLister) IncomingProductions(child store.Child, _ *manifest.DiscoveryIndex) []manifest.Production {
	return child.Productions
}

// GetEmission returns no accepted fixture output.
func (m MapLister) GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission) {
	return nil, nil, store.Emission{}
}
