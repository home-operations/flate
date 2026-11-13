// Package filtertest supplies in-memory relation fixtures for selection tests.
package filtertest

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// MapLister supplies immutable objects and discovery productions for filter fixtures.
type MapLister map[manifest.NamedResource]manifest.BaseManifest

// GetObject returns the manifest stored under id, or nil if absent.
//
//nolint:ireturn // The upstream interface contract requires this return type.
func (m MapLister) GetObject(id manifest.NamedResource) manifest.BaseManifest { return m[id] }

// GetDeclaration returns the statically admitted fixture manifest.
//
//nolint:ireturn // The upstream interface contract requires this return type.
func (m MapLister) GetDeclaration(id manifest.NamedResource) manifest.BaseManifest { return m[id] }

// Lookup returns the fixture object and its declaration roles.
//
//nolint:ireturn // The fixture MUST implement the heterogeneous store lookup contract.
func (m MapLister) Lookup(id manifest.NamedResource) (manifest.BaseManifest, manifest.DeclarationPolicy) {
	return m[id], (store.Child{Obj: m[id]}).Policy(id)
}

// Objects returns the complete unordered fixture inventory.
func (m MapLister) Objects() []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for _, obj := range m {
		if out == nil {
			out = make([]manifest.BaseManifest, 0, len(m))
		}
		out = append(out, obj)
	}
	return out
}

// ListObjects returns fixture objects of the exact group and kind.
func (m MapLister) ListObjects(group, kind string) []manifest.BaseManifest {
	var out []manifest.BaseManifest
	for id, obj := range m {
		if id.Group == group && id.Kind == kind {
			out = append(out, obj)
		}
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
//
//nolint:ireturn // The upstream interface contract requires this return type.
func (m MapLister) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact { return nil }

// GetStatus returns no assessment for map-only fixtures.
func (m MapLister) GetStatus(manifest.NamedResource) (store.StatusInfo, bool) {
	return store.StatusInfo{}, false
}

// IncomingProductions returns only the contribution evidence supplied by the fixture.
//
//nolint:gocritic // The admission reader interface requires a Child value snapshot.
func (m MapLister) IncomingProductions(child store.Child, _ *manifest.DiscoveryIndex) []manifest.Production {
	return child.Productions
}

// GetEmission returns no accepted fixture output.
//
//nolint:ireturn // The upstream interface contract requires this return type.
func (m MapLister) GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission) {
	return nil, nil, store.Emission{}
}

// ReferenceFacts exposes map-only data without supplied inputs.
func (m MapLister) ReferenceFacts(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
	_ manifest.NamedResource,
) store.ReferenceFacts {
	data, _ := m[id].(manifest.Data)
	return store.ReferenceFacts{
		Data:        data,
		Original:    data,
		Accepted:    data != nil,
		Productions: m.Productions(id, index),
	}
}
