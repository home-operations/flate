package kustomization

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Supplied identities MUST win even after an unknown producer reaches the store.
type substitutionProvider struct {
	sources  map[manifest.NamedResource]manifest.BaseManifest
	fallback values.Provider
}

func (p *substitutionProvider) Data(id manifest.NamedResource) manifest.Data {
	if obj, ok := p.sources[id].(manifest.Data); ok {
		return obj
	}
	return p.fallback.Data(id)
}
