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

func (p *substitutionProvider) ConfigMap(namespace, name string) *manifest.ConfigMap {
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: namespace, Name: name}
	if obj, ok := p.sources[id].(*manifest.ConfigMap); ok {
		return obj
	}
	return p.fallback.ConfigMap(namespace, name)
}

func (p *substitutionProvider) Secret(namespace, name string) *manifest.Secret {
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: namespace, Name: name}
	if obj, ok := p.sources[id].(*manifest.Secret); ok {
		return obj
	}
	return p.fallback.Secret(namespace, name)
}
