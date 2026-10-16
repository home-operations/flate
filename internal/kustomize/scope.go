package kustomize

import (
	"sync"

	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

var namespaceScopes sync.Map

type scopeResolver struct{}

func init() { manifest.RegisterScopeResolver(scopeResolver{}) }

func (scopeResolver) LookupScope(meta yaml.TypeMeta) (bool, bool) {
	value, _ := namespaceScopes.Load(meta)
	namespaced, ok := value.(bool)
	return namespaced, ok
}

func (r scopeResolver) ResolveScope(meta yaml.TypeMeta) bool {
	if value, ok := r.LookupScope(meta); ok {
		return value
	}
	BuildMutex.Lock()
	defer BuildMutex.Unlock()
	if value, ok := r.LookupScope(meta); ok {
		return value
	}
	// Builds may load custom schemas; only builtin scope may determine defaults.
	namespaced, found := openapi.IsNamespaceScoped(meta)
	namespaced = namespaced || !found
	namespaceScopes.Store(meta, namespaced)
	return namespaced
}
