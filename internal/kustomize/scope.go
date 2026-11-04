package kustomize

import (
	"sync"

	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

var namespaceScopes sync.Map //nolint:gochecknoglobals // Builtin scope results persist across all render invocations.

type namespaceScope struct{ namespaced, known bool }

type scopeResolver struct{}

//nolint:gochecknoinits // Importing the krusty owner registers the SDK scope resolver before use.
func init() { manifest.RegisterScopeResolver(scopeResolver{}) }

func (scopeResolver) LookupScope(meta yaml.TypeMeta) manifest.ScopeLookup {
	value, _ := namespaceScopes.Load(meta)
	scope, ok := value.(namespaceScope)
	return manifest.ScopeLookup{Namespaced: scope.namespaced, Found: ok}
}

func (scopeResolver) ResolveScope(meta yaml.TypeMeta) manifest.ScopeLookup {
	if value, ok := namespaceScopes.Load(meta); ok {
		scope, _ := value.(namespaceScope)
		return manifest.ScopeLookup{Namespaced: scope.namespaced, Found: scope.known}
	}
	BuildMutex.Lock()
	defer BuildMutex.Unlock()
	if value, ok := namespaceScopes.Load(meta); ok {
		scope, _ := value.(namespaceScope)
		return manifest.ScopeLookup{Namespaced: scope.namespaced, Found: scope.known}
	}
	// Builds may load custom schemas; only builtin scope may determine defaults.
	namespaced, found := openapi.IsNamespaceScoped(meta)
	namespaced = namespaced || !found
	namespaceScopes.Store(meta, namespaceScope{namespaced: namespaced, known: found})
	return manifest.ScopeLookup{Namespaced: namespaced, Found: found}
}
