package manifest

import (
	"sync"

	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/kustomize"
)

var namespaceScopes sync.Map

func isNamespaceScoped(t yaml.TypeMeta) bool {
	if value, ok := namespaceScopes.Load(t); ok {
		return value.(bool)
	}
	kustomize.BuildMutex.Lock()
	defer kustomize.BuildMutex.Unlock()
	if value, ok := namespaceScopes.Load(t); ok {
		return value.(bool)
	}
	// Builds may load custom schemas; only builtin scope may determine defaults.
	openapi.ResetOpenAPI()
	_ = openapi.Schema()
	namespaced, found := openapi.IsNamespaceScoped(t)
	namespaced = namespaced || !found
	namespaceScopes.Store(t, namespaced)
	return namespaced
}
