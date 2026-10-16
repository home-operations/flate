package manifest

import "sigs.k8s.io/kustomize/kyaml/yaml"

type scopeResolver interface {
	ResolveScope(yaml.TypeMeta) bool
	LookupScope(yaml.TypeMeta) (bool, bool)
}

var scopes scopeResolver

// RegisterScopeResolver installs the process scope owner once during initialization.
// Standalone SDK users MUST register before parsing or querying scope. ResolveScope
// prepares builtin answers; LookupScope reads only those prepared answers.
func RegisterScopeResolver(resolver scopeResolver) {
	if scopes != nil || resolver == nil {
		panic("manifest: scope resolver must be registered once during initialization")
	}
	scopes = resolver
}

// Namespaced resolves the authored GVK through the registered scope owner.
// Unknown or versionless kinds use the upstream namespaced fallback.
func (k KindInfo) Namespaced() bool {
	return k.Kind != "" && scopes.ResolveScope(yaml.TypeMeta{APIVersion: k.apiVersion, Kind: k.Kind})
}
