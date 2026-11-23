package manifest

import "sigs.k8s.io/kustomize/kyaml/yaml"

// ScopeLookup distinguishes resolved scopes from unknown scopes.
type ScopeLookup struct {
	Namespaced bool
	Found      bool
}

type scopeResolver interface {
	ResolveScope(yaml.TypeMeta) ScopeLookup
	LookupScope(yaml.TypeMeta) ScopeLookup
}

//nolint:gochecknoglobals // The registry shares discovered immutable resource scopes.
var scopes scopeResolver

// RegisterScopeResolver installs the process scope owner once during initialization.
// Standalone SDK users MUST register before parsing or querying scope. ResolveScope
// returns namespaced and known flags; LookupScope reads only prepared answers.
func RegisterScopeResolver(resolver scopeResolver) {
	if scopes != nil || resolver == nil {
		panic("manifest: scope resolver must be registered once during initialization")
	}
	scopes = resolver
}

// Namespaced resolves resource scope through the registered scope owner.
// Core identities without a version use v1. Unknown scopes use the upstream fallback.
func (k KindInfo) Namespaced() bool {
	if k.Kind == "" {
		return false
	}
	version := k.apiVersion
	if version == "" && k.Group == "" {
		version = "v1"
	}
	return scopes.ResolveScope(yaml.TypeMeta{APIVersion: version, Kind: k.Kind}).Namespaced
}

// AdmitsRoot reports whether an object's scope and identity permit independent admission.
func AdmitsRoot(obj BaseManifest) bool {
	return obj.Named().Namespace != "" || !KindOf(obj).Namespaced()
}
