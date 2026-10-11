package kustomize

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks, expands referenced and inline substitutions, then applies
// overlay before rendering or fingerprinting. A nil overlay adds no overrides.
// Substitution references contain flat strings, with no parsed YAML tree to cache.
func Prepare(ks *manifest.Kustomization, provider values.Provider, overlay map[string]string) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReferenceWithSubstitutions(ks, provider, overlay); err != nil {
		return nil, err
	}
	return ks, nil
}
