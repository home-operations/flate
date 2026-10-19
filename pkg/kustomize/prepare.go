package kustomize

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks and resolves referenced, inline and overlay substitutions.
// A nil overlay adds no overrides.
func Prepare(ks *manifest.Kustomization, provider values.Provider, overlay map[string]string) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReferenceWithSubstitutions(ks, provider, overlay); err != nil {
		return nil, err
	}
	return ks, nil
}
