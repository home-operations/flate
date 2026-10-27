package kustomize

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare clones ks and resolves referenced and inline substitutions.
func Prepare(ks *manifest.Kustomization, provider values.Provider) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		return nil, err
	}
	return ks, nil
}
