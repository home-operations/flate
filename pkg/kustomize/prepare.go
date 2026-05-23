package kustomize

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// Prepare runs the standard pre-render dance for a Kustomization so it
// is ready to feed into RenderFlux:
//
//  1. Clone ks so subsequent mutations don't touch the store-canonical
//     copy (the immutability contract every flate controller honors —
//     see pkg/manifest/doc.go).
//  2. Expand spec.postBuild.substituteFrom references against the
//     supplied values provider so ks.PostBuildSubstitute reflects the
//     merged result a render would consume.
//
// Embedders rendering a single Kustomization without standing up the
// orchestrator's KS controller call Prepare then RenderFlux. Mirrors
// the symmetric helm.Prepare for HelmReleases.
func Prepare(ks *manifest.Kustomization, provider values.Provider) (*manifest.Kustomization, error) {
	ks = ks.Clone()
	if err := values.ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		return nil, err
	}
	return ks, nil
}
