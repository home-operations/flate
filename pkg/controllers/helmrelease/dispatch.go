package helmrelease

import "github.com/home-operations/flate/pkg/manifest"

// isFluxSourceKind reports whether obj is a Flux source CR — one
// the source controller is registered to reconcile. Mirrors the
// subset of kustomization.shouldDispatchAsObject that the HR
// controller actually wants to re-emit: only sources are
// chart-rendered in the wild (tofu-controller's OCIRepository, ESO
// chart's HelmRepository fallback). Charts that render KS / HR are
// rare and risky — restricting the AddObject dispatch to sources
// keeps the blast radius small.
func isFluxSourceKind(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.GitRepository,
		*manifest.OCIRepository,
		*manifest.HelmRepository,
		*manifest.Bucket,
		*manifest.HelmChartSource,
		*manifest.ExternalArtifact:
		return true
	}
	return false
}
