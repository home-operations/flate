package helmrelease

import "github.com/home-operations/flate/pkg/manifest"

// isFluxSourceKind limits HelmRelease ownership and AddObject publication to sources.
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
