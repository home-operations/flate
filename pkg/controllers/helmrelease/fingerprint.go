package helmrelease

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// helmReleaseFingerprint produces a stable hash of the inputs that
// determine helm.Template's output for hr. Excludes metadata.labels
// and metadata.annotations on purpose — kustomize-controller-emitted
// HRs differ from their file-loaded sources only in label stamping,
// and re-rendering on a label diff is pure waste. Returns "" when
// the payload can't be hashed (degrades safely: manifest.Fingerprint
// returns "", which never matches, so the dedup short-circuit is
// skipped and we re-render).
func helmReleaseFingerprint(hr *manifest.HelmRelease) string {
	return manifest.Fingerprint(helmReleaseFingerprintPayload(hr))
}

func helmReleaseFingerprintPayload(hr *manifest.HelmRelease) any {
	return struct {
		ReleaseName              string
		ReleaseNamespace         string
		Chart                    manifest.HelmChart
		Values                   map[string]any
		Spec                     helmv2.HelmReleaseSpec
		ChartValuesFiles         []string
		IgnoreMissingValuesFiles bool
		CRDsPolicy               string
		DisableSchemaValidation  bool
		DisableOpenAPIValidation bool
	}{
		ReleaseName:              hr.ReleaseName(),
		ReleaseNamespace:         hr.ReleaseNamespace(),
		Chart:                    hr.Chart,
		Values:                   hr.Values,
		Spec:                     hr.HelmReleaseSpec,
		ChartValuesFiles:         hr.ChartValuesFiles,
		IgnoreMissingValuesFiles: hr.IgnoreMissingValuesFiles,
		CRDsPolicy:               hr.CRDsPolicy,
		DisableSchemaValidation:  hr.DisableSchemaValidation,
		DisableOpenAPIValidation: hr.DisableOpenAPIValidation,
	}
}
