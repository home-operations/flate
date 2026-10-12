package helmrelease

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// helmReleaseFingerprint hashes render inputs and omitted-reference advisories.
// Labels and annotations MUST not invalidate parent-stamped artifact replays.
// Unhashable inputs return "", which MUST bypass the dedup short-circuit.
func helmReleaseFingerprint(hr *manifest.HelmRelease, sourceIdentity string, omitted []manifest.NamedResource) string {
	return manifest.Fingerprint(struct {
		Omitted                  []manifest.NamedResource `json:",omitempty"`
		SourceIdentity           string                   `json:",omitempty"`
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
		Omitted:                  omitted,
		SourceIdentity:           sourceIdentity,
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
	})
}
