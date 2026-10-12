package helmrelease

import (
	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// helmReleaseFingerprint hashes render inputs and omitted-reference advisories.
// Labels and annotations MUST not invalidate parent-stamped artifact replays.
// Unhashable inputs return "", which MUST bypass the dedup short-circuit.
func helmReleaseFingerprint(hr *manifest.HelmRelease, sourceIdentity string, omitted []manifest.NamedResource) string {
	payload := helmReleaseFingerprintPayload(hr, sourceIdentity)
	if len(omitted) > 0 {
		payload = struct {
			Release any
			Omitted []manifest.NamedResource
		}{payload, omitted}
	}
	return manifest.Fingerprint(payload)
}

func helmReleaseFingerprintPayload(hr *manifest.HelmRelease, sourceIdentity string) any {
	return struct {
		SourceIdentity           string `json:",omitempty"`
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
	}
}
