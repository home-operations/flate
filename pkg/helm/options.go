package helm

import (
	"slices"
	"strings"

	"helm.sh/helm/v4/pkg/chart/common"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

// RenderOptions supplies one render's captured sources and template settings.
type RenderOptions struct {
	Options
	Sources Sources
	// WipeSecrets binds the emitted Secret policy to accepted history.
	WipeSecrets bool
	// DependencySubstitutions binds emitted dependency projection to accepted history.
	DependencySubstitutions values.DependencySubstitutions
	// Admit receives the cache owner's content key; true returns empty output without rendering.
	Admit func(string) (bool, error)
}

// Options collects the helm template flags flate supports.
type Options struct {
	// DisableChartDigestTracking preserves direct OCI chart versions when true.
	// False enables tracking. Revision validation and full source cache identity
	// apply in every mode.
	DisableChartDigestTracking bool
	// SkipCRDs excludes CRDs from the rendered output.
	SkipCRDs bool
	// SkipTests excludes templates that are helm test hooks.
	SkipTests bool
	// SkipKinds is an arbitrary list of kinds to drop.
	SkipKinds []string

	// KubeVersion overrides Capabilities.KubeVersion (.Capabilities.KubeVersion).
	KubeVersion string
	// APIVersions overrides Capabilities.APIVersions
	// (.Capabilities.APIVersions). Comma-separated.
	APIVersions string

	// EnableDNS enables DNS lookups during templating.
	EnableDNS bool
}

// SkipResourceKinds returns the union of canonical and user-specified
// kinds to drop from rendered output.
func (o Options) SkipResourceKinds() []string {
	out := slices.Grow(slices.Clone(o.SkipKinds), 1)
	if o.SkipCRDs {
		out = append(out, manifest.KindCustomResourceDefinition)
	}
	return out
}

// capabilities builds common.Capabilities from the supplied options.
// Returns the default capabilities when no overrides are provided.
func (o Options) capabilities() (*common.Capabilities, error) {
	if o.KubeVersion == "" && o.APIVersions == "" {
		return common.DefaultCapabilities, nil
	}
	caps := common.DefaultCapabilities.Copy()
	if o.KubeVersion != "" {
		kv, err := common.ParseKubeVersion(o.KubeVersion)
		if err != nil {
			return nil, err
		}
		caps.KubeVersion = *kv
	}
	if o.APIVersions != "" {
		caps.APIVersions = common.VersionSet(strings.FieldsFunc(o.APIVersions, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t'
		}))
	}
	return caps, nil
}
