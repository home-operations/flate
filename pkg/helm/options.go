package helm

import (
	"strings"

	"helm.sh/helm/v4/pkg/chart/common"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

// RenderOptions supplies one render's captured sources and template settings.
type RenderOptions struct {
	Options

	// Preparation borrows immutable values across calls. Retire all owners before mutation.
	Preparation *Preparation `json:"-"`

	// Work borrows stage counters from this render execution.
	Work    *manifest.RenderWork `json:"-"`
	Sources Sources              `json:"Sources"`
	// PreparationTasks gates history decoding outside a task body.
	// Callers already holding a task slot MUST leave it nil.
	PreparationTasks *task.Service                       `json:"-"`
	Omitted          []manifest.NamedResource            `json:"Omitted"`
	Admit            func(RenderAdmission) (bool, error) `json:"Admit"`
}

// RenderAdmission carries accepted cache evidence bound to the current request.
type RenderAdmission struct {
	Possible  bool
	Accepted  bool
	Key       string
	Documents []manifest.DocOccurrence
	Receipt   *manifest.RenderReceipt
}

// Options collects the helm template flags flate supports.
type Options struct {
	// DisableChartDigestTracking preserves direct OCI chart versions when true.
	// False enables tracking. Revision validation and full source cache identity
	// apply in every mode.
	DisableChartDigestTracking bool `json:"DisableChartDigestTracking"`
	// SkipCRDs controls upstream Helm CRD generation.
	SkipCRDs bool `json:"SkipCRDs"`
	// SkipTests excludes templates that are helm test hooks.
	SkipTests bool `json:"SkipTests"`
	// SkipKinds MUST hold the complete, sorted, unique emission policy, including CRDs.
	// Callers keep it immutable and keep SkipCRDs consistent with its CRD membership.
	SkipKinds []string `json:"SkipKinds"`

	// KubeVersion overrides Capabilities.KubeVersion (.Capabilities.KubeVersion).
	KubeVersion string `json:"KubeVersion"`
	// APIVersions overrides Capabilities.APIVersions
	// (.Capabilities.APIVersions). Comma-separated.
	APIVersions string `json:"APIVersions"`

	// EnableDNS enables DNS lookups during templating.
	EnableDNS bool `json:"EnableDNS"`
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
			//nolint:wrapcheck // Template preserves Helm's exact Kubernetes-version diagnostic.
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
