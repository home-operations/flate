package helm

import (
	"strings"

	"helm.sh/helm/v4/pkg/chart/common"
)

// Options collects the helm template flags flate supports.
type Options struct {
	// SkipCRDs excludes CRDs from the rendered output.
	SkipCRDs bool
	// SkipTests excludes templates that are helm test hooks.
	SkipTests bool
	// SkipSecrets excludes Secret resources from the output. flate
	// uses placeholder values anyway but stripping makes diffs tidier.
	SkipSecrets bool
	// SkipKinds is an arbitrary list of kinds to drop.
	SkipKinds []string

	// KubeVersion overrides Capabilities.KubeVersion (.Capabilities.KubeVersion).
	KubeVersion string
	// APIVersions overrides Capabilities.APIVersions
	// (.Capabilities.APIVersions). Comma-separated.
	APIVersions string

	// IsUpgrade sets .Release.IsUpgrade instead of .Release.IsInstall.
	IsUpgrade bool
	// NoHooks excludes hook-annotated templates.
	NoHooks bool
	// ShowOnly limits output to specific template paths.
	ShowOnly []string
	// EnableDNS enables DNS lookups during templating.
	EnableDNS bool
	// SkipSchemaValidation opts out of `values.schema.json` validation
	// for every HR — helm's jsonschema validator recompiles the schema
	// per render and dominates allocation churn on big repos. The
	// per-HR placeholder-detection path still kicks in on top of this:
	// even with this flag false, schema validation is skipped when the
	// HR values contain a wipe placeholder.
	SkipSchemaValidation bool
}

// SkipResourceKinds returns the union of canonical and user-specified
// kinds to drop from rendered output.
func (o Options) SkipResourceKinds() []string {
	extra := 0
	if o.SkipCRDs {
		extra++
	}
	if o.SkipSecrets {
		extra++
	}
	out := make([]string, len(o.SkipKinds), len(o.SkipKinds)+extra)
	copy(out, o.SkipKinds)
	if o.SkipCRDs {
		out = append(out, "CustomResourceDefinition")
	}
	if o.SkipSecrets {
		out = append(out, "Secret")
	}
	return out
}

// capabilities builds common.Capabilities from the supplied options.
// Returns the default capabilities when no overrides are provided.
func (o Options) capabilities() (*common.Capabilities, error) {
	caps := common.DefaultCapabilities.Copy()
	if o.KubeVersion != "" {
		kv, err := common.ParseKubeVersion(o.KubeVersion)
		if err != nil {
			return nil, err
		}
		caps.KubeVersion = *kv
	}
	if o.APIVersions != "" {
		caps.APIVersions = common.VersionSet(splitComma(o.APIVersions))
	}
	return caps, nil
}

// splitComma splits s on commas / whitespace into non-empty tokens.
// strings.FieldsFunc already skips empty spans, so no post-filter needed.
func splitComma(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
}
