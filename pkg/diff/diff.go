package diff

import (
	"github.com/home-operations/flate/pkg/manifest"
)

// Parent identifies the Flux Kustomization or HelmRelease that rendered a
// manifest. Its zero value identifies an ownerless document. It distinguishes
// resources from different producers and attributes structured output.
type Parent struct {
	manifest.NamedResource

	// Path is the Flux Kustomization spec.path (only set for KS
	// parents), with the conventional `./` prefix stripped. It is display
	// metadata, not part of producer identity.
	Path string `json:"path"`
}

// Doc pairs a rendered manifest with its optional parent.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// Options tunes Compare and RenderDocs behavior.
type Options struct {
	subjects     [2]map[manifest.NamedResource]manifest.DiagnosticIdentity
	producerKeys [2]map[manifest.NamedResource]manifest.NamedResource
	identities   [2][]manifest.NamedResource
	// StripAttrs lists annotation/label keys removed from each
	// manifest's metadata (and pod-template metadata) before the diff
	// is computed. Cuts chart-bump noise — annotations like
	// `helm.sh/chart` or `checksum/config` whose values rotate on
	// every chart bump would otherwise produce a diff entry per
	// resource. dyff matches K8s lists by identifier but still
	// reports string-value changes verbatim, so this pre-filter still
	// earns its keep.
	StripAttrs []string
	// StripFields lists dotted spec field-paths (e.g.
	// "spec.restic.unlock") deleted from each manifest before the diff
	// is computed. Same rationale as StripAttrs but for volatile values
	// charts template into the spec rather than metadata — notably
	// volsync's `unlock: {{ now }}`, which rotates every render and
	// cannot be stabilized at render time (Helm exposes no funcMap
	// hook). See manifest.StripResourceFields.
	StripFields []string
	// Normalize, when non-nil, is an extra per-manifest scrub applied to
	// each (cloned) document AFTER StripAttrs/StripFields and binaryData
	// redaction, before resources are paired and compared. Use it to
	// suppress render-time noise the built-in strips don't cover —
	// e.g. an SDK consumer redacting Secret values or chart-minted TLS
	// certs so a per-render-random value doesn't read as a change.
	// nil = no extra scrub (the CLI default).
	Normalize func(map[string]any)
	// Format selects the output style (see the Format constants). The
	// zero value renders the human default.
	Format Format
	// Suppressed lists additional producer disclosures. Compare appends its
	// selected suppressions without modifying this slice and returns them as data.
	// JSON also includes them in its suppressed array.
	Suppressed []Suppression
}

// RenderDocs compares normalized documents by canonical parent and resource identity
// and returns the formatted diff for opts.Format. Parent.Path is display metadata.
// Native formats use dyff for content comparison and Kubernetes resource labels;
// unified, HTML and JSON formats present the same canonical pairs.
// Discovered warnings are returned even when formatting fails.
//
//nolint:gocritic // The SDK takes a value snapshot of rendering options.
func RenderDocs(left, right []Doc, opts Options) ([]byte, []manifest.Warning, error) {
	result := prepare(left, right, &opts)
	out, err := renderPrepared(result, &opts)
	return out, result.Warnings, err
}
