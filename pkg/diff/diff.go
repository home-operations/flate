package diff

import (
	"bytes"
	"cmp"
	"fmt"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
)

// Parent identifies the Flux Kustomization or HelmRelease that rendered a
// manifest. It is a pairing discriminator so resources from different
// producers never match, and identifies the producer in structured output.
type Parent struct {
	manifest.NamedResource
	// Path is the Flux Kustomization spec.path (only set for KS
	// parents), with the conventional `./` prefix stripped. It is display
	// metadata, not part of producer identity.
	Path string `json:"path"`
}

// Doc pairs a rendered manifest with its parent.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// Options tunes Compare and RenderDocs behavior.
type Options struct {
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
func RenderDocs(left, right []Doc, opts Options) ([]byte, []manifest.Warning, error) {
	paired, warnings, leftCopied, rightCopied := pair(left, right, opts)
	opts.Format = cmp.Or(opts.Format, FormatHuman)
	var out []byte
	var err error
	switch opts.Format {
	case FormatDiff, FormatHTML:
		var buf bytes.Buffer
		encoder := output.Encoder{Writer: &buf, Format: output.Format(opts.Format)}
		err = encoder.Lines(changedPairs(paired))
		if err == nil {
			out = buf.Bytes()
		}
	case FormatJSON:
		out, err = renderJSON(changes(paired, leftCopied, rightCopied), opts.Suppressed)
	case FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea:
		out, err = renderNative(paired, opts.Format)
	default:
		err = fmt.Errorf("unsupported diff format %q", opts.Format)
	}
	return out, warnings, err
}
