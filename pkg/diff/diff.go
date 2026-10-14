package diff

import (
	"bytes"
	"cmp"
	"fmt"

	"github.com/home-operations/flate/internal/output"
)

// Parent identifies the Flux Kustomization or HelmRelease that rendered a
// manifest. It is a pairing discriminator so resources from different
// producers never match, and identifies the producer in structured output.
type Parent struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
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
	// selected suppression notes without modifying this slice. Text and HTML
	// formats disclose them in a leading note block; JSON includes them in its
	// suppressed array.
	Suppressed []Suppression
}

// RenderDocs is the top-level entry point: it compares the two doc sets and
// returns the formatted diff for opts.Format. The dyff text styles
// (github/human/brief/gitlab/gitea, and the zero value) render the whole
// set through dyff for native per-resource labels; FormatDiff takes the
// per-resource unified-diff path.
func RenderDocs(left, right []Doc, opts Options) ([]byte, error) {
	opts.Format = cmp.Or(opts.Format, FormatHuman)
	switch opts.Format {
	case FormatDiff, FormatHTML:
		var buf bytes.Buffer
		encoder := output.Encoder{Writer: &buf, Format: output.Format(opts.Format)}
		if err := encoder.Lines(changedPairs(left, right, opts), suppressionNotes(opts.Suppressed)); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case FormatJSON:
		return renderJSON(left, right, opts)
	case FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea:
		return renderNative(left, right, opts)
	default:
		return nil, fmt.Errorf("unsupported diff format %q", opts.Format)
	}
}
