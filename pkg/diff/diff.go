package diff

import "fmt"

// Parent identifies the Flux Kustomization or HelmRelease that rendered a
// manifest. It never appears in the output — it's a pairing discriminator
// (see pairKey) so a Deployment rendered by HelmRelease A never diffs
// against the same-named Deployment from HelmRelease B.
type Parent struct {
	Kind      string
	Namespace string
	Name      string
	// Path is the Flux Kustomization spec.path (only set for KS
	// parents). Slash-normalized, with the conventional `./` prefix
	// stripped. Disambiguates two KS parents that share a (kind, ns,
	// name) but render from different overlays.
	Path string
}

// Doc pairs a rendered manifest with its parent.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// Options tunes RenderDocs behavior.
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
	// Format selects the output style (see the Format constants). The
	// zero value renders the human default.
	Format Format
}

// RenderDocs is the top-level entry point: it compares the two doc sets and
// returns the formatted diff for opts.Format. The dyff text styles
// (github/human/brief/gitlab/gitea, and the zero value) render the whole
// set through dyff for native per-resource labels; FormatDiff takes the
// per-resource unified-diff path.
func RenderDocs(left, right []Doc, opts Options) ([]byte, error) {
	switch {
	case opts.Format == "" || opts.Format.isDyffText():
		return renderNative(left, right, opts)
	case opts.Format == FormatDiff:
		return renderUnified(left, right, opts)
	case opts.Format == FormatHTML:
		return renderHTML(left, right, opts)
	default:
		return nil, fmt.Errorf("unsupported diff format %q", opts.Format)
	}
}
