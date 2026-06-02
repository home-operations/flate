package diff

import (
	"fmt"
	"strings"
)

// Parent identifies the Flux Kustomization or HelmRelease that
// rendered a manifest. The diff header includes this so the reviewer
// can see which app the change belongs to.
type Parent struct {
	Kind      string `json:"kind,omitempty"      yaml:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name,omitempty"      yaml:"name,omitempty"`
	// Path is the Flux Kustomization spec.path (only set for KS
	// parents). Slash-normalized, with the conventional `./` prefix
	// stripped so headers stay tidy.
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
}

// Doc pairs a rendered manifest with its parent. Run consumes these so
// each ResourceDiff knows which Flux KS / HR produced it.
type Doc struct {
	Manifest map[string]any
	Parent   Parent
}

// ResourceDiff is the per-resource result of a Run.
type ResourceDiff struct {
	Parent    Parent `json:"parent,omitzero"     yaml:"parent,omitempty"`
	Kind      string `json:"kind"                yaml:"kind"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name      string `json:"name"                yaml:"name"`
	Diff      string `json:"diff"                yaml:"diff"`
}

// Header returns the "Parent: ns/name Child: ns/name" prefix used
// in diff output. The Kustomization source path is intentionally
// omitted so KS-owned and HR-owned resources render symmetrically
// — `Parent.Path` survives on the struct for JSON/YAML consumers
// but never appears in the human-facing header.
func (d ResourceDiff) Header() string {
	parts := make([]string, 0, 2)
	if d.Parent.Kind != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", d.Parent.Kind, joinNS(d.Parent.Namespace, d.Parent.Name)))
	}
	parts = append(parts, fmt.Sprintf("%s: %s", d.Kind, joinNS(d.Namespace, d.Name)))
	return strings.Join(parts, " ")
}

func joinNS(ns, name string) string {
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

// Options tunes Run behavior.
type Options struct {
	// StripAttrs lists annotation/label keys removed from each
	// manifest's metadata (and pod-template metadata) before the diff
	// is computed. Cuts chart-bump noise — annotations like
	// `helm.sh/chart` or `checksum/config` whose values rotate on
	// every chart bump would otherwise produce a diff entry per
	// resource. dyff matches K8s lists by identifier but still
	// reports string-value changes verbatim, so this pre-filter still
	// pulls its weight after the dyff swap.
	StripAttrs []string
	// Format selects the per-resource diff body style Run renders.
	// yaml/json/markdown (and the zero value) fall back to the github
	// style — those aggregations embed or fence the github diff-syntax
	// body — while the plain-text styles (diff/github/human/brief/
	// gitlab/gitea) render their own body. Render must be called with
	// the same Format so aggregation matches the body.
	Format Format
}

// RenderDocs is the top-level entry point: it compares the two doc sets
// and returns the formatted diff for opts.Format. The dyff text styles
// (github/human/brief/gitlab/gitea, and the zero value) render the whole
// set through dyff for native per-resource labels; the structured and
// aggregated formats (diff/yaml/json/markdown) use flate's per-resource
// Run+Render pipeline, which keeps parent attribution.
func RenderDocs(left, right []Doc, opts Options) ([]byte, error) {
	switch opts.Format {
	case "", FormatGitHub, FormatHuman, FormatBrief, FormatGitLab, FormatGitea:
		return renderNative(left, right, opts)
	default:
		diffs, err := Run(left, right, opts)
		if err != nil {
			return nil, err
		}
		return Render(diffs, opts.Format)
	}
}

// Run compares two manifest sets and returns the resources whose
// rendered form differs. Resources missing on either side are compared
// against an empty document, producing a wholesale addition/removal.
// Each pair's body is rendered in the style Options.Format resolves to
// (see bodyStyle); identical resources are dropped. Used for the
// structured/aggregated formats — the dyff text styles go through
// renderNative (see RenderDocs).
func Run(left, right []Doc, opts Options) ([]ResourceDiff, error) {
	left = normalizeDocs(left, opts.StripAttrs)
	right = normalizeDocs(right, opts.StripAttrs)
	style := bodyStyle(opts.Format)
	out := make([]ResourceDiff, 0, len(left))
	for _, p := range pair(left, right) {
		body, err := renderPairBody(p, style)
		if err != nil {
			return nil, err
		}
		if body == "" {
			continue // identical resources
		}
		out = append(out, ResourceDiff{
			Parent: p.parent,
			Kind:   p.kind, Namespace: p.namespace, Name: p.name, Diff: body,
		})
	}
	return out, nil
}

// renderPairBody renders a single resource pair's diff body in the
// given style: a plain unified diff for FormatDiff, otherwise the
// matching dyff report.
func renderPairBody(p pairedResource, style Format) (string, error) {
	if style == FormatDiff {
		return unifiedBody(p.a, p.b, p.kind+" "+joinNS(p.namespace, p.name))
	}
	return dyffBody(p.a, p.b, style)
}
