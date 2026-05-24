package diff

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Format selects the diff output flavor.
type Format string

// Format values understood by Render.
const (
	// FormatDiff is dyff's `--output github` mode: path-based diff
	// syntax (`@@`, `+`, `-`, `!`) that GitHub's diff lexer renders
	// natively as a colored diff block when wrapped in a ```diff
	// fence. K8s-aware: list entries are matched by identifier
	// (container name, env-var name, etc.), so reordering a list
	// produces no diff churn.
	FormatDiff   Format = "diff"
	FormatObject Format = "object"
	FormatYAML   Format = "yaml"
	FormatJSON   Format = "json"
)

// Options tunes Run behavior.
type Options struct {
	// Format selects the output flavor. Default FormatDiff.
	Format Format
}

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

// Doc pairs a rendered manifest with its parent. diff.Run consumes
// these so each ResourceDiff knows which Flux KS / HR produced it.
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

// Header returns the flux-local-style "[path] Parent: ns/name
// Child: ns/name" prefix used in diff output.
func (d ResourceDiff) Header() string {
	var parts []string
	if d.Parent.Path != "" {
		parts = append(parts, d.Parent.Path)
	}
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

// Run compares two manifest sets and returns the resources whose
// rendered form differs. Resources missing on either side are
// reported with the counterpart as an empty document, producing a
// wholesale addition/removal in the dyff output. Pairs are keyed by
// (parent, kind, namespace, name) so a Deployment rendered by
// HelmRelease A doesn't accidentally diff against the same-named
// Deployment from HelmRelease B.
func Run(left, right []Doc, _ Options) ([]ResourceDiff, error) {
	pairs := pair(left, right)
	out := make([]ResourceDiff, 0, len(pairs))
	for _, p := range pairs {
		body, err := dyffDiff(p.a, p.b)
		if err != nil {
			return nil, err
		}
		if body == "" {
			// Identical resources: dyff yields no diffs. Skip.
			continue
		}
		out = append(out, ResourceDiff{
			Parent: p.parent,
			Kind:   p.kind, Namespace: p.namespace, Name: p.name, Diff: body,
		})
	}
	return out, nil
}

// Render serializes a diff result set into the requested format.
func Render(diffs []ResourceDiff, format Format) ([]byte, error) {
	switch format {
	case "", FormatDiff:
		var b bytes.Buffer
		for _, d := range diffs {
			h := d.Header()
			// Per-resource header framed with `---` / `+++` so GitHub's
			// diff lexer renders it as the file-name banner above each
			// resource's body. The body itself is dyff `github`-mode
			// output (path-keyed `@@` hunks, `+`/`-` value lines, `!`
			// change-type markers).
			fmt.Fprintf(&b, "--- %s\n+++ %s\n", h, h)
			b.WriteString(d.Diff)
			if !strings.HasSuffix(d.Diff, "\n") {
				b.WriteByte('\n')
			}
		}
		return b.Bytes(), nil
	case FormatObject:
		var b bytes.Buffer
		for _, d := range diffs {
			b.WriteString(d.Header())
			b.WriteByte('\n')
			b.WriteString(d.Diff)
			if !strings.HasSuffix(d.Diff, "\n") {
				b.WriteByte('\n')
			}
		}
		return b.Bytes(), nil
	case FormatYAML:
		return yaml.Marshal(diffs)
	case FormatJSON:
		return json.MarshalIndent(diffs, "", "  ")
	}
	return nil, fmt.Errorf("unknown diff format %q", format)
}

type pairedResource struct {
	parent                Parent
	kind, namespace, name string
	a, b                  map[string]any
}

type pairKey struct {
	pKind, pNS, pName string
	kind, ns, name    string
}

func pair(left, right []Doc) []pairedResource {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side int, d Doc) {
		kind := manifest.DocKind(d.Manifest)
		name, ns := manifest.DocMetadata(d.Manifest)
		k := pairKey{d.Parent.Kind, d.Parent.Namespace, d.Parent.Name, kind, ns, name}
		p, ok := idx[k]
		if !ok {
			p = &pairedResource{parent: d.Parent, kind: kind, namespace: ns, name: name}
			idx[k] = p
		}
		if side == 0 {
			p.a = d.Manifest
		} else {
			p.b = d.Manifest
		}
	}
	for _, d := range left {
		add(0, d)
	}
	for _, d := range right {
		add(1, d)
	}
	out := make([]pairedResource, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b pairedResource) int {
		if c := cmp.Compare(a.parent.Kind, b.parent.Kind); c != 0 {
			return c
		}
		if c := cmp.Compare(a.parent.Namespace, b.parent.Namespace); c != 0 {
			return c
		}
		if c := cmp.Compare(a.parent.Name, b.parent.Name); c != 0 {
			return c
		}
		if c := cmp.Compare(a.kind, b.kind); c != 0 {
			return c
		}
		if c := cmp.Compare(a.namespace, b.namespace); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
	return out
}
