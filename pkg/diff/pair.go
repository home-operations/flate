package diff

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// pairedResource is one resource matched across the two input sets: a
// (left, right) pair sharing the same parent and identity. Either side
// is nil when the resource exists on only one side (added/removed).
type pairedResource struct {
	parent                Parent
	kind, namespace, name string
	a, b                  map[string]any
}

// pairKey identifies a resource for cross-set matching.
type pairKey struct {
	// pPath disambiguates two KS parents with the same (kind, ns, name)
	// but different spec.path — a real-world collision in repos where
	// the same KS is rendered twice from different overlays.
	pKind, pNS, pName, pPath string
	apiVersion               string
	kind, ns, name           string
}

// pair matches each resource in left against its counterpart in right
// by (parent, apiVersion, kind, namespace, name), so a Deployment
// rendered by HelmRelease A never diffs against the same-named
// Deployment from HelmRelease B. The result is sorted by parent then
// resource identity for deterministic output.
func pair(left, right []Doc) []pairedResource {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side int, d Doc) {
		kind := manifest.DocKind(d.Manifest)
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		name, ns := manifest.DocMetadata(d.Manifest)
		k := pairKey{d.Parent.Kind, d.Parent.Namespace, d.Parent.Name, d.Parent.Path, apiVersion, kind, ns, name}
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
		return cmp.Or(
			cmp.Compare(a.parent.Kind, b.parent.Kind),
			cmp.Compare(a.parent.Namespace, b.parent.Namespace),
			cmp.Compare(a.parent.Name, b.parent.Name),
			cmp.Compare(a.parent.Path, b.parent.Path),
			cmp.Compare(a.kind, b.kind),
			cmp.Compare(a.namespace, b.namespace),
			cmp.Compare(a.name, b.name),
		)
	})
	return out
}
