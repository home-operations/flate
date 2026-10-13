package diff

import (
	"cmp"
	"iter"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// pairedResource is one resource matched across the two input sets: a
// (left, right) pair sharing the same parent and identity. Either side
// is nil when the resource exists on only one side (added/removed).
type pairedResource struct {
	parent                Parent
	apiVersion            string
	kind, namespace, name string
	a, b                  map[string]any
}

type changedPair struct {
	pairedResource
	from, to string
}

// changedPairs yields YAML differences after normalization, which can change identities.
// A nil side marshals to an empty document, so its counterpart is a full add/remove.
func changedPairs(left, right []Doc, opts Options) iter.Seq2[changedPair, error] {
	return func(yield func(changedPair, error) bool) {
		left = normalizeDocs(left, opts.StripAttrs, opts.StripFields, opts.Normalize)
		right = normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
		for _, p := range pair(left, right) {
			from, err := marshalForUnified(p.a)
			if err != nil {
				yield(changedPair{}, err)
				return
			}
			to, err := marshalForUnified(p.b)
			if err != nil {
				yield(changedPair{}, err)
				return
			}
			if from != to && !yield(changedPair{pairedResource: p, from: from, to: to}, nil) {
				return
			}
		}
	}
}

// label is the resource's human-facing title, e.g. "Deployment apps/web" —
// the heading the unified and HTML paths put above each resource's diff.
func (p pairedResource) label() string {
	return p.kind + " " + (manifest.NamedResource{Namespace: p.namespace, Name: p.name}).NamespacedName()
}

// pairKey identifies a resource for cross-set matching.
type pairKey struct {
	pKind, pNS, pName string
	apiVersion        string
	kind, ns, name    string
}

// pair matches each resource in left against its counterpart in right
// by (parent, apiVersion, kind, namespace, name), so a Deployment
// rendered by HelmRelease A never diffs against the same-named
// Deployment from HelmRelease B. The result is sorted by parent then
// resource identity for deterministic output. Duplicate identities retain
// the last document on each side, matching apply order, and emit a warning.
func pair(left, right []Doc) []pairedResource {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side string, d Doc) {
		kind := manifest.DocKind(d.Manifest)
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		name, ns := manifest.DocMetadata(d.Manifest)
		k := pairKey{d.Parent.Kind, d.Parent.Namespace, d.Parent.Name, apiVersion, kind, ns, name}
		p, ok := idx[k]
		if !ok {
			p = &pairedResource{parent: d.Parent, apiVersion: apiVersion, kind: kind, namespace: ns, name: name}
			idx[k] = p
		}
		dst := &p.a
		if side == "right" {
			dst = &p.b
			p.parent = d.Parent
		}
		if *dst != nil {
			slog.Warn("diff: duplicate document shadows an earlier document",
				"side", side, "parentKind", d.Parent.Kind,
				"parentNamespace", d.Parent.Namespace, "parentName", d.Parent.Name,
				"parentPath", d.Parent.Path, "apiVersion", apiVersion,
				"kind", kind, "namespace", ns, "name", name)
		}
		*dst = d.Manifest
	}
	for _, d := range left {
		add("left", d)
	}
	for _, d := range right {
		add("right", d)
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
			cmp.Compare(a.apiVersion, b.apiVersion),
		)
	})
	return out
}
