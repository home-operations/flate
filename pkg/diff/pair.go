package diff

import (
	"cmp"
	"fmt"
	"iter"
	"log/slog"
	"slices"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
)

// pairedResource is one resource matched across the two input sets: a
// (left, right) pair sharing the same parent and identity. Either side
// is nil when the resource exists on only one side (added/removed).
type pairedResource struct {
	parent     Parent
	resource   manifest.NamedResource
	apiVersion string
	aPos, bPos int
	a, b       map[string]any
	aChart     string
	bChart     string
}

// changedPairs yields YAML differences after normalization, which can change identities.
// A nil side marshals to an empty document, so its counterpart is a full add/remove.
func changedPairs(left, right []Doc, opts Options) iter.Seq2[output.LineChange, error] {
	return func(yield func(output.LineChange, error) bool) {
		paired, _, _ := pair(left, right, opts)
		for _, p := range paired {
			from, err := marshalForUnified(p.a)
			if err != nil {
				yield(output.LineChange{}, err)
				return
			}
			to, err := marshalForUnified(p.b)
			if err != nil {
				yield(output.LineChange{}, err)
				return
			}
			if from == to {
				continue
			}
			status, _ := classify(p.a, p.b)
			if !yield(output.LineChange{
				Resource:   p.resource,
				Parent:     p.parent.NamedResource,
				ParentPath: p.parent.Path, From: from, To: to, Status: string(status),
			}, nil) {
				return
			}
		}
	}
}

// A nil side must stay empty so line formats report a wholesale add/remove.
func marshalForUnified(m map[string]any) (string, error) {
	if m == nil {
		return "", nil
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	return string(b), nil
}

// pairKey identifies a resource for cross-set matching.
type pairKey struct {
	parent, resource manifest.NamedResource
}

// pair matches each resource in left against its counterpart in right
// by canonical parent and resource identity, so a Deployment
// rendered by HelmRelease A never diffs against the same-named
// Deployment from HelmRelease B. The result is sorted by parent then
// resource identity for deterministic output. Duplicate identities retain
// the last document on each side, matching apply order, and emit a warning.
func pair(left, right []Doc, opts Options) ([]pairedResource, bool, bool) {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	add := func(side string, d Doc, chart string, pos int) {
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		resource := manifest.DocIdentity(d.Manifest)
		k := pairKey{d.Parent.NamedResource, resource}
		p, ok := idx[k]
		if !ok {
			p = &pairedResource{resource: resource, aPos: len(left), bPos: len(right)}
			idx[k] = p
		}
		p.parent = d.Parent
		p.apiVersion = apiVersion
		dst, dstChart, dstPos := &p.a, &p.aChart, &p.aPos
		if side == "right" {
			dst, dstChart, dstPos = &p.b, &p.bChart, &p.bPos
		}
		if *dst != nil {
			slog.Warn("diff: duplicate document shadows an earlier document",
				"side", side, "parentGroup", d.Parent.Group, "parentKind", d.Parent.Kind,
				"parentNamespace", d.Parent.Namespace, "parentName", d.Parent.Name,
				"parentPath", d.Parent.Path, "apiVersion", apiVersion,
				"group", resource.Group, "kind", resource.Kind, "namespace", resource.Namespace, "name", resource.Name)
		}
		*dst, *dstChart, *dstPos = d.Manifest, chart, pos
	}
	normalizedLeft, leftCopied := normalizeDocs(left, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedLeft {
		add("left", d, chartLabel(left[i].Manifest), i)
	}
	normalizedRight, rightCopied := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedRight {
		add("right", d, chartLabel(right[i].Manifest), i)
	}
	out := make([]pairedResource, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b pairedResource) int {
		return cmp.Or(
			a.parent.Compare(b.parent.NamedResource),
			a.resource.Compare(b.resource),
		)
	})
	return out, leftCopied, rightCopied
}
