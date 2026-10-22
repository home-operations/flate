package diff

import (
	"cmp"
	"fmt"
	"iter"
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

// A nil side marshals to an empty document, so its counterpart is a full add/remove.
func changedPairs(paired []pairedResource) iter.Seq2[output.LineChange, error] {
	return func(yield func(output.LineChange, error) bool) {
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

// Duplicate identities must retain the last normalized document, matching apply order.
func pair(left, right []Doc, opts Options) ([]pairedResource, []manifest.Warning, bool, bool) {
	idx := make(map[pairKey]*pairedResource, len(left)+len(right))
	var warnings []manifest.Warning
	add := func(side Side, d, original Doc, pos int) {
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
		if side == SideCurrent {
			dst, dstChart, dstPos = &p.b, &p.bChart, &p.bPos
		}
		if *dst != nil {
			warnings = append(warnings, manifest.Warning{
				Resource: resource, Category: manifest.WarnDuplicateDocument,
				Message: "duplicate document shadows an earlier document",
				Detail: []string{"side: " + string(side), "parent: " + d.Parent.String(),
					"parent path: " + d.Parent.Path, "apiVersion: " + manifest.DocAPIVersion(original.Manifest)},
			})
		}
		*dst, *dstChart, *dstPos = d.Manifest, chartLabel(original.Manifest), pos
	}
	normalizedLeft, leftCopied := normalizeDocs(left, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedLeft {
		add(SideOrig, d, left[i], i)
	}
	normalizedRight, rightCopied := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedRight {
		add(SideCurrent, d, right[i], i)
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
	slices.SortFunc(warnings, manifest.CompareWarning)
	return out, warnings, leftCopied, rightCopied
}
