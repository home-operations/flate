package diffprep

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Pair retains normalized sides and authored attribution for a canonical resource pair.
type Pair struct {
	Parent             Parent
	Resource           manifest.NamedResource
	APIVersion         string
	OldPos, NewPos     int
	Old, New           map[string]any
	OldChart, NewChart string
}

type pairKey struct {
	parent, resource manifest.NamedResource
}

// Prepare normalizes and pairs documents, retaining the last normalized duplicate.
func Prepare(left, right []Doc, opts Options) Prepared {
	idx := make(map[pairKey]*Pair, len(left)+len(right))
	var warnings []manifest.Warning
	add := func(side Side, d, original Doc, pos int) {
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		resource := manifest.DocIdentity(d.Manifest)
		k := pairKey{d.Parent.NamedResource, resource}
		p, ok := idx[k]
		if !ok {
			p = &Pair{Resource: resource, OldPos: len(left), NewPos: len(right)}
			idx[k] = p
		}
		p.Parent = d.Parent
		p.APIVersion = apiVersion
		dst, dstChart, dstPos := &p.Old, &p.OldChart, &p.OldPos
		if side == SideCurrent {
			dst, dstChart, dstPos = &p.New, &p.NewChart, &p.NewPos
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
	out := make([]Pair, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b Pair) int {
		return cmp.Or(
			a.Parent.Compare(b.Parent.NamedResource),
			a.Resource.Compare(b.Resource),
		)
	})
	slices.SortFunc(warnings, manifest.CompareWarning)
	return Prepared{Pairs: out, Warnings: warnings, leftCopied: leftCopied, rightCopied: rightCopied}
}
