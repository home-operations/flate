package diff

import (
	"cmp"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

type pair struct {
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

func prepare(left, right []Doc, opts *Options) prepared {
	idx := make(map[pairKey]*pair, len(left)+len(right))
	var warnings []manifest.Warning
	add := func(side Side, d, original Doc, pos int) {
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		resource := manifest.DocIdentity(d.Manifest)
		parent := d.Parent.NamedResource
		keys := opts.producerKeys[0]
		if side == SideCurrent {
			keys = opts.producerKeys[1]
		}
		if key, known := keys[parent]; known {
			parent = key
		}
		k := pairKey{parent, resource}
		p, ok := idx[k]
		if !ok {
			p = &pair{Resource: resource, OldPos: len(left), NewPos: len(right)}
			idx[k] = p
		}
		p.Parent = d.Parent
		p.APIVersion = apiVersion
		dst, dstChart, dstPos := &p.Old, &p.OldChart, &p.OldPos
		if side == SideCurrent {
			dst, dstChart, dstPos = &p.New, &p.NewChart, &p.NewPos
		}
		if *dst != nil {
			parent := d.Parent.String()
			subjects := opts.subjects[0]
			if side == SideCurrent {
				subjects = opts.subjects[1]
			}
			if subject, ok := subjects[d.Parent.NamedResource]; ok {
				parent = subject.String()
			}
			warnings = append(warnings, manifest.Warning{
				Resource: resource, Category: manifest.WarnDuplicateDocument,
				Message: "duplicate document shadows an earlier document",
				Detail: []string{
					"side: " + string(side), "parent: " + parent,
					"parent path: " + d.Parent.Path, "apiVersion: " + manifest.DocAPIVersion(original.Manifest),
				},
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
	out := make([]pair, 0, len(idx))
	for _, p := range idx {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b pair) int {
		return cmp.Or(
			a.Parent.Compare(b.Parent.NamedResource),
			a.Resource.Compare(b.Resource),
		)
	})
	slices.SortFunc(warnings, manifest.CompareWarning)
	return prepared{
		Pairs:       out,
		Warnings:    warnings,
		Subjects:    opts.subjects,
		leftCopied:  leftCopied,
		rightCopied: rightCopied,
	}
}
