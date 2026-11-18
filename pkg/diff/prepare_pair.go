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
	idx := make(map[pairKey]int, len(left)+len(right))
	entries := make([]pair, 0, len(left)+len(right))
	var warnings []manifest.Warning
	add := func(side side, d, original Doc, pos int) {
		apiVersion := manifest.DocAPIVersion(d.Manifest)
		resource := manifest.DocIdentity(d.Manifest)
		parent := d.Parent.NamedResource
		sideIndex := 0
		if side == sideCurrent {
			sideIndex = 1
		}
		keys := opts.producerKeys[sideIndex]
		if key, known := keys[parent]; known {
			parent = key
		}
		k := pairKey{parent: parent, resource: resource}
		entry, ok := idx[k]
		if !ok {
			entry = len(entries)
			entries = append(entries, pair{Resource: resource, OldPos: len(left), NewPos: len(right)})
			idx[k] = entry
		}
		p := &entries[entry]
		p.Parent = d.Parent
		p.APIVersion = apiVersion
		dst, dstChart, dstPos := &p.Old, &p.OldChart, &p.OldPos
		if side == sideCurrent {
			dst, dstChart, dstPos = &p.New, &p.NewChart, &p.NewPos
		}
		if *dst != nil {
			parent := d.Parent.String()
			subjects := opts.subjects[sideIndex]
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
		add(sideOrig, d, left[i], i)
	}
	normalizedRight, rightCopied := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedRight {
		add(sideCurrent, d, right[i], i)
	}
	refineGenerated(idx, entries, [2][]Doc{left, right})
	out := make([]pair, 0, len(idx))
	for _, entry := range idx {
		out = append(out, entries[entry])
	}
	slices.SortFunc(out, func(a, b pair) int {
		if order := cmp.Or(a.Parent.Compare(b.Parent.NamedResource), a.Resource.Compare(b.Resource)); order != 0 {
			return order
		}
		return a.canonicalParent(opts).Compare(b.canonicalParent(opts))
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

func (p *pair) canonicalParent(opts *Options) manifest.NamedResource {
	side := 0
	if p.New != nil {
		side = 1
	}
	parent := p.Parent.NamedResource
	if key, known := opts.producerKeys[side][parent]; known {
		return key
	}
	return parent
}
