package diff

import (
	"cmp"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

type pair struct {
	Parent             Parent
	Resource           manifest.NamedResource
	Output             [2]manifest.NamedResource
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
		sideIndex := 0
		if side == SideCurrent {
			sideIndex = 1
		}
		keys := opts.producerKeys[sideIndex]
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
		p.Output[sideIndex] = resource
		if opts.identities[sideIndex] != nil {
			p.Output[sideIndex] = opts.identities[sideIndex][pos]
		}
		dst, dstChart, dstPos := &p.Old, &p.OldChart, &p.OldPos
		if side == SideCurrent {
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
		add(SideOrig, d, left[i], i)
	}
	normalizedRight, rightCopied := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i, d := range normalizedRight {
		add(SideCurrent, d, right[i], i)
	}
	pairGeneratorRenames(idx)
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

// pairGeneratorRenames joins the two sides of an object a kustomize generator renamed:
// its name carries a hash of its data, so a data change otherwise reports a removal and an addition.
// An ambiguous base name stays unpaired.
func pairGeneratorRenames(idx map[pairKey]*pair) {
	type candidates struct{ removed, added []pairKey }
	var bases map[pairKey]*candidates
	for k, p := range idx {
		if (p.Old == nil) == (p.New == nil) {
			continue
		}
		name, ok := generatorBaseName(k.resource.Name)
		if !ok {
			continue
		}
		base := k
		base.resource.Name = name
		if bases == nil {
			bases = make(map[pairKey]*candidates)
		}
		c := bases[base]
		if c == nil {
			c = &candidates{}
			bases[base] = c
		}
		if p.New == nil {
			c.removed = append(c.removed, k)
		} else {
			c.added = append(c.added, k)
		}
	}
	for _, c := range bases {
		if len(c.removed) != 1 || len(c.added) != 1 {
			continue
		}
		removed, added := idx[c.removed[0]], idx[c.added[0]]
		added.Old, added.OldChart, added.OldPos = removed.Old, removed.OldChart, removed.OldPos
		added.Output[0] = removed.Output[0]
		delete(idx, c.removed[0])
	}
}

// generatorBaseName strips the suffix kustomize api v0.21.2 hasher.encode appends:
// ten characters of a SHA-256 hex digest with 0, 1, 3, a and e remapped to g, h, k, m and t.
func generatorBaseName(name string) (string, bool) {
	cut := len(name) - generatorHashLength - 1
	if cut < 1 || name[cut] != '-' {
		return "", false
	}
	for _, r := range name[cut+1:] {
		if !strings.ContainsRune(generatorHashAlphabet, r) {
			return "", false
		}
	}
	return name[:cut], true
}

const (
	generatorHashLength   = 10
	generatorHashAlphabet = "2456789bcdfghkmt"
)
