package diff

import (
	"cmp"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type pair struct {
	Parent             Parent
	Resource           manifest.NamedResource
	APIVersion         string
	OldPos, NewPos     int
	Old, New           map[string]any
	OldChart, NewChart string
	ProducerChange     *producerTransition
}

type producerTransition struct {
	From, To manifest.DiagnosticIdentity
}

func (d *Doc) producer() manifest.DiagnosticIdentity {
	if d.Producer.ID != (manifest.NamedResource{}) {
		return d.Producer
	}
	return manifest.DiagnosticFor(d.Parent.NamedResource, nil)
}

type pairIndex struct {
	byIdentity map[manifest.NamedResource]int
	entries    []pair
	originals  [2][]Doc
	competing  [2]map[producerOccurrence]bool
	warnings   []manifest.Warning
}

type producerOccurrence struct {
	entry int
	id    manifest.NamedResource
}

func prepare(left, right []Doc, opts *Options) prepared {
	index := pairIndex{
		byIdentity: make(map[manifest.NamedResource]int, len(left)+len(right)),
		entries:    make([]pair, 0, len(left)+len(right)),
		originals:  [2][]Doc{left, right},
	}
	normalizedLeft, leftCopied := normalizeDocs(left, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i := range normalizedLeft {
		index.add(0, &normalizedLeft[i], i, opts.subjects[0])
	}
	normalizedRight, rightCopied := normalizeDocs(right, opts.StripAttrs, opts.StripFields, opts.Normalize)
	for i := range normalizedRight {
		index.add(1, &normalizedRight[i], i, opts.subjects[1])
	}
	refineGenerated(index.byIdentity, index.entries, index.originals)
	out := make([]pair, 0, len(index.byIdentity))
	for _, entry := range index.byIdentity {
		p := &index.entries[entry]
		if p.Old != nil && p.New != nil {
			from, to := left[p.OldPos].producer(), right[p.NewPos].producer()
			if from.ID != to.ID {
				p.ProducerChange = &producerTransition{
					From: producerSubject(&from, opts.subjects[0]),
					To:   producerSubject(&to, opts.subjects[1]),
				}
				p.Old, p.New = withoutOwnerLabels(p.Old), withoutOwnerLabels(p.New)
			}
		}
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b pair) int {
		return cmp.Or(a.Parent.Compare(b.Parent.NamedResource), a.Resource.Compare(b.Resource))
	})
	slices.SortFunc(index.warnings, manifest.CompareWarning)
	return prepared{
		Pairs:       out,
		Warnings:    index.warnings,
		Subjects:    opts.subjects,
		leftCopied:  leftCopied,
		rightCopied: rightCopied,
	}
}

func (ix *pairIndex) add(
	sideIndex int,
	d *Doc,
	pos int,
	subjects map[manifest.NamedResource]manifest.DiagnosticIdentity,
) {
	resource := manifest.DocIdentity(d.Manifest)
	entry, ok := ix.byIdentity[resource]
	if !ok {
		entry = len(ix.entries)
		ix.entries = append(ix.entries, pair{
			Resource: resource, OldPos: len(ix.originals[0]), NewPos: len(ix.originals[1]),
		})
		ix.byIdentity[resource] = entry
	}
	p := &ix.entries[entry]
	dst, chart, dstPos := &p.Old, &p.OldChart, &p.OldPos
	if sideIndex == 1 {
		dst, chart, dstPos = &p.New, &p.NewChart, &p.NewPos
	}
	original := &ix.originals[sideIndex][pos]
	if *dst != nil {
		prev, next := ix.originals[sideIndex][*dstPos].producer().ID, original.producer().ID
		if prev == next || ix.seenCompetitor(sideIndex, entry, prev, next) {
			parent := d.Parent.String()
			if subject, found := subjects[d.Parent.NamedResource]; found {
				parent = subject.String()
			}
			ix.warnings = append(ix.warnings, manifest.Warning{
				Resource: resource, Category: manifest.WarnDuplicateDocument,
				Message: "duplicate document shadows an earlier document",
				Detail: []string{
					"side: " + string([2]side{sideOrig, sideCurrent}[sideIndex]), "parent: " + parent,
					"parent path: " + d.Parent.Path, "apiVersion: " + manifest.DocAPIVersion(original.Manifest),
				},
			})
		}
		if prev != next && !store.PreferProducer(next, prev) {
			return
		}
	}
	*dst, *chart, *dstPos = d.Manifest, chartLabel(original.Manifest), pos
	p.Parent, p.APIVersion = d.Parent, manifest.DocAPIVersion(d.Manifest)
}

func (ix *pairIndex) seenCompetitor(side, entry int, previous, next manifest.NamedResource) bool {
	if ix.competing[side] == nil {
		ix.competing[side] = make(map[producerOccurrence]bool)
	}
	seen := ix.competing[side]
	candidate := producerOccurrence{entry: entry, id: next}
	duplicate := seen[candidate]
	seen[producerOccurrence{entry: entry, id: previous}] = true
	seen[candidate] = true
	return duplicate
}

func producerSubject(
	actual *manifest.DiagnosticIdentity,
	subjects map[manifest.NamedResource]manifest.DiagnosticIdentity,
) manifest.DiagnosticIdentity {
	if subject, ok := subjects[actual.ID]; ok && actual.ID != (manifest.NamedResource{}) {
		return subject
	}
	return *actual
}

func withoutOwnerLabels(doc map[string]any) map[string]any {
	meta, _ := doc["metadata"].(map[string]any)
	labels, ok := meta["labels"].(map[string]any)
	if !ok {
		return doc
	}
	var retained map[string]any
	for _, key := range [...]string{
		"kustomize.toolkit.fluxcd.io/name", "kustomize.toolkit.fluxcd.io/namespace",
		"resourceset.fluxcd.controlplane.io/name", "resourceset.fluxcd.controlplane.io/namespace",
		"helm.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/namespace",
	} {
		if _, found := labels[key]; found {
			if retained == nil {
				retained = maps.Clone(labels)
			}
			delete(retained, key)
		}
	}
	if retained == nil && len(labels) != 0 {
		return doc
	}
	doc, meta = maps.Clone(doc), maps.Clone(meta)
	doc["metadata"] = meta
	if len(retained) == 0 {
		delete(meta, "labels")
	} else {
		meta["labels"] = retained
	}
	return doc
}
