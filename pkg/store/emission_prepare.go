package store

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

func prepareEmission(next, prev *Emission, prepared []admission, byID map[manifest.NamedResource]int) *Emission {
	if prev != nil && next.Complete && !prev.OutputReads.Valid() {
		{
			oldValues3 := prev.entries
			for oldIndex3 := range oldValues3 {
				old := oldValues3[oldIndex3]
				if i, ok := byID[old.child.Obj.Named()]; ok {
					prepared[i].objectHead, prepared[i].objectGeneration = old.objectHead, old.objectGeneration
					prepared[i].objectChanged = !reconcileEqual(prepared[i].child, old.child)
				}
			}
		}
		prev = nil
	}
	e := *next
	e.entriesOwned = false
	e.validated = new(atomic.Pointer[manifest.BaseManifest])
	e.shadowed = nil
	if prev != nil {
		e.shadowed = slices.Clone(prev.shadowed)
	}
	if (next.Complete || len(next.Failures) != 0) && !e.shadows(new(next.binding)) {
		e.shadowed = append(e.shadowed, next.binding)
	}
	e.previous = nil
	capacity := len(prepared)
	if prev != nil && !next.Complete && len(next.Failures) == 0 {
		capacity += len(prev.entries)
	}
	e.entries = make([]admission, 0, capacity)
	e.Children, e.payloadReads, e.borrowed = nil, nil, nil
	e.Manifests = slices.Clone(next.Manifests)
	e.Failures = slices.Clone(next.Failures)
	e.Unreadable = slices.Clone(next.Unreadable)
	clear(byID)
	{
		aValues5 := prepared
		for i := range aValues5 {
			a := aValues5[i]
			byID[a.child.Obj.Named()] = i
		}
	}
	// Stable positions retain reverse membership offsets independently of document order.
	if prev != nil {
		{
			oldValues4 := prev.entries
			for oldIndex4 := range oldValues4 {
				old := oldValues4[oldIndex4]
				id := old.child.Obj.Named()
				if i, ok := byID[id]; ok {
					entry := prepared[i]
					if !next.Complete && entry.child.Generated && old.child.Doc != nil && !old.child.Generated {
						entry = old
					}
					entry.changed = entry.child.Generated != old.child.Generated ||
						!manifest.Equal(entry.child.Obj, old.child.Obj) ||
						!manifest.ValueEqual(entry.child.Doc, old.child.Doc)
					entry.objectChanged = !reconcileEqual(entry.child, old.child)
					entry.objectHead = old.objectHead
					entry.objectGeneration = old.objectGeneration
					e.entries = append(e.entries, entry)
					delete(byID, id)
				} else if !next.Complete && len(next.Failures) == 0 && !old.child.Generated {
					old.changed, old.objectChanged = false, false
					e.entries = append(e.entries, old)
				}
			}
		}
	}
	{
		aValues6 := prepared
		for aIndex4 := range aValues6 {
			a := aValues6[aIndex4]
			id := a.child.Obj.Named()
			if _, ok := byID[id]; ok {
				a.changed = true
				e.entries = append(e.entries, a)
				delete(byID, id)
			}
		}
	}
	if len(e.entries) > 0 {
		e.Children = make([]Child, len(e.entries))
		{
			aValues7 := e.entries
			for i := range aValues7 {
				a := aValues7[i]
				e.Children[i] = a.child
				if reads := a.child.output; reads != nil {
					if e.payloadReads == nil {
						e.payloadReads = reads
					} else if e.payloadReads != reads {
						if e.borrowed == nil {
							e.borrowed = make(map[*ReadSet]struct{})
						}
						e.borrowed[reads] = struct{}{}
					}
				}
			}
		}
	}
	inherited := prev != nil &&
		slices.EqualFunc(next.Manifests, prev.Manifests, func(a, b manifest.DocOccurrence) bool {
			return a.Ordinal == b.Ordinal && manifest.ValueEqual(a.Document, b.Document)
		})
	if inherited &&
		(!next.Complete || slices.EqualFunc(prev.entries, e.entries, func(a, b admission) bool { return a.child.Obj.Named() == b.child.Obj.Named() })) {
		e.documentOwners = prev.documentOwners
	} else {
		e.documentOwners = bindDocuments(next.Manifests, prepared)
	}
	if !next.Complete && len(next.Failures) == 0 && prev != nil {
		if !inherited {
			visited := make(map[manifest.NamedResource]struct{}, len(prepared))
			{
				entryValues5 := prepared
				for entryIndex4 := range entryValues5 {
					entry := entryValues5[entryIndex4]
					visited[entry.child.Obj.Named()] = struct{}{}
				}
			}
			available := make(map[manifest.NamedResource][]int, len(e.Manifests))
			for i, doc := range e.Manifests {
				key := manifest.DocIdentity(doc.Document)
				available[key] = append(available[key], i)
			}
			used := make([]bool, len(e.Manifests))
			for i, doc := range prev.Manifests {
				owner := prev.documentOwners[i]
				if _, replaced := visited[owner]; owner != (manifest.NamedResource{}) && replaced {
					continue
				}
				present := false
				for _, j := range available[manifest.DocIdentity(doc.Document)] {
					if !used[j] && manifest.ValueEqual(doc.Document, e.Manifests[j].Document) {
						used[j], present = true, true
						break
					}
				}
				if !present {
					e.Manifests = append(e.Manifests, doc)
					e.documentOwners = append(e.documentOwners, owner)
				}
			}
		}
		e.coverage = slices.Clone(e.coverage)
		{
			oldValues5 := prev.coverage
			for oldIndex5 := range oldValues5 {
				old := oldValues5[oldIndex5]
				if !slices.ContainsFunc(e.coverage, func(f footprint) bool {
					return f.id == old.id && slices.Equal(f.record.Productions, old.record.Productions)
				}) {
					e.coverage = append(e.coverage, old)
				}
			}
		}
	}
	return &e
}

func bindDocuments(docs []manifest.DocOccurrence, entries []admission) []manifest.NamedResource {
	byDocument := make(map[manifest.NamedResource][]admission, len(entries))
	{
		entryValues6 := entries
		for entryIndex5 := range entryValues6 {
			entry := entryValues6[entryIndex5]
			if entry.child.Doc != nil {
				key := manifest.DocIdentity(entry.child.Doc)
				byDocument[key] = append(byDocument[key], entry)
			}
		}
	}
	owners := make([]manifest.NamedResource, len(docs))
	for i, doc := range docs {
		{
			entryValues7 := byDocument[manifest.DocIdentity(doc.Document)]
			for entryIndex6 := range entryValues7 {
				entry := entryValues7[entryIndex6]
				if manifest.ValueEqual(doc.Document, entry.child.Doc) {
					owners[i] = entry.child.Obj.Named()
					break
				}
			}
		}
	}
	return owners
}

func sameMembership(a, b *Emission) bool {
	if a.family != b.family || len(a.entries) != len(b.entries) ||
		!slices.EqualFunc(a.shadowed, b.shadowed, func(a, b sourceBinding) bool { return a.equal(&b) }) ||
		!a.binding.equal(&b.binding) ||
		!slices.EqualFunc(a.coverage, b.coverage, func(a, b footprint) bool {
			return a.id == b.id && slices.Equal(a.record.Productions, b.record.Productions)
		}) {
		return false
	}
	{
		xValues := a.entries
		for i := range xValues {
			x := xValues[i]
			y := b.entries[i]
			if x.child.Obj.Named() != y.child.Obj.Named() || x.child.Generated != y.child.Generated ||
				!x.binding.equal(&y.binding) ||
				!slices.Equal(x.child.Productions, y.child.Productions) ||
				!manifest.LabelsEqual(
					x.child.Obj,
					y.child.Obj,
					[2]map[string]any{x.child.Doc, y.child.Doc},
					ownershipLabel,
				) ||
				!slices.EqualFunc(
					x.targets,
					y.targets,
					func(a, b manifest.ProducerTarget) bool { return a.NamedResource == b.NamedResource },
				) {
				return false
			}
		}
	}
	return true
}

//nolint:gocritic // The slices callback contract requires value parameters.
func admissionEqual(a, b admission) bool {
	return a.binding.equal(&b.binding) && childEqual(a.child, b.child)
}

func childEqual(a, b Child) bool {
	return a.Generated == b.Generated && manifest.Equal(a.Obj, b.Obj) && manifest.ValueEqual(a.Doc, b.Doc) &&
		slices.Equal(a.Productions, b.Productions) &&
		a.payload == b.payload &&
		a.output == b.output
}

func semanticEqual(a, b Child) bool {
	if a.Doc == nil || b.Doc == nil {
		return semanticObjectsEqual(a, b)
	}
	if len(a.Doc) != len(b.Doc) {
		return false
	}
	for key, value := range a.Doc {
		other, ok := b.Doc[key]
		if !ok {
			return false
		}
		if key != "metadata" {
			if !manifest.ValueEqual(value, other) {
				return false
			}
			continue
		}
		am, aok := value.(map[string]any)
		bm, bok := other.(map[string]any)
		if !aok || !bok {
			return manifest.ValueEqual(value, other)
		}
		if !manifest.MapEqualExcept(am, bm, func(key string) bool { return key == "labels" }) {
			return false
		}
		al, _ := am["labels"].(map[string]any)
		bl, _ := bm["labels"].(map[string]any)
		if !manifest.MapEqualExcept(al, bl, ownershipLabel) {
			return false
		}
	}
	return true
}

func ownershipLabel(key string) bool {
	switch key {
	case "kustomize.toolkit.fluxcd.io/name",
		"kustomize.toolkit.fluxcd.io/namespace",
		"helm.toolkit.fluxcd.io/name",
		"helm.toolkit.fluxcd.io/namespace",
		"resourceset.fluxcd.controlplane.io/name",
		"resourceset.fluxcd.controlplane.io/namespace":
		return true
	}
	return false
}

func semanticObjectsEqual(a, b Child) bool {
	if manifest.Equal(a.Obj, b.Obj) {
		return true
	}
	type metadata interface {
		GetLabels() map[string]string
		GetAnnotations() map[string]string
	}
	am, aok := a.Obj.(metadata)
	bm, bok := b.Obj.(metadata)
	if !aok || !bok {
		return false
	}
	if !maps.Equal(am.GetAnnotations(), bm.GetAnnotations()) {
		return false
	}
	if !manifest.MapEqualExcept(am.GetLabels(), bm.GetLabels(), ownershipLabel) {
		return false
	}
	ad, aerr := json.Marshal(a.Obj)
	bd, berr := json.Marshal(b.Obj)
	return aerr == nil && berr == nil && bytes.Equal(ad, bd)
}
