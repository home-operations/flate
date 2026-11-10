package loader

import (
	"hash/maphash"
	"maps"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) selectionContext() selection.SourceContext {
	side, known := l.comparisonSide()
	roots := l.scopeRoots
	if l.scopes != nil {
		roots = l.scopes.rootFacts
	}
	return selection.SourceContext{
		Root:                l.SourceRoot,
		EntryPath:           l.entryPath,
		NativeEntry:         l.entry == entryKustomize,
		PlainEntry:          l.entry == entryPlain,
		Frozen:              l.frozen,
		SideKnown:           known,
		Side:                side,
		Roots:               roots,
		InputReaders:        l.inputReaders,
		InputObjects:        l.inputObjects,
		TemplateProjections: l.templateProjections,
	}
}

// ProducerFacts reuses immutable projections across concurrent preparations.
func (l *Loader) ProducerFacts(object manifest.BaseManifest) selection.ProducerFacts {
	bucket := &l.producerFacts[producerBucket(object)]
	previous := bucket.Load()
	if previous != nil {
		if cached, known := (*previous)[object]; known {
			occurrence := authoredOccurrence{
				Identity: cached.identity,
				Authored: object.Named(),
				object:   object,
				facts:    cached.facts,
			}
			return selection.ProducerFacts{Declaration: object, Occurrence: occurrenceView(&occurrence)}
		}
	}
	projection, _ := manifest.ProjectProducer(object, nil)
	occurrence := compactOccurrence(object, projection)
	occurrence.object, occurrence.Authored = object, object.Named()
	for {
		var next map[manifest.BaseManifest]producerFact
		if previous == nil {
			next = make(map[manifest.BaseManifest]producerFact, 1)
		} else {
			next = maps.Clone(*previous)
		}
		next[object] = producerFact{identity: occurrence.Identity, facts: occurrence.facts}
		if bucket.CompareAndSwap(previous, &next) {
			return selection.ProducerFacts{Declaration: object, Occurrence: occurrenceView(&occurrence), Derived: true}
		}
		previous = bucket.Load()
		if cached, known := (*previous)[object]; known {
			occurrence.Identity, occurrence.facts = cached.identity, cached.facts
			return selection.ProducerFacts{Declaration: object, Occurrence: occurrenceView(&occurrence)}
		}
	}
}

// SelectionIndex borrows the loader-owned census; callers must not mutate it.
func (l *Loader) SelectionIndex() *manifest.DiscoveryIndex {
	return l.Index
}

// Occurrence borrows exact declaration coordinates and immutable parsed facts.
func (l *Loader) Occurrence(id manifest.NamedResource) (selection.OccurrenceFacts, bool) {
	record, indexed := l.Index.Lookup(id)
	occurrence, catalog := l.catalogOccurrence(id)
	located := catalog && !occurrence.requiresBuild && occurrence.Identity.Name == occurrence.Authored.Name
	facts := occurrenceView(&occurrence)
	facts.Identity = id
	facts.Located, facts.CatalogKnown = located, catalog
	facts.Mounted = l.documents.artifacts[occurrence.Declaration.Source]
	facts.Traversed = l.documents.cache[record.Path].traversed
	if facts.Catalog == nil {
		view := l.physicalView()
		facts.Catalog = view.object(id, &record, l)
	}
	return facts, indexed || catalog
}

// AdmissionCandidate borrows the immutable declaration prepared during construction.
func (l *Loader) AdmissionCandidate(id manifest.NamedResource) store.Child {
	return l.candidates[id]
}

// OccurrenceRoots borrows the parsed render paths in the effective source namespace.
//
//nolint:gocritic // The census method returns a source and its immutable paths.
func (l *Loader) OccurrenceRoots(id manifest.NamedResource) (manifest.NamedResource, []string) {
	occurrence, known := l.catalogOccurrence(id)
	if !known || occurrence.facts == nil || len(occurrence.facts.roots) == 0 {
		return manifest.NamedResource{}, nil
	}
	source, _, _ := occurrence.SourcePath()
	return source, occurrence.facts.roots
}

func occurrenceView(occurrence *authoredOccurrence) selection.OccurrenceFacts {
	view := selection.OccurrenceFacts{
		Declaration: selection.OccurrenceID{
			Source: occurrence.Declaration.Source, File: occurrence.Declaration.File,
			Document: occurrence.Declaration.Document, Item: occurrence.Declaration.item,
		},
		Identity: occurrence.Identity, Authored: occurrence.Authored,
		Object: occurrence.object, RequiresBuild: occurrence.requiresBuild,
	}
	if facts := occurrence.facts; facts != nil {
		view.Catalog = facts.catalog
		view.Source, view.Path, view.HasPath = facts.source, facts.path, facts.hasPath
		view.Namespaced, view.Uncertain = facts.namespaced, facts.uncertain
		view.Retained, view.Structural, view.NeedsCandidates = facts.retained, facts.structural, facts.needsCandidates
		view.Labels, view.Labeled, view.Witness = facts.labels, facts.labeled, facts
		view.Refs, view.Reverse = facts.refs, facts.reverse
		view.TemplateInputs, view.TemplateObjects = facts.templateInputs, facts.templateObjects
		view.Roots, view.Targets, view.AuthoredTargets = facts.roots, facts.targets, facts.authoredTargets
		view.Policy = facts.policy
	} else if occurrence.object != nil {
		view.Source, view.Path, view.HasPath = occurrence.object.SourcePath()
		view.Namespaced = manifest.KindOf(occurrence.object).Namespaced()
		view.Catalog = occurrence.object
	}
	return view
}

// RootEvidence with completeOwners false requires the caller to complete support with its preparation inventory.
func (l *Loader) RootEvidence(
	id manifest.NamedResource,
	reads *store.ReadSet,
	completeOwners bool,
) selection.RootEvidence {
	view := l.physicalView()
	return view.evidence(id, reads, l, completeOwners)
}

func physicalOccurrence(view *selection.OccurrenceFacts) authoredOccurrence {
	facts, _ := view.Witness.(*occurrenceFacts)
	return authoredOccurrence{
		Declaration: occurrenceID{
			Source: view.Declaration.Source, File: view.Declaration.File,
			Document: view.Declaration.Document, item: view.Declaration.Item,
		},
		Identity: view.Identity, Authored: view.Authored, object: view.Object,
		facts: facts, requiresBuild: view.RequiresBuild,
	}
}

//nolint:gochecknoglobals // Immutable process seed distributes producer identities across the existing cache buckets.
var producerFactSeed = maphash.MakeSeed()

func producerBucket(object manifest.BaseManifest) uint64 {
	return maphash.Comparable(producerFactSeed, object) % uint64(len(Catalog{}.projections))
}

func (l *Loader) comparisonSide() (manifest.TreeSide, bool) {
	if l.sideKnown {
		return l.side, true
	}
	if l.Options.Changes != nil {
		input := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		_, snapshots, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == input {
				return manifest.TreeSide(side), true
			}
		}
	}
	return 0, false
}
