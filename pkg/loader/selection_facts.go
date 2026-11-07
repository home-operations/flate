package loader

import (
	"hash/maphash"
	"maps"
	"path/filepath"

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

func (l *Loader) comparedFileChanged(evidence *selection.SourceFacts, file string) bool {
	if evidence.Delta == nil {
		return false
	}
	if evidence.Paired && evidence.Input != nil {
		if evidence.ToIndex == "." {
			return evidence.Delta.Contains(filepath.ToSlash(filepath.Clean(file)))
		}
		relative, err := filepath.Rel(evidence.Origin, filepath.Join(l.SourceRoot, filepath.FromSlash(file)))
		return err == nil && evidence.Delta.Contains(filepath.ToSlash(relative))
	}
	return evidence.Delta.Contains(file)
}

// Occurrence borrows exact declaration coordinates and immutable parsed facts.
func (l *Loader) Occurrence(id manifest.NamedResource) (selection.OccurrenceFacts, bool) {
	record, indexed := l.Index.Lookup(id)
	occurrence, catalog := l.catalogOccurrence(id)
	_, located := l.occurrence(id)
	facts := occurrenceView(&occurrence)
	facts.Identity, facts.Record = id, record
	facts.Located, facts.CatalogKnown = located, catalog
	facts.Candidate = l.candidates[id]
	facts.Mounted = l.documents.artifacts[occurrence.Declaration.Source]
	facts.Traversed = l.documents.cache[record.Path].traversed
	if record.Builder != "" {
		facts.PreparedRoot = l.registeredRoot(&record)
	}
	if record.BuilderOwner.ReportHidden() {
		facts.NativeOwner = record.BuilderOwner
	}
	if facts.Catalog == nil {
		facts.Catalog = l.catalogObject(id, &record)
	}
	return facts, indexed || catalog
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

// RootEvidence captures physical support without consulting selection membership.
func (l *Loader) RootEvidence(id manifest.NamedResource, reads *store.ReadSet) selection.RootEvidence {
	evidence := l.rootFacts(id)
	evidence.BuiltChild = l.preparedObject(id)
	if reads == nil && !l.frozen {
		return evidence
	}
	var objects admissionReader = l.Store
	if reads != nil {
		objects = reads
	}
	rootSupport(&evidence, objects, l.Index, &l.roots.currentOnly, l.SourceRoot)
	if reads == nil {
		completeRootSupport(&evidence, objects, l.Index, l.SourceRoot)
	}
	if evidence.PreparedRoot != "" && reads != nil {
		children, current := l.BuiltChildren(evidence.PreparedRoot, evidence.Record.BuilderOwner, reads)
		for i := range children {
			if children[i].Obj.Named() == id {
				evidence.BuiltChild, evidence.BuiltCurrent = children[i], current
				break
			}
		}
	}
	return evidence
}

func (l *Loader) rootFacts(id manifest.NamedResource) selection.RootEvidence {
	record, indexed := l.Index.Lookup(id)
	root := id
	if occurrence, known := l.occurrence(id); known && occurrence.Authored.Namespace == "" && !indexed {
		root.Namespace = ""
		record, indexed = l.Index.Lookup(root)
	}
	var evidence selection.RootEvidence
	if cached, known := l.roots.lookup(root); known {
		evidence = cached.facts
		evidence.StaticRecord, evidence.StaticRecordKnown = evidence.Record, true
	} else if indexed {
		evidence = l.rootAdmission(record.Path, record.File, id.KindInfo().Structural).evidence(l, root, &record).facts
	} else {
		occurrence, _ := l.Occurrence(id)
		evidence = selection.RootEvidence{Identity: id, Record: record, Occurrence: occurrence}
	}
	evidence.Identity, evidence.Record = id, record
	evidence.PreparedRoot = ""
	if record.Builder != "" {
		evidence.PreparedRoot = l.registeredRoot(&record)
	}
	evidence.Scope.Prepared = evidence.PreparedRoot != ""
	evidence.Scope.Records = l.Index.Records
	evidence.Scope.NativeEntry = l.nativeEntry != nil && l.nativeEntry.Named() == id

	return evidence
}

func (l *Loader) rootScopeFacts(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	scope *rootAdmission,
) selection.RootScopeFacts {
	doc := l.documents.cache[record.Path]
	facts := selection.RootScopeFacts{
		Path:        record.Path,
		Eligibility: doc.eligible,
		Records:     l.Index.Records,
		Claims:      l.Index.Claims,
		Loaded:      doc.loaded,
		Bootstrap:   doc.bootstrap,
		Initial:     doc.initial,
		Terminal: l.terminalInput(
			record.Path,
		),
		NamespaceEligible: id == (manifest.NamedResource{}) || id.Namespace != "" || !id.KindInfo().Namespaced(),
		NativeOperand:     l.nativeOperand(record.Path),
		NativeEntry:       l.nativeEntry != nil && l.nativeEntry.Named() == id,
		EntryKustomize:    l.entry == entryKustomize,
		EntryPlain:        l.entry == entryPlain,
		Prepared:          record.Builder != "" && l.registeredRoot(record) != "",
		Candidate:         l.candidates[id].Obj != nil,
		Compact:           l.documents.compact,
		Peers:             scope.peers,
		Owners:            scope.owners,
	}
	object := l.CatalogObject(id)
	if application, valid := manifest.ApplicationFor(object); valid {
		facts.Application = true
		facts.ApplicationSource, facts.ApplicationPath = application.Source, application.Path
		location := SourceView(l.Store, application.Source, l.SourceRoot)
		facts.LocalApplication = !location.Pinned && !location.External
		input, _ := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		source, _ := l.Store.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
		target := filepath.Join(location.Path, manifest.NormalizeClaimBase(application.Path))
		facts.SameEntryOrigin = sameOriginPath(input, source, l.entryPath, target)
	}
	return facts
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
