package loader

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// Catalog borrows immutable loader facts until its owner calls Close.
type Catalog struct {
	index       *manifest.DiscoveryIndex
	locations   map[manifest.NamedResource]authoredOccurrence
	roots       rootInventory
	files       map[string]document
	candidates  map[manifest.NamedResource]store.Child
	projections [64]*map[manifest.BaseManifest]producerFact
	nativeEntry *manifest.Kustomization
	sourceRoot  string
	entry       entryKind
	compact     bool
}

// DetachCatalog keeps borrowed catalog facts alive beyond Loader.Close.
func (l *Loader) DetachCatalog() *Catalog {
	if l.catalog == nil {
		l.catalog = &Catalog{
			index: l.Index, locations: l.locations, roots: l.roots, files: l.documents.cache,
			candidates: l.candidates, nativeEntry: l.nativeEntry,
			sourceRoot: l.SourceRoot, entry: l.entry, compact: l.documents.compact,
		}
		for i := range l.producerFacts {
			l.catalog.projections[i] = l.producerFacts[i].Load()
		}
	}
	return l.catalog
}

// Close releases borrowed catalog storage and is safe to repeat.
func (c *Catalog) Close() {
	if c != nil {
		*c = Catalog{}
	}
}

// CatalogObject borrows the declaration with its effective identity.
//
//nolint:ireturn // Catalog declarations retain their original manifest types.
func (c *Catalog) CatalogObject(id manifest.NamedResource) manifest.BaseManifest {
	if c == nil {
		return nil
	}
	if c.nativeEntry != nil && c.nativeEntry.Named() == id {
		return c.nativeEntry
	}
	record, _ := c.index.Lookup(id)
	if record.Builder != "" && record.Declaration != nil {
		return manifest.CatalogObject(record.Declaration, id)
	}
	if occurrence, known := catalogOccurrence(c.locations, id); known {
		object := occurrence.object
		if object == nil && occurrence.facts != nil {
			object = occurrence.facts.catalog
		}
		if object != nil {
			return manifest.CatalogObject(object, id)
		}
	}
	object := record.Declaration
	if object == nil {
		object = c.candidates[id].Obj
	}
	if object == nil {
		return nil
	}
	if projections := c.projections[producerBucket(object)]; projections != nil {
		if cached, known := (*projections)[object]; known && cached.facts.catalog != nil {
			object = cached.facts.catalog
		}
	}
	return manifest.CatalogObject(object, id)
}

// RootEvidence captures current support without retaining the supplied read set.
func (c *Catalog) RootEvidence(id manifest.NamedResource, reads *store.ReadSet) selection.RootEvidence {
	if c == nil || c.index == nil {
		return selection.RootEvidence{}
	}
	record, indexed := c.index.Lookup(id)
	root := id
	if !indexed && id.Namespace != "" {
		if occurrence, known := catalogOccurrence(c.locations, id); known && occurrence.Authored.Namespace == "" {
			root.Namespace = ""
			record, indexed = c.index.Lookup(root)
		}
	}
	var evidence selection.RootEvidence
	if cached, known := c.roots.lookup(root); known {
		evidence = cached.view()
		evidence.StaticRecord = &cached.record
	} else if indexed {
		doc := c.files[record.Path]
		occurrence, _ := catalogOccurrence(c.locations, id)
		evidence = selection.RootEvidence{
			Identity: id, Record: selection.RootRecordFrom(&record), Occurrence: occurrenceView(&occurrence),
			Scope: selection.RootScopeFacts{
				Path:           record.Path,
				Eligibility:    doc.eligible,
				Owners:         doc.rootOwners,
				Peers:          doc.rootPeers,
				Records:        c.index.Records,
				Claims:         c.index.Claims,
				Loaded:         doc.loaded,
				Bootstrap:      doc.bootstrap,
				Initial:        doc.initial,
				Terminal:       doc.terminal,
				NativeOperand:  doc.rootNative,
				Compact:        c.compact,
				EntryKustomize: c.entry == entryKustomize,
				Candidate:      c.candidates[id].Obj != nil || record.Declaration != nil,
			},
		}
	}
	evidence.Identity, evidence.Record = id, selection.RootRecordFrom(&record)
	evidence.Scope.Records = c.index.Records
	object := c.CatalogObject(id)
	evidence.Scope.NamespaceEligible = object != nil && manifest.AdmitsRoot(object)
	if reads != nil {
		rootSupport(&evidence, reads, c.index, &c.roots.currentOnly, c.sourceRoot)
		completeRootSupport(&evidence, reads, c.index)
		evidence.Stale = !reads.Valid()
	}
	return evidence
}

// SourceExternal uses the caller's capture so replacement invalidates the observation.
func (c *Catalog) SourceExternal(id manifest.NamedResource, reads *store.ReadSet) bool {
	return c != nil && reads != nil && SourceView(reads, id, c.sourceRoot).External
}
