package loader

import (
	"path/filepath"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Catalog borrows immutable loader facts until its owner calls Close.
type Catalog struct {
	physicalView

	EntryScopes   *selection.EntryScopes
	buckets       [64]*map[manifest.BaseManifest]*producerFact
	retainedRoots rootInventory
	provider      func(manifest.NamedResource, *store.ReadSet, *manifest.DiscoveryIndex) store.SourceProvider
}

type physicalView struct {
	index       *manifest.DiscoveryIndex
	locations   map[manifest.NamedResource]authoredOccurrence
	roots       *rootInventory
	files       map[string]document
	candidates  map[manifest.NamedResource]store.Child
	projections *[64]*map[manifest.BaseManifest]*producerFact
	nativeEntry *manifest.Kustomization
	sourceRoot  string
	inventory   string
	entry       entryKind
	compact     bool
	eager       bool
}

// DetachCatalog keeps borrowed catalog facts alive beyond Loader.Close.
func (l *Loader) DetachCatalog() *Catalog {
	if l.catalog == nil {
		l.catalog = &Catalog{
			physicalView: l.physicalView(), provider: l.SourceProvider, EntryScopes: l.entryScopes.Load(),
		}
		l.catalog.retainedRoots = l.roots
		l.catalog.roots = &l.catalog.retainedRoots
		l.catalog.projections = &l.catalog.buckets
		for i := range l.producerFacts {
			l.catalog.projections[i] = l.producerFacts[i].Load()
		}
	}
	return l.catalog
}

// Close releases borrowed catalog storage and is safe to repeat.
func (c *Catalog) Close() {
	if c != nil {
		retireEntryScopes(c.EntryScopes)
		*c = Catalog{}
	}
}

// CatalogObject borrows the declaration with its effective identity.
func (c *Catalog) CatalogObject(id manifest.NamedResource) manifest.BaseManifest {
	if c == nil {
		return nil
	}
	record, _ := c.index.Lookup(id)
	return c.object(id, &record, nil)
}

// RootEvidence MUST remain physical evidence until selection.CaptureRoot enriches it.
func (c *Catalog) RootEvidence(
	id manifest.NamedResource,
	reads *store.ReadSet,
) selection.RootEvidence {
	if c == nil || c.index == nil {
		return selection.RootEvidence{}
	}
	facts := c.evidence(id, reads, nil)
	facts.EntryScope = c.EntryScopes.Scope(id)
	return facts
}

func (l *Loader) physicalView() physicalView {
	inventory := ""
	if _, split := l.entryScope(); split {
		inventory = l.entryPath
	}
	return physicalView{
		index: l.Index, locations: l.locations, roots: &l.roots, files: l.documents.cache,
		candidates: l.candidates, nativeEntry: l.nativeEntry, inventory: inventory,
		sourceRoot: l.SourceRoot, entry: l.entry, compact: l.documents.compact, eager: l.documents.eager,
	}
}

func (v *physicalView) object(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	live *Loader,
) manifest.BaseManifest {
	if v.nativeEntry != nil && v.nativeEntry.Named() == id {
		return v.nativeEntry
	}
	if record.Builder != "" && record.Declaration != nil {
		return v.projectObject(id, record.Declaration, nil, live)
	}
	var object, projection manifest.BaseManifest
	if occurrence, known := catalogOccurrence(v.locations, id); known {
		object = occurrence.object
		if occurrence.facts != nil {
			projection = occurrence.facts.catalog
		}
		if object == nil {
			object = projection
		}
	}
	if object == nil {
		object = record.Declaration
	}
	if object == nil {
		object = v.candidates[id].Obj
	}
	if object == nil && live != nil {
		object = live.IndexedObject(id, *record).Obj
		if object == nil {
			object = live.Store.GetObject(id)
		}
	}
	return v.projectObject(id, object, projection, live)
}

func (v *physicalView) projectObject(
	id manifest.NamedResource,
	object, projection manifest.BaseManifest,
	live *Loader,
) manifest.BaseManifest {
	if object == nil {
		return nil
	}
	// Catalog projection MUST not imply that authored data is available.
	if projection != nil {
		return catalogIdentity(projection, id)
	}
	bucket := producerBucket(object)
	var projections *map[manifest.BaseManifest]*producerFact
	if live != nil {
		projections = live.producerFacts[bucket].Load()
	} else if v.projections != nil {
		projections = v.projections[bucket]
	}
	if projections != nil {
		if cached, known := (*projections)[object]; known && cached.facts.catalog != nil {
			object = cached.facts.catalog
		}
	}
	return catalogIdentity(object, id)
}

func catalogIdentity(object manifest.BaseManifest, id manifest.NamedResource) manifest.BaseManifest {
	if object.Named() == id {
		return object
	}
	return manifest.CatalogObject(object, id)
}

func (v *physicalView) evidence(
	id manifest.NamedResource,
	reads *store.ReadSet,
	live *Loader,
) selection.RootEvidence {
	evidence := v.rootFacts(id, live, nil)
	var objects sourceStore = reads
	if reads == nil {
		objects = nil
		if live != nil && live.frozen {
			objects = live.Store
		}
	}
	if objects != nil {
		v.applicationScope(&evidence, objects)
	}
	if live != nil {
		evidence.BuiltChild, evidence.BuiltCurrent = live.PreparedObject(id, reads)
	}
	if reads != nil {
		evidence.Stale = !reads.Valid()
	}
	return evidence
}

func (v *physicalView) applicationScope(evidence *selection.RootEvidence, objects sourceStore) {
	if !evidence.Scope.Application {
		return
	}
	input, _ := objects.GetArtifact(evidence.Scope.ApplicationSource, store.ArtifactContent).(*store.SourceArtifact)
	evidence.Scope.LocalApplication = input.WorkingTree()
	evidence.Scope.InputApplication = input != nil && input.Provider == store.ProviderInput
	if evidence.Scope.LocalApplication && input.Provider == store.ProviderAlias {
		origin, known := input.IndexPath(v.sourceRoot, ".")
		evidence.Scope.LocalApplication = known && filepath.IsLocal(origin)
	}
	evidence.Scope.Claim = ""
	if coordinate, known := input.IndexPath(v.sourceRoot, evidence.Scope.ApplicationPath); known {
		evidence.Scope.Claim = coordinate + "/"
	} else {
		evidence.Scope.LocalApplication = false
	}
}

func (v *physicalView) rootFacts(
	id manifest.NamedResource,
	live *Loader,
	declaration manifest.BaseManifest,
) selection.RootEvidence {
	record, indexed := v.index.Lookup(id)
	root := id
	if !indexed && id.Namespace != "" {
		occurrence, known := catalogOccurrence(v.locations, id)
		located := known && !occurrence.requiresBuild &&
			occurrence.Identity.Name == occurrence.Authored.Name && occurrence.Authored.Namespace == ""
		if located {
			root.Namespace = ""
			record, indexed = v.index.Lookup(root)
		}
	}
	object := declaration
	if object == nil {
		object = v.object(id, &record, live)
	}
	var evidence selection.RootEvidence
	if cached, known := v.roots.lookup(root); known {
		evidence = cached.view()
		evidence.StaticRecord = &cached.record
	} else {
		doc := v.files[record.Path]
		admission := rootAdmission{owners: doc.rootOwners, peers: doc.rootPeers}
		if live != nil && indexed && (!doc.rootKnown || id.KindInfo().Discovery) {
			admission = live.rootAdmission(record.Path, record.File, id.KindInfo().Structural)
		}
		snapshot := v.rootScope(live, &rootScopeInput{
			identity: id, record: record, admission: admission, declaration: object,
		})
		if !indexed {
			snapshot.occurrence, _ = catalogOccurrence(v.locations, id)
		}
		evidence = snapshot.view()
	}
	if !evidence.Scope.PackageRoot {
		evidence.Scope.NamespaceEligible = rootNamespaceEligible(id, object)
	}
	evidence.Identity, evidence.Record = id, selection.RootRecordFrom(&record)
	setViewIdentity(&evidence.Occurrence, id)
	evidence.Scope.Records = v.index.Records
	evidence.Scope.NativeEntry = v.nativeEntry != nil && v.nativeEntry.Named() == id
	if live != nil && record.Builder != "" {
		evidence.PreparedRoot = live.registeredRoot(id, &record)
	}
	if live != nil && !live.frozen {
		evidence.Scope.Prepared = evidence.PreparedRoot != ""
	}
	return evidence
}

type rootScopeInput struct {
	identity    manifest.NamedResource
	record      manifest.DiscoveryRecord
	admission   rootAdmission
	declaration manifest.BaseManifest
	candidate   bool
	records     map[manifest.NamedResource]manifest.DiscoveryRecord
}

func (v *physicalView) rootScope(live *Loader, input *rootScopeInput) rootEvidence {
	id, record := input.identity, &input.record
	doc := v.files[record.Path]
	object := input.declaration
	if object == nil {
		object = v.object(id, record, live)
	}
	var occurrence authoredOccurrence
	if record.Path != "" {
		occurrence, _ = catalogOccurrence(v.locations, id)
		if live != nil {
			occurrence = live.initialOccurrence(id, record)
		}
	}
	terminal, native, prepared := doc.terminal, doc.rootNative, v.roots.prepared[id]
	if live != nil {
		terminal, native = live.terminalInput(record.Path), live.nativeOperand(record.Path)
		if !live.frozen {
			prepared = record.Builder != "" && live.registeredRoot(id, record) != ""
		}
	}
	evidence := rootEvidence{
		identity: id, occurrence: occurrence, record: selection.RootRecordFrom(record),
		scope: selection.RootScopeFacts{
			Path: record.Path, Eligibility: doc.eligible, Records: v.index.Records, Claims: v.index.Claims,
			Loaded: doc.loaded, Bootstrap: doc.bootstrap, Initial: doc.initial, Terminal: terminal,
			NativeOperand: native, NativeEntry: v.nativeEntry != nil && v.nativeEntry.Named() == id,
			EntryKustomize: v.entry == entryKustomize, Compact: v.compact && !v.eager,
			OutsideInventory: v.inventory != "" && record.Path != "" && !safepath.Contains(v.inventory, record.Path),
			Owners:           input.admission.owners, Peers: input.admission.peers,
			Candidate: v.candidates[id].Obj != nil,
		},
	}
	previous, known := v.roots.lookup(id)
	switch {
	case !known, live == nil:
	case prepared:
		evidence = *previous
	case !doc.initial:
		evidence.initialScope = new(previous.scope)
		evidence.initialOccurrence = new(occurrenceView(previous.currentOccurrence()))
		if selection.Root(previous.view()).Independent {
			evidence.current = new(evidence.occurrence)
			evidence.occurrence = previous.occurrence
		}
	}
	evidence.scope.Prepared = prepared
	evidence.scope.Candidate = evidence.scope.Candidate || input.candidate
	evidence.scope.NamespaceEligible = rootNamespaceEligible(id, object)
	if input.records != nil {
		evidence.scope.Records = input.records
	}
	v.captureApplication(live, object, &evidence, &doc)
	return evidence
}

func (v *physicalView) captureApplication(
	live *Loader,
	object manifest.BaseManifest,
	evidence *rootEvidence,
	doc *document,
) {
	id, prepared := evidence.identity, evidence.scope.Prepared
	application, valid := manifest.ApplicationFor(object)
	if prepared {
		if authored, located := catalogOccurrence(v.locations, id); located {
			application.Source, application.Path, valid = authored.SourcePath()
		}
	}
	if !valid {
		evidence.scope.Application = false
		return
	}
	evidence.scope.Application = true
	evidence.scope.ApplicationSource, evidence.scope.ApplicationPath = application.Source, application.Path
	retained, known := doc.rootApplications[id]
	if live != nil && (!live.frozen || !known) {
		retained = live.rootFactsForApplication(&application)
	}
	evidence.scope.LocalApplication, evidence.scope.InputApplication = retained.local, retained.input
	evidence.scope.SameEntryOrigin = retained.sameOrigin
}

func (l *Loader) rootFactsForApplication(application *manifest.EntryApplication) rootApplication {
	input, _ := l.Store.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	target := input.ApplicationOrigin(application.Path)
	if target == "" {
		return rootApplication{}
	}
	bootstrap := l.Store.BootstrapInput(l.Options.ConfiguredInput)
	return rootApplication{
		local:      true,
		input:      input.Provider == store.ProviderInput,
		sameOrigin: bootstrap.MatchesOrigin(l.entryPath, target),
	}
}

func rootNamespaceEligible(id manifest.NamedResource, object manifest.BaseManifest) bool {
	return id == (manifest.NamedResource{}) || object != nil && manifest.AdmitsRoot(object)
}

type rootApplication struct {
	local      bool
	input      bool
	sameOrigin bool
}

// SourceExternal uses the caller's capture so replacement invalidates the observation.
func (c *Catalog) SourceExternal(id manifest.NamedResource, reads *store.ReadSet) bool {
	return c != nil && reads != nil && c.provider != nil && c.provider(id, reads, c.index) == store.ProviderFetch
}
