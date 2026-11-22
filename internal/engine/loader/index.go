package loader

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// FinalizeIndex MUST freeze source-qualified graph contributions before reconciliation.
func (l *Loader) FinalizeIndex(ctx context.Context, repoRoot string, freeze bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.configureDocuments()
	if l.documents.compact && l.frozen {
		return nil
	}
	l.SourceRoot = cmp.Or(l.SourceRoot, repoRoot)
	l.mountSources()
	var err error
	if freeze {
		err = l.PersistInputs(ctx)
	}
	if err != nil {
		return err
	}
	if err := l.prepareIndexScopes(ctx, repoRoot, freeze); err != nil {
		return err
	}
	if l.documents.compact && l.indexDerived && l.derived == l.construction {
		if freeze {
			l.freezeConstruction()
		}
		return nil
	}
	l.resetDerivedProductions()
	if err := l.resolveAuthored(ctx, repoRoot); err != nil {
		return err
	}
	emittedFiles := map[string][]manifest.NamedResource{}
	if err := l.buildProductions(ctx, repoRoot, emittedFiles); err != nil {
		return err
	}

	if err := l.finalizeRecords(ctx, repoRoot, emittedFiles); err != nil {
		return err
	}
	if err := l.resolvePaths(ctx); err != nil {
		return err
	}
	l.retainUnknownOwnerReach()
	if err := l.completeIndexInputs(repoRoot); err != nil {
		return err
	}

	if l.documents.compact {
		l.derived, l.indexDerived = l.construction, true
		if freeze {
			l.freezeConstruction()
		}
		return nil
	}
	if freeze {
		l.seedSourceAuthority()
	}
	l.Store.SetDiscoveryIndex(l.Index)
	if freeze {
		l.capturePreparedRoots()
		l.frozen = true
	}
	l.scopeRoots = l.scopes.rootFacts
	l.scopes = nil
	return nil
}

func (l *Loader) freezeConstruction() {
	l.capturePreparedRoots()
	l.seedSourceAuthority()
	l.inputReaders = map[manifest.NamedResource][]manifest.NamedResource{}
	for _, object := range l.Store.Objects() {
		id := object.Named()
		if _, indexed := l.Index.Lookup(id); indexed {
			continue
		}
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if !indexesReferenceReader(&ref) {
				continue
			}
			if !slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
	l.publishFrozenRecords()
}

func (l *Loader) capturePreparedRoots() {
	if len(l.builtRoots) == 0 {
		return
	}
	// Detached membership MUST retain the frozen fact without borrowing build captures.
	l.roots.prepared = make(map[manifest.NamedResource]bool, len(l.builtRoots))
	for id := range l.builtRoots {
		record, _ := l.Index.Lookup(id)
		l.roots.prepared[id] = l.registeredRoot(id, &record) != ""
	}
}

// Source initialization MUST run at a serial construction boundary.
func (l *Loader) seedSourceAuthority() {
	if l.frozen {
		return
	}
	l.side, l.sideKnown = l.comparisonSide()
	for i := range l.bindings {
		for binding := l.bindings[i].Load(); binding != nil; binding = binding.next {
			binding.visited = false
		}
	}
	var binding *sourceBinding
	seed := func(source manifest.NamedResource) {
		binding = findSourceBinding(l.sourceBucket(source).Load(), source)
		if binding != nil && binding.visited {
			return
		}
		facts := l.admissionSourceFacts(source, nil, true)
		binding = findSourceBinding(l.sourceBucket(facts.Source).Load(), facts.Source)
		binding.visited = true
	}
	seed(manifest.BootstrapSourceID)
	//nolint:gocritic // Source references MUST be read without a second map lookup.
	for _, record := range l.Index.Records {
		for _, ref := range record.Refs {
			if binding.source == ref {
				continue
			}
			if ref.KindInfo().IsSource() {
				seed(ref)
			}
		}
	}
	for source := range l.Index.CompleteSources {
		seed(source)
	}
	for source := range l.Options.Changes.Sources() {
		seed(source)
	}
}

// Retire releases admission readers while preserving completed owners' read tracking.
func (l *Loader) Retire() {
	l.constructionPurpose.Store(nil)
	l.InputContent = nil
	if l.Options.Session == nil {
		l.retireBuiltBatches()
		l.inputReaders, l.inputObjects = nil, nil
		l.templateProjections = nil
		for i := range l.producerFacts {
			l.producerFacts[i].Store(nil)
		}
		l.documents = documentReader{}
		l.facts, l.scopes, l.Options.ParsedInputs, l.snapshot = nil, nil, nil, nil
		l.candidates, l.admitted, l.locations = nil, nil, nil
		l.roots = rootInventory{}
		for i := range l.bindings {
			l.bindings[i].Store(nil)
		}
		l.projections.Clear()
		return
	}
}

// SweepPreparations MUST run at a serial construction or quiescent runtime boundary.
func (l *Loader) SweepPreparations() {
	if l == nil {
		return
	}
	l.sweepProducerFacts()
	l.sweepSourceBindings()
	removed := false
	for owner, batch := range l.builtBatches {
		if !batch.input.Support.Valid() {
			l.Store.Retire(batch.input.Support)
			delete(l.builtBatches, owner)
			removed = true
		}
	}
	if removed {
		l.refreshNativeBuildFacts("", true)
	}
}

func (l *Loader) retireBuiltBatches() {
	for _, batch := range l.builtBatches {
		l.Store.Retire(batch.input.Support)
	}
	clear(l.builtBatches)
}

func (l *Loader) sweepProducerFacts() {
	for i := range l.producerFacts {
		facts := l.producerFacts[i].Load()
		if facts == nil {
			continue
		}
		for object := range *facts {
			if l.producerFactCurrent(object) {
				continue
			}
			delete(*facts, object)
		}
	}
}

func (l *Loader) sweepSourceBindings() {
	for i := range l.bindings {
		bindings := l.bindings[i].Load()
		if bindings == nil {
			continue
		}
		for binding := bindings; binding != nil; binding = binding.next {
			evidence := binding.current.Load()
			if !evidence.bound {
				continue
			}
			input, _ := l.Store.GetArtifact(binding.source, store.ArtifactContent|store.ArtifactRevision).(*store.SourceArtifact)
			if evidence.matchesInputs(input, l.Store.GetObject(binding.source)) {
				continue
			}
			next := *evidence
			next.bound, next.paired, next.filesKnown = false, false, false
			next.input, next.declaration, next.files = nil, nil, nil
			next.delta = nil
			binding.current.Store(&next)
		}
	}
}

// Close MUST follow every loader and catalog consumer and preserve detached result data.
func (l *Loader) Close() {
	if l == nil {
		return
	}
	files := l.documents.cache
	l.retireBuiltBatches()
	l.Retire()
	for root := range files {
		doc := files[root]
		if doc.unprepared {
			for i := range doc.objects {
				manifest.ReleaseDoc(doc.objects[i].Doc)
			}
		}
		if l.catalog != nil {
			files[root] = document{
				eligible: doc.eligible, rootKnown: doc.rootKnown, rootAdmitted: doc.rootAdmitted,
				rootOwners: doc.rootOwners, rootPeers: doc.rootPeers, rootNative: doc.rootNative,
				rootApplications: doc.rootApplications,
				terminal:         doc.terminal, loaded: doc.loaded, bootstrap: doc.bootstrap, initial: doc.initial,
			}
		}
	}
	l.documents = documentReader{}
	l.InputContent = nil
	l.Options.ParsedInputs, l.Options.Changes, l.Options.Projections = nil, nil, nil
	l.Options.ConfiguredInput = nil
	l.Options.Cache, l.inputCaches = nil, nil
	l.facts, l.scopes, l.admitted, l.snapshot = nil, nil, nil, nil
	l.inputReaders, l.inputObjects, l.templateProjections, l.transformingProducers = nil, nil, nil, nil
	for i := range l.bindings {
		l.bindings[i].Store(nil)
	}
	for i := range l.producerFacts {
		l.producerFacts[i].Store(nil)
	}
	l.constructionPurpose.Store(nil)
	scopes := l.entryScopes.Swap(nil)
	if l.catalog == nil {
		retireEntryScopes(scopes)
	}
	l.projections.Clear()
	l.catalog, l.scopeRoots = nil, nil
	l.SourceProvider = nil
	l.locations, l.candidates, l.roots = nil, nil, rootInventory{}
	l.builtRoots = nil
}

func (l *Loader) publishFrozenRecords() {
	l.indexRecordInputs()
	l.inputObjects = map[manifest.NamedResource][]manifest.NamedResource{}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, occurrence := range l.locations {
		if !occurrence.facts.labeled {
			continue
		}
		key := id
		key.Name = ""
		l.inputObjects[key] = append(l.inputObjects[key], id)
	}
	for id := range l.candidates {
		child := l.candidates[id]
		if _, located := l.locations[id]; located {
			continue
		}
		if !manifest.KindOf(child.Obj).TypedLabels {
			continue
		}
		key := id
		key.Name = ""
		l.inputObjects[key] = append(l.inputObjects[key], id)
	}
	l.frozen = true
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopeRoots = l.scopes.rootFacts
	l.scopes, l.facts, l.Options.ParsedInputs, l.documents.parsed, l.snapshot = nil, nil, nil, nil, nil
	l.Options.Cache, l.inputCaches = nil, nil
	l.documents.recordInputs = false
	// Authored payloads remain available; snapshot and projection scratch end at freeze.
	//nolint:gocritic // Map values must be replaced after releasing snapshot scratch.
	for file, doc := range l.documents.cache {
		doc.parsed, doc.projection = nil, nil
		l.documents.cache[file] = doc
	}
}

func (l *Loader) prepareIndexScopes(ctx context.Context, repoRoot string, freeze bool) error {
	if l.scopes == nil || !l.scopes.complete || l.documents.compact && freeze {
		l.ensureScopes()
		if repoRoot != "" {
			for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
				state := l.sourceState(ks.EffectiveSourceID(), repoRoot)
				scope := l.scopeSource(new(state))
				l.scopes.sources[scope].ready = true
			}
		}
		return l.admit(ctx, repoRoot, nil)
	}
	return ctx.Err()
}

func (l *Loader) resetDerivedProductions() {
	if l.documents.compact && l.indexDerived {
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for id, record := range l.Index.Records {
			if record.Path == "" && record.Builder == "" && record.Origin.Policy(id, nil).Root {
				delete(l.Index.Records, id)
				delete(l.locations, id)
				continue
			}
			record.Productions = slices.DeleteFunc(record.Productions, func(p manifest.Production) bool {
				return record.Builder == "" || p.Owner != record.BuilderOwner ||
					p.Declaration != (manifest.NamedResource{}) ||
					p.Path != ""
			})
			l.Index.Records[id] = record
		}
	}
}

func (l *Loader) refreshIndexParents(repoRoot string) {
	var claims []manifest.KSClaim
	if l.scopes != nil {
		claims = slices.DeleteFunc(l.scopes.claims, func(c manifest.KSClaim) bool {
			r, _ := l.Index.Lookup(c.ID)
			return l.documents.cache[r.Path].eligible == selection.RootInactive
		})
		l.scopes.claims = claims
	}
	l.indexClaimOwners()
	parents := l.contextualParents()
	//nolint:gocritic // The iterator supplies a value snapshot that this loop updates before publication.
	for id, r := range l.Index.Files() {
		kind := id.KindInfo()
		if origin := new(l.documents).localPath(r.Path); origin != r.Path {
			root := new(l.documents).mountedRoot(r.Path)
			file, err := filepath.Rel(root, r.Path)
			r.File = ""
			if err == nil {
				r.File, _ = l.documents.artifacts[root].IndexPath(repoRoot, filepath.ToSlash(file))
			}
		}
		switch owner := parents[id]; {
		case selection.Root(l.roots.get(id).view()).Independent:
			r.Parent = manifest.NamedResource{}
		case kind.Parent && owner != (manifest.NamedResource{}):
			r.Parent = owner
		case kind.Parent && (r.Path != "" || r.Parent == (manifest.NamedResource{})):
			for owners := range l.Index.ClaimOwners(r.File, manifest.NamedResource{}, id, nil) {
				r.Parent = owners[0]
				break
			}
		}
		if kind.Structural {
			if object := l.CatalogObject(id); object != nil {
				source, _, _ := object.SourcePath()
				r.Pinned = SourceView(l.Store, source, repoRoot).Pinned
			}
		}
		l.Index.Records[id] = r
	}
}

func (l *Loader) completeIndexInputs(repoRoot string) error {
	l.markCompleteSources()
	if err := l.indexDeclarations(); err != nil {
		return err
	}
	if l.documents.compact {
		l.Index.Visibility = l.inputVisibility()
	}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		incomplete := doc.decodeErr != nil ||
			slices.ContainsFunc(doc.objects, func(object parsedObject) bool { return object.outcome.Err != nil })
		if incomplete {
			root := new(l.documents).mountedRoot(file)
			for source := range l.Index.CompleteSources {
				input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
				if input != nil && input.LocalPath == root {
					delete(l.Index.CompleteSources, source)
				}
			}
		}
		doc.terminal = l.terminalInput(file)
		l.documents.cache[file] = doc
	}
	for _, ids := range l.Index.Claims {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	l.Index.Components = new(l.documents).claimComponentIndex()
	l.qualifyOrderingOccurrences()
	l.indexTransformingProducers()
	return l.captureNativeRoots(repoRoot)
}

func (l *Loader) indexDeclarations() error {
	observed := len(l.Index.Declarations) != 0
	if !observed {
		for file := range l.documents.cache {
			if doc := l.documents.cache[file]; doc.declarations != nil || doc.declarationsData != nil {
				observed = true
				break
			}
		}
	}
	if !observed {
		return nil
	}
	bootstrap, _ := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	declarations, err := l.indexDeclarationSource(manifest.BootstrapSourceID, bootstrap, nil)
	if err != nil {
		return err
	}
	if l.scopes != nil {
		for i := range l.scopes.sources {
			scope := &l.scopes.sources[i]
			declarations, err = l.indexDeclarationSource(scope.id, scope.artifact, declarations)
			if err != nil {
				return err
			}
		}
	}
	for source := range l.Options.Changes.Sources() {
		input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		declarations, err = l.indexDeclarationSource(source, input, declarations)
		if err != nil {
			return err
		}
	}
	index := *l.Index
	index.Declarations = declarations
	l.Index = &index
	return nil
}

func (l *Loader) indexDeclarationSource(
	source manifest.NamedResource,
	input *store.SourceArtifact,
	declarations map[manifest.NamedResource]map[string]manifest.FileDeclarations,
) (map[manifest.NamedResource]map[string]manifest.FileDeclarations, error) {
	previous, known := declarations[source]
	if !known {
		previous = l.Index.Declarations[source]
	}
	files, err := l.documents.sourceDeclarations(input, previous)
	if err != nil {
		return nil, err
	}
	if files == nil {
		return declarations, nil
	}
	if declarations == nil {
		declarations = map[manifest.NamedResource]map[string]manifest.FileDeclarations{}
	}
	declarations[source] = files
	return declarations, nil
}

func (l *Loader) indexClaimOwners() {
	l.Index.Readers = map[string][]manifest.FileReader{}
	for prefix, ids := range l.Index.Claims {
		ids = slices.DeleteFunc(ids, func(id manifest.NamedResource) bool {
			r, _ := l.Index.Lookup(id)
			return l.documents.cache[r.Path].eligible == selection.RootInactive
		})
		if len(ids) == 0 {
			delete(l.Index.Claims, prefix)
			continue
		}
		l.Index.Claims[prefix] = ids
	}
}

func (l *Loader) markCompleteSources() {
	if l.scopes == nil {
		return
	}
	clear(l.Index.CompleteSources)
	markComplete := func(source manifest.NamedResource) {
		if l.Index.CompleteSources == nil {
			l.Index.CompleteSources = map[manifest.NamedResource]bool{}
		}
		l.Index.CompleteSources[source] = true
	}
	if !l.scopes.complete {
		return
	}
	if l.documents.bootstrap != "" {
		markComplete(manifest.BootstrapSourceID)
	}
	for i := range l.scopes.sources {
		source := &l.scopes.sources[i]
		if source.ready && !source.external && source.artifact != nil {
			markComplete(source.id)
		}
	}
}

func indexesReferenceReader(ref *manifest.Reference) bool {
	readinessInput := ref.Ordering && !ref.Optional
	return (ref.ContentInput() || readinessInput) && (ref.Name != "" || ref.Selector != nil)
}

func (l *Loader) indexRecordInputs() {
	for id := range l.Index.Records {
		for ref := range l.References(id) {
			if !indexesReferenceReader(&ref) {
				continue
			}
			if !slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
}

func (l *Loader) producerFactCurrent(object manifest.BaseManifest) bool {
	id := object.Named()
	if l.Store.GetDeclaration(id) == object {
		return true
	}
	record, indexed := l.Index.Lookup(id)
	if indexed && record.Declaration == object {
		prepared, _ := l.PreparedObject(id, nil)
		if record.Builder == "" || prepared.Obj == object {
			return true
		}
	}
	if candidate := l.candidates[id]; candidate.Obj == object && record.Builder == "" {
		return true
	}
	return false
}
