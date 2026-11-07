package loader

import (
	"context"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// FinalizeIndex freezes source-qualified graph contributions before reconciliation.
func (l *Loader) FinalizeIndex(ctx context.Context, repoRoot string, freeze bool) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	l.documents.compact = l.Options.Session != nil
	if l.documents.compact && l.frozen {
		return nil
	}
	l.mountSources()
	l.prepareIndexScopes(repoRoot, freeze)
	if l.documents.compact && l.indexDerived && l.derived == l.construction {
		if freeze {
			l.freezeConstruction()
		}
		return nil
	}
	l.resetDerivedProductions()
	l.refreshIndexParents(repoRoot)
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
	l.completeIndexInputs(repoRoot)
	if err := l.captureNativeRoots(repoRoot); err != nil {
		return err
	}

	if l.documents.compact {
		l.derived, l.indexDerived = l.construction, true
		if !freeze {
			return nil
		}
		l.freezeConstruction()
		return nil
	}
	if freeze {
		l.seedSourceAuthority()
	}
	l.Store.SetDiscoveryIndex(l.Index)
	if freeze {
		l.frozen = true
	}
	l.scopeRoots = l.scopes.rootFacts
	l.scopes = nil
	return nil
}

func (l *Loader) freezeConstruction() {
	l.seedSourceAuthority()
	l.inputReaders = map[manifest.NamedResource][]manifest.NamedResource{}
	for _, object := range l.Store.Objects() {
		id := object.Named()
		if _, indexed := l.Index.Lookup(id); indexed {
			continue
		}
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if ref.ContentInput() && (ref.Name != "" || ref.Selector != nil) &&
				!slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
	l.publishFrozenRecords()
}

func (l *Loader) seedSourceAuthority() {
	if l.frozen {
		return
	}
	l.side, l.sideKnown = l.comparisonSide()
	seed := func(source manifest.NamedResource) {
		l.SourceFacts(source, nil)
		previous := l.bindings.Load()
		if previous == nil {
			return
		}
		evidence := (*previous)[source]
		if !evidence.initialKnown {
			evidence.initial, _ = l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			evidence.initialKnown = true
			l.publishSource(source, &evidence)
		}
	}
	seed(manifest.BootstrapSourceID)
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
	for root := range l.documents.cache {
		doc := l.documents.cache[root]
		if doc.builtInput == nil {
			continue
		}
		if l.Options.Session == nil {
			l.Store.Retire(doc.builtInput.Support)
			doc.built, doc.builtInput = nil, nil
			l.documents.cache[root] = doc
		}
	}
	l.InputContent = nil
	if l.Options.Session == nil {
		l.inputReaders, l.inputObjects = nil, nil
		l.templateProjections = nil
		for i := range l.producerFacts {
			l.producerFacts[i].Store(nil)
		}
		l.documents = documentReader{}
		l.facts, l.scopes, l.Options.ParsedInputs = nil, nil, nil
		l.candidates, l.admitted, l.locations = nil, nil, nil
		l.roots = rootInventory{}
		l.bindings.Store(nil)
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
	for root := range l.documents.cache {
		doc := l.documents.cache[root]
		if doc.builtInput != nil && !doc.builtInput.Support.Valid() {
			l.Store.Retire(doc.builtInput.Support)
			doc.built, doc.builtInput = nil, nil
			l.documents.cache[root] = doc
		}
	}
}

func (l *Loader) sweepProducerFacts() {
	for i := range l.producerFacts {
		facts := l.producerFacts[i].Load()
		if facts == nil {
			continue
		}
		for object := range *facts {
			id := object.Named()
			if l.Store.GetDeclaration(id) == object {
				continue
			}
			record, indexed := l.Index.Lookup(id)
			if indexed && record.Declaration == object && (record.Builder == "" || l.preparedObject(id).Obj == object) {
				continue
			}
			if candidate := l.candidates[id]; candidate.Obj == object && record.Builder == "" {
				continue
			}
			delete(*facts, object)
		}
	}
}

func (l *Loader) sweepSourceBindings() {
	if bindings := l.bindings.Load(); bindings != nil {
		for id := range *bindings {
			evidence := (*bindings)[id]
			input, _ := l.Store.GetArtifact(id, store.ArtifactContent|store.ArtifactRevision).(*store.SourceArtifact)
			if evidence.matchesInputs(input, l.Store.GetObject(id)) {
				continue
			}
			evidence.bound, evidence.paired, evidence.pathsKnown = false, false, false
			evidence.input, evidence.declaration, evidence.paths = nil, nil, nil
			evidence.delta = nil
			(*bindings)[id] = evidence
		}
	}
}

// Close MUST follow every loader and catalog consumer and preserve detached result data.
func (l *Loader) Close() {
	if l == nil {
		return
	}
	files := l.documents.cache
	l.Retire()
	for root := range files {
		doc := files[root]
		if doc.builtInput != nil {
			l.Store.Retire(doc.builtInput.Support)
		}
		if l.catalog != nil {
			files[root] = document{
				eligible: doc.eligible, rootKnown: doc.rootKnown, rootAdmitted: doc.rootAdmitted,
				rootOwners: doc.rootOwners, rootPeers: doc.rootPeers, rootNative: doc.rootNative,
				terminal: doc.terminal, loaded: doc.loaded, bootstrap: doc.bootstrap, initial: doc.initial,
			}
		}
	}
	l.documents = documentReader{}
	l.InputContent = nil
	l.Options.ParsedInputs, l.Options.Changes, l.Options.Projections = nil, nil, nil
	l.Options.ConfiguredInput = nil
	l.facts, l.scopes, l.admitted = nil, nil, nil
	l.inputReaders, l.inputObjects, l.templateProjections = nil, nil, nil
	l.bindings.Store(nil)
	for i := range l.producerFacts {
		l.producerFacts[i].Store(nil)
	}
	l.constructionPurpose.Store(nil)
	l.projections.Clear()
	l.catalog, l.scopeRoots = nil, nil
	l.locations, l.candidates, l.roots = nil, nil, rootInventory{}
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
		if _, labeled := child.Obj.(interface{ GetLabels() map[string]string }); !labeled {
			continue
		}
		key := id
		key.Name = ""
		l.inputObjects[key] = append(l.inputObjects[key], id)
	}
	l.frozen = true
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopeRoots = l.scopes.rootFacts
	l.scopes, l.facts, l.Options.ParsedInputs, l.documents.parsed = nil, nil, nil, nil
}

func (l *Loader) prepareIndexScopes(repoRoot string, freeze bool) {
	if l.scopes == nil || !l.scopes.complete || l.documents.compact && freeze {
		l.ensureScopes()
		if repoRoot != "" {
			for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
				state := l.sourceState(ks.SourceID(), repoRoot)
				scope := l.scopeSource(new(state))
				l.scopes.sources[scope].ready = true
			}
		}
		l.admit(repoRoot)
	}
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
	own := l.indexClaimOwners()
	//nolint:gocritic // The iterator supplies a value snapshot that this loop updates before publication.
	for id, r := range l.Index.Files() {
		kind := id.KindInfo()
		if origin := new(l.documents).localPath(r.Path); origin != r.Path {
			rel, _ := filepath.Rel(repoRoot, origin)
			r.File = filepath.ToSlash(rel)
		}
		if selection.Root(l.roots.get(id).facts).Independent {
			r.Parent = manifest.NamedResource{}
		} else if kind.Parent && (r.Path != "" || r.Parent == (manifest.NamedResource{})) {
			if parent, ok := manifest.StrictParent(claims, r.File, id, own[id]); ok {
				r.Parent = parent
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

func (l *Loader) completeIndexInputs(repoRoot string) {
	l.markCompleteSources()
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
				if source == manifest.BootstrapSourceID && root == repoRoot ||
					input != nil && input.LocalPath == root {
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
}

func (l *Loader) indexClaimOwners() map[manifest.NamedResource]map[string]struct{} {
	l.Index.Readers = map[string][]manifest.FileReader{}
	own := map[manifest.NamedResource]map[string]struct{}{}
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
		for _, id := range ids {
			if _, indexed := l.Index.Lookup(id); !indexed {
				continue
			}
			if own[id] == nil {
				own[id] = map[string]struct{}{}
			}
			own[id][prefix] = struct{}{}
		}
	}
	return own
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

func (l *Loader) indexRecordInputs() {
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, record := range l.Index.Records {
		refs := l.recordInputReferences(id, &record)
		if refs == nil {
			continue
		}
		for ref := range refs {
			if !ref.ContentInput() || ref.Name == "" && ref.Selector == nil {
				continue
			}
			if !slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
}

func (l *Loader) recordInputReferences(
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) iter.Seq[manifest.Reference] {
	var refs iter.Seq[manifest.Reference]
	if object := l.Store.GetDeclaration(id); object != nil {
		refs = manifest.References(object, manifest.ReferenceRelations)
	} else if child := l.candidates[id]; child.Obj != nil && record.Path == "" {
		refs = manifest.References(child.Obj, manifest.ReferenceRelations)
	} else if occurrence, ok := l.locations[id]; ok {
		refs = occurrence.references()
	}
	return refs
}
