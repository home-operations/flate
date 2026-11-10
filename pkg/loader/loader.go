// Package loader hydrates a Store from Flux source filesystems.
package loader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/home-operations/flate/internal/claimpath"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

type options struct {
	Cache *cache.Cache
	Work  manifest.WorkScope
	// Projections shares source-free producer facts during paired planning.
	Projections *manifest.ProducerProjections
	// Changes supplies frozen source-qualified comparison facts.
	Changes *change.Set
	// ConfiguredInput preserves the caller's bootstrap view; nil is meaningful before admission.
	ConfiguredInput *store.SourceArtifact
	// Session supplies the committed execution decision.
	Session *selection.Session
	// Missing borrows the immutable missing-input policy for this invocation.
	Missing      *depwait.MissingPolicy
	RemoteInputs *source.HTTPInputs
	RemoteRetry  cache.RetryConfig
	// ParsedInputs reuses immutable content only from matching admitted source views.
	ParsedInputs *ParsedInputs
}

// Loader collects source-qualified discovery evidence before admitting roots.
// Source membership MUST remain stable across sequential Load calls. The first
// Load classifies the entry for all subsequent loads; independent entry policies
// require separate loaders.
type Loader struct {
	constructionPurpose atomic.Pointer[[]manifest.NamedResource]
	work                *manifest.WorkRecord
	Store               *store.Store
	Options             options
	// SourceRoot anchors slash-separated discovery coordinates and bounds
	// standalone input admission. Source artifacts supply their own FS.
	SourceRoot string

	// Index records static discovery facts and is initialized by New. Caller
	// replacements MUST initialize Records before Load. Mutable until FinalizeIndex returns.
	Index *manifest.DiscoveryIndex
	// InputContent contains transient evidence paired with the current derived index.
	InputContent          map[manifest.NamedResource]inputprojection.Projection
	documents             documentReader
	facts                 *ParsedInputs
	snapshot              *ParsedInputs
	inputCaches           map[string]*inputCache
	scopes                *scopeConstruction
	entry                 entryKind
	nativeEntry           *manifest.Kustomization
	entryPath             string
	candidates            map[manifest.NamedResource]store.Child
	builtRoots            map[manifest.NamedResource]builtRoot
	admitted              map[manifest.NamedResource]manifest.BaseManifest
	roots                 rootInventory
	scopeRoots            []selection.NativeRootFacts
	catalog               *Catalog
	locations             map[manifest.NamedResource]authoredOccurrence
	bindings              [64]atomic.Pointer[map[manifest.NamedResource]*atomic.Pointer[admissionSource]]
	inputReaders          map[manifest.NamedResource][]manifest.NamedResource
	inputObjects          map[manifest.NamedResource][]manifest.NamedResource
	templateProjections   map[manifest.NamedResource]manifest.TemplateProjection
	projections           manifest.ProducerProjections
	producerFacts         [64]atomic.Pointer[map[manifest.BaseManifest]producerFact]
	construction, derived uint64
	indexDerived          bool
	frozen                bool
	side                  manifest.TreeSide
	sideKnown             bool

	// IgnoreFile, when non-empty, is the gitignore-grammar file Load
	// reads in place of <root>/.krmignore, with its patterns anchored
	// at the Load root. Discovery sets it for the --path scan only:
	// spec.path targets followed afterwards keep their own
	// per-directory .krmignore, exactly as if the file sat at the
	// scan root.
	IgnoreFile string

	// PreferExisting suppresses overwrites of resources already in
	// the store and their discovery records. Used by the
	// orchestrator's recursive spec.path discovery so the initial
	// --path scan's data wins over downstream paths that may point
	// into a different tree.
	PreferExisting bool
}

type entryKind uint8

const (
	entryUnset entryKind = iota
	entryPlain
	entryKustomize
)

// New initializes discovery state.
func New(s *store.Store) *Loader {
	return &Loader{
		work:  s.WorkScope().Traversal(manifest.BootstrapSourceID),
		Store: s, Options: options{Work: s.WorkScope(), Missing: depwait.NewMissingPolicy(nil)},
		Index:     &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		documents: documentReader{cache: map[string]document{}, views: map[string]fs.FS{}},
	}
}

// AdmitDeclarations retains pathless reference evidence before repository discovery.
func (l *Loader) AdmitDeclarations(children []store.Declaration) {
	for _, child := range children {
		l.Store.AddDeclaration(child)
		id := child.Obj.Named()
		r := l.Index.Records[id]
		if r.Path == "" && r.Builder == "" {
			r.Origin, r.Declaration = child.Origin, child.Obj
			r.Refs = discoveryReferences(nil, child.Obj)
			l.Index.Records[id] = r
		}
	}
}

// Sources yields eligible immutable source-artifact evidence in unspecified order.
func (l *Loader) Sources(ctx context.Context) iter.Seq2[store.Child, error] {
	return func(yield func(store.Child, error) bool) {
		//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
		for id, r := range l.Index.Files() {
			if info := id.KindInfo(); info.Group != manifest.SourceDomain || !info.Discovery {
				continue
			}
			if l.documents.cache[r.Path].eligible == selection.RootInactive {
				continue
			}
			if !l.yieldSource(ctx, id, new(r), yield) {
				return
			}
		}
	}
}

// Kustomizations returns eligible source-qualified owner evidence.
func (l *Loader) Kustomizations() []store.Child {
	return l.eligibleKustomizations(l.kustomizations())
}

func (l *Loader) eligibleKustomizations(kss []store.Child) []store.Child {
	if len(l.Index.Records) == 0 {
		return kss
	}
	return slices.DeleteFunc(kss, func(child store.Child) bool {
		return l.inactiveKustomization(child.Obj.Named())
	})
}

func (l *Loader) inactiveKustomization(id manifest.NamedResource) bool {
	r, indexed := l.Index.Lookup(id)
	return indexed && l.documents.cache[r.Path].eligible == selection.RootInactive
}

func (l *Loader) kustomizations() []store.Child {
	objects := l.Store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
	count := len(objects)
	for id := range l.Index.Files() {
		if id.Group == manifest.FluxKustomizeDomain && id.Kind == manifest.KindKustomization &&
			l.Store.GetObject(id) == nil {
			count++
		}
	}
	out := make([]store.Child, 0, count)
	l.eachKustomization(objects, func(child store.Child) { out = append(out, child) })
	slices.SortFunc(out, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	return out
}

func (l *Loader) eachKustomization(objects []manifest.BaseManifest, yield func(store.Child)) {
	for _, obj := range objects {
		if _, _, applies := obj.SourcePath(); !applies || !l.Store.Policy(obj.Named()).Root {
			continue
		}
		id := obj.Named()
		// Compact census MUST derive synthetic ownership from accepted output.
		if l.documents.compact && id.ReportHidden() {
			continue
		}
		_, doc, emission := l.Store.GetEmission(id)
		emission.Finish()
		yield(store.Child{Obj: obj, Doc: doc})
	}
	l.eachIndexedKustomization(yield)
}

func (l *Loader) eachIndexedKustomization(yield func(store.Child)) {
	//nolint:gocritic // The iterator supplies filtered value snapshots through its callback.
	for id, r := range l.Index.Files() {
		if id.Group != manifest.FluxKustomizeDomain || id.Kind != manifest.KindKustomization {
			continue
		}
		if obj, policy := l.Store.Lookup(id); obj != nil && policy.Root {
			continue
		}
		if !l.declarationLocated(id, l.Store) {
			continue
		}
		child := l.IndexedObject(id, r)
		if r.Builder != "" && r.Declaration != nil {
			child.Obj = r.Declaration
		}
		if _, ok := child.Obj.(*manifest.Kustomization); ok {
			yield(child)
		}
	}
}

// IndexedObject returns the retained declaration under its contextual identity.
//
//nolint:gocritic // The SDK contract accepts a discovery record value snapshot.
func (l *Loader) IndexedObject(id manifest.NamedResource, r manifest.DiscoveryRecord) store.Child {
	child := l.candidates[id]
	if r.Origin == manifest.SuppliedDeclaration {
		return store.Child{Obj: l.Store.GetDeclaration(id), Origin: r.Origin}
	}
	if child.Obj == nil {
		child = l.authoredObject(r.Path, r.Authored)
	}
	if l.documents.compact {
		if ks, ok := child.Obj.(*manifest.Kustomization); ok && ks.Named() != id {
			projected := *ks
			projected.Name, projected.Namespace = id.Name, id.Namespace
			child.Obj = &projected
			if l.candidates == nil {
				l.candidates = map[manifest.NamedResource]store.Child{}
			}
			l.candidates[id] = child
		}
	} else if child.Obj != nil && child.Obj.Named().Namespace != id.Namespace {
		child.Obj = child.Obj.WithNamespace(id.Namespace)
	}
	return child
}

// References yields immutable authored reference evidence without hydrating its declaration.
func (l *Loader) References(id manifest.NamedResource) iter.Seq[manifest.Reference] {
	if occurrence, ok := l.catalogOccurrence(id); ok {
		return occurrence.references()
	}
	record, _ := l.Index.Lookup(id)
	return manifest.References(l.IndexedObject(id, record).Obj, manifest.ReferenceRelations)
}

// Load records Flux objects under root by walking the kustomize
// resource graph. When root has a kustomization.yaml it's treated as
// a kustomize package and only files reachable through `resources:`
// are loaded; when it has none, a recursive walk finds and enters
// kustomize packages it encounters, loading every YAML otherwise.
//
// Honors ctx cancellation; visited-set protects against cycles.
func (l *Loader) Load(
	ctx context.Context,
	root string,
	owner manifest.NamedResource,
	work *manifest.WorkRecord,
) (_ int, resultErr error) {
	if l.Store == nil {
		return 0, errors.New("loader: Store is nil")
	}
	defer func() {
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(resultErr)
	}()
	l.documents.compact = l.Options.Session != nil
	l.documents.recordInputs = l.documents.compact && l.Options.Cache != nil && l.Options.Cache.CanPersistRenders()
	l.mountSources()
	abs, err := filepath.Abs(root)
	if err != nil {
		return 0, fmt.Errorf("resolve root: %w", err)
	}
	inputRoot, err := filepath.Abs(cmp.Or(l.SourceRoot, abs))
	if err != nil {
		return 0, fmt.Errorf("resolve input root: %w", err)
	}
	if new(l.documents).mountedRoot(abs) == "" &&
		(new(l.documents).mountedRoot(inputRoot) == "") {
		skip, mountErr := l.mountDirectory(ctx, inputRoot)
		if skip || mountErr != nil {
			return 0, mountErr
		}
	}
	l.documents.bootstrap = cmp.Or(l.documents.bootstrap, inputRoot)
	if err := l.restoreCachedInputs(ctx); err != nil {
		return 0, err
	}
	l.ensureScopes()
	reader, view := l.documents, l.physicalView()
	evidence := view.rootFacts(owner, l, nil)
	ordinary := !l.frozen && selection.Root(evidence).Independent &&
		evidence.Occurrence.Declaration != (selection.OccurrenceID{})
	if purpose := l.constructionPurpose.Load(); purpose != nil {
		ordinary = ordinary || slices.Contains(*purpose, owner)
	}
	switch {
	case !reader.compact,
		owner == (manifest.NamedResource{}),
		l.Options.Session.Snapshot().OrdinaryDemand(owner),
		ordinary:
	default:
		reader.intent = manifest.ApplicationDocuments
	}
	builder := l.documents.read(abs).file
	l.setEntry(abs, builder)
	if coverage, scanned := l.scopes.scanned[abs]; scanned && covers(coverage, reader.intent) {
		work = nil
		return 0, nil
	}
	var files []fileAdmission
	w := traversal{
		work:      work,
		files:     &files,
		documents: reader, policy: traversalAdmission, loader: l,
		directories: map[string]struct{}{},
		build:       builder != "", ignoreCache: map[ignoreKey]bool{},
	}
	loaded := l.loadRoot(ctx, abs, &w)
	work = w.work
	if !loaded {
		return w.count, w.err
	}
	if !w.proceed(ctx) {
		return w.count, w.err
	}
	if reader.intent != manifest.ApplicationDocuments {
		l.markInitialFiles(abs, files)
	}
	l.scopes.scanned[abs] = reader.intent
	if l.documents.compact {
		l.construction++
	}
	return w.count, w.err
}

func (l *Loader) loadRoot(ctx context.Context, root string, w *traversal) bool {
	if w.work != nil && w.work.Key.Source != (manifest.NamedResource{}) {
		if l.scopes.traversalWork == nil {
			l.scopes.traversalWork = map[string]*manifest.WorkRecord{}
		}
		l.scopes.traversalWork[new(l.documents).mountedRoot(root)] = l.Options.Work.Traversal(w.work.Key.Source)
	}
	factsSource := l.sourceFacts(root)
	key := loadContext{path: root, ignore: l.IgnoreFile}
	if facts, _ := factsSource.load(key); facts.err != nil && ctx.Err() == nil {
		w.work, w.err = nil, facts.err
		return false
	}
	defer func() {
		if w.err != nil && ctx.Err() == nil {
			facts, _ := factsSource.load(key)
			facts.err = w.err
			factsSource.loads.values[key] = facts
		}
	}()
	facts, _ := factsSource.load(key)
	l.scopes.complete = l.scopes.complete && !l.loadChangesScopes(root, &facts, w.documents.intent)
	ignore, skip, err := l.loadRootIgnore(ctx, root)
	if skip || err != nil {
		w.err = err
		return false
	}
	w.ignore = ignore
	l.loadCached(ctx, root, w)
	return true
}

// Authored input replaces synthetic aliases and their ready artifacts.
func (l *Loader) acceptObject(id manifest.NamedResource) bool {
	if !l.PreferExisting {
		return true
	}
	if l.Index.Records[id].Path != "" {
		return false
	}
	if obj, policy := l.Store.Lookup(id); obj == nil || !policy.Root {
		return true
	}
	artifact, _ := l.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if artifact == nil || !artifact.InputAlias {
		return false
	}
	l.Store.DeleteObject(id)
	return true
}

func (l *Loader) loadFile(ctx context.Context, path string, reader *documentReader) (int, error) {
	if l.documents.parsed != nil {
		l.documents.restore(path)
	}
	if cached := l.documents.cache[path]; (cached.loaded || cached.selected) && covers(cached.coverage, reader.intent) {
		return 0, nil
	}
	previous := reader.cache[path]
	reader.regular = true
	var objs []parsedObject
	var err error
	if reader.compact {
		objs, err = reader.censusFile(ctx, path, manifest.ParseDocOptions{})
	} else {
		objs, err = reader.fileObjects(path, manifest.ParseDocOptions{})
	}
	if err != nil {
		return 0, err
	}
	if previous.objects != nil && previous.coverage == manifest.ApplicationDocuments &&
		reader.intent != manifest.ApplicationDocuments {
		for id := range l.Index.Records {
			record := l.Index.Records[id]
			if record.Path == path && record.Builder == "" {
				delete(l.Index.Records, id)
				delete(l.locations, id)
			}
		}
	}
	count := l.recordLoadedObjects(path, objs)
	cached := l.documents.cache[path]
	cached.loaded = true
	l.documents.cache[path] = cached
	return count, nil
}

// recordDataFile keeps Component data available for lazy promotion without
// publishing untemplated Component resources into the Store.
func (l *Loader) recordDataFile(ctx context.Context, absPath string, reader *documentReader) error {
	if cached := l.documents.cache[absPath]; cached.selected && covers(cached.coverage, reader.intent) {
		return nil
	}
	if l.documents.compact {
		return l.recordCompactDataFile(ctx, absPath, reader)
	}
	objs, err := reader.fileObjects(
		absPath,
		manifest.ParseDocOptions{DefaultNamespace: ""},
	)
	if err != nil {
		return err
	}
	{
		childValues3 := objs
		for childIndex3 := range childValues3 {
			child := childValues3[childIndex3]
			obj := child.Obj
			if obj == nil || child.outcome.Err != nil {
				continue
			}
			id := obj.Named()
			if !manifest.KindOf(obj).Data {
				continue
			}
			if !l.acceptObject(id) {
				continue
			}
			l.recordObject(obj, absPath)
		}
	}
	return nil
}

func (l *Loader) recordOccurrence(path string, o *authoredOccurrence) {
	if o.facts.uncertain || !o.facts.retained && len(o.facts.targets) == 0 && !o.facts.needsCandidates {
		return
	}
	if !l.acceptObject(o.Identity) {
		return
	}
	if l.locations == nil {
		l.locations = map[manifest.NamedResource]authoredOccurrence{}
	}
	l.locations[o.Identity] = *o
	r := manifest.DiscoveryRecord{
		Authored: o.Authored,
		Declared: o.facts.declared,
		Path:     path,
		File:     l.fileCoordinate(path),
	}
	{
		refValues := o.facts.refs
		for refIndex := range refValues {
			ref := refValues[refIndex]
			if discoveryReference(new(ref)) {
				r.Refs = append(r.Refs, ref.NamedResource)
			}
		}
	}
	l.Index.Records[o.Identity] = r
}

// recordObject retains raw locations and authored source references for deferred admission.
func (l *Loader) recordObject(obj manifest.BaseManifest, absPath string) {
	id := obj.Named()
	if l.candidates[id].Obj != nil {
		if record, exists := l.Index.Lookup(id); exists && record.Path == "" && record.Origin.Policy(id, nil).Root {
			return
		}
	}
	delete(l.candidates, id)
	r := manifest.DiscoveryRecord{Authored: id, Path: absPath, File: l.fileCoordinate(absPath)}
	var refs [16]manifest.NamedResource
	if captured := discoveryReferences(refs[:0], obj); len(captured) != 0 {
		r.Refs = slices.Clone(captured)
	}
	l.Index.Records[id] = r
}

func discoveryReferences(refs []manifest.NamedResource, obj manifest.BaseManifest) []manifest.NamedResource {
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if discoveryReference(new(ref)) {
			refs = append(refs, ref.NamedResource)
		}
	}
	return refs
}

func discoveryReference(refValue *manifest.Reference) bool {
	ref := *refValue

	return ref.Use == "valuesFrom" ||
		ref.Use == "substituteFrom" && ref.Group == "" && ref.Kind == manifest.KindConfigMap ||
		(ref.Use == sourceReferenceUse || ref.Use == "chart" || ref.Use == "artifact") && ref.Kind != "" &&
			ref.Name != ""
}

func (l *Loader) fileCoordinate(absPath string) string {
	if l.SourceRoot != "" {
		if rel, err := filepath.Rel(l.SourceRoot, absPath); err == nil {
			return filepath.ToSlash(rel)
		}
	}
	return filepath.ToSlash(absPath)
}

func isManifestFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

func (l *Loader) replayFile(ctx context.Context, fileValue *fileAdmission, reader *documentReader) (int, error) {
	file := *fileValue

	if file.kind == admissionWarning {
		l.Store.AddWarning(file.warning)
		return 0, nil
	}
	if file.kind == admissionBootstrap {
		l.documents.restore(file.file)
		{
			childValues4, err := reader.censusFile(ctx, file.file, manifest.ParseDocOptions{})
			if err != nil {
				return 0, err
			}
			for childIndex4 := range childValues4 {
				child := childValues4[childIndex4]
				if child.fact == nil || child.fact.Declaration != file.coordinate ||
					!l.acceptObject(child.fact.Identity) {
					continue
				}
				l.recordOccurrence(file.file, child.fact)
				doc := l.documents.cache[file.file]
				doc.bootstrap = true
				l.documents.cache[file.file] = doc
				return 1, nil
			}
		}
		return 0, nil
	}
	var n int
	var err error
	if file.kind == admissionData {
		err = l.recordDataFile(ctx, file.file, reader)
	} else {
		n, err = l.loadFile(ctx, file.file, reader)
	}
	if err != nil && (file.kind == admissionPackage || file.kind == admissionData) {
		message := "component data file failed to parse"
		if file.kind == admissionPackage {
			message = "kustomization file failed to parse"
		}
		l.Store.AddWarning(
			manifest.Warning{
				Category: manifest.WarnInputParseFailed,
				Message:  message,
				Detail:   []string{file.file, err.Error()},
			},
		)
		return n, nil
	}
	if err != nil {
		return n, fmt.Errorf("loader: %w", err)
	}
	return n, nil
}

const (
	sourceReferenceUse = "source"
)

func (l *Loader) recordLoadedObjects(path string, objs []parsedObject) int {
	count := 0
	{
		childValues := objs
		for childIndex := range childValues {
			child := childValues[childIndex]
			if child.fact != nil {
				l.recordOccurrence(path, child.fact)
				if !child.fact.facts.uncertain && child.fact.Identity.KindInfo().Discovery {
					count++
				}
				continue
			}
			obj := child.Obj
			if obj == nil || child.outcome.Err != nil {
				continue
			}
			info := manifest.KindOf(obj)
			if !info.Tracked {
				_, _, hasPath := obj.SourcePath()
				if hasPath || len(manifest.ProducerTargets(obj)) != 0 {
					l.recordObject(obj, path)
				}
				continue
			}
			id := obj.Named()
			if !l.acceptObject(id) {
				continue
			}
			l.recordObject(obj, path)
			if info.Discovery {
				count++
			}
		}
	}
	return count
}

func (l *Loader) recordCompactDataFile(ctx context.Context, absPath string, reader *documentReader) error {
	facts, err := reader.censusFile(ctx, absPath, manifest.ParseDocOptions{})
	if err != nil {
		return err
	}
	{
		childValues2 := facts
		for childIndex2 := range childValues2 {
			child := childValues2[childIndex2]
			if child.outcome.Err != nil {
				continue
			}
			if child.fact.Identity.KindInfo().Data {
				l.recordOccurrence(absPath, child.fact)
			}
		}
	}
	return nil
}

func (l *Loader) yieldSource(
	ctx context.Context,
	id manifest.NamedResource,
	rValue *manifest.DiscoveryRecord,
	yield func(store.Child, error) bool,
) bool {
	r := *rValue
	obj := l.IndexedObject(id, r)
	if r.Builder != "" {
		obj, _ = l.PreparedObject(id, nil)
	}
	if l.documents.compact {
		var err error
		if occurrence, ok := l.occurrence(id); ok && obj.Obj == nil {
			obj, err = l.OccurrenceObject(ctx, new(occurrenceView(&occurrence)), nil)
		}
		if err != nil {
			yield(store.Child{}, err)
			return false
		}
		if obj.Obj == nil {
			return true
		}
		if l.candidates == nil {
			l.candidates = map[manifest.NamedResource]store.Child{}
		}
		l.candidates[id] = obj
	}
	if obj.Obj != nil && !yield(obj, nil) {
		return false
	}
	return true
}

func (l *Loader) yieldVisitedOwners(file string, yield func(manifest.NamedResource) bool) bool {
	for v := range l.scopes.fileVisits(file) {
		if v.owner != (manifest.NamedResource{}) && v.role == kustomizeinput.Resource && !yield(v.owner) {
			return false
		}
	}

	return true
}

func (l *Loader) yieldFileClaims(coordinate string, yield func(manifest.NamedResource) bool) bool {
	for _, id := range l.Index.Claims[coordinate+"/"] {
		if !yield(id) {
			return false
		}
	}
	for end, group := range claimpath.Groups(l.Index.Claims, coordinate) {
		if end == 0 && (coordinate == ".." || strings.HasPrefix(coordinate, "../")) {
			continue
		}
		for _, id := range group {
			if !yield(id) {
				return false
			}
		}
	}
	return true
}

func (l *Loader) mountDirectory(ctx context.Context, inputRoot string) (bool, error) {
	location, locateErr := gitrepo.Locate(ctx, inputRoot, inputRoot)
	if locateErr != nil {
		return false, locateErr
	}
	input, locateErr := gittree.AdmitDirectory(ctx, location)
	if locateErr != nil {
		if l.IgnoreFile != "" && errors.Is(locateErr, fs.ErrNotExist) {
			return true, nil
		}
		return false, locateErr
	}
	//nolint:errcheck // AdmitDirectory supplies the native filesystem required by the operation view.
	l.documents.views[inputRoot] = input.FS.(*gittree.FS).OperationView()
	l.documents.edgesDirty = true
	return false, nil
}

func (l *Loader) loadRootIgnore(ctx context.Context, abs string) (*ignoreSet, bool, error) {
	var ignore *ignoreSet
	var err error
	if l.IgnoreFile != "" {
		inputFile := new(l.documents).input(abs)
		info, statErr := fs.Stat(inputFile.fs, inputFile.name)
		new(l.documents).rememberStat(abs, info, statErr)
		if errors.Is(statErr, fs.ErrNotExist) || statErr == nil && !info.IsDir() {
			return nil, true, nil
		}
		if statErr != nil {
			return nil, false, fmt.Errorf("stat root: %w", statErr)
		}
		ignore, err = new(l.documents).loadIgnoreFile(ctx, l.IgnoreFile, abs)
	} else {
		ignore, err = new(l.documents).loadIgnore(abs)
	}
	if err != nil {
		return nil, false, err
	}
	return ignore, false, nil
}

func (l *Loader) loadCached(ctx context.Context, abs string, w *traversal) {
	sourceState := l.sourceFacts(abs)
	key := loadContext{path: abs, ignore: l.IgnoreFile}
	facts, replay := sourceState.load(key)
	if replay && facts.complete {
		w.work = nil
		l.replayLoad(ctx, &facts, w)
		facts.complete = w.proceed(ctx)
	} else {
		facts = loadFacts{directories: map[string]loadDirectory{}}
		w.files = &facts.files
		w.walk(ctx, graphVisit{root: abs, file: abs}, nil)
		facts.complete = w.proceed(ctx)
		for file := range w.directories {
			doc := l.documents.cache[file]
			facts.directories[file] = loadDirectory{adHoc: doc.adHoc, traversed: doc.traversed}
		}
	}
	if facts.complete {
		if w.documents.intent != manifest.ApplicationDocuments {
			l.markInitialFiles(abs, facts.files)
		}
	}
	facts.coverage = w.documents.intent
	facts.observed = facts.observed || w.documents.intent != manifest.ApplicationDocuments
	sourceState.loads.values[key] = facts
}

func (l *Loader) replayLoad(ctx context.Context, facts *loadFacts, w *traversal) {
	for file, directory := range facts.directories {
		l.documents.restore(file)
		doc := l.documents.cache[file]
		doc.adHoc, doc.traversed = doc.adHoc || directory.adHoc, doc.traversed || directory.traversed
		l.documents.cache[file] = doc
	}
	{
		fileValues := facts.files
		for fileIndex := range fileValues {
			file := fileValues[fileIndex]
			if !w.proceed(ctx) {
				break
			}
			n, err := l.replayFile(ctx, new(file), &w.documents)
			w.count += n
			if err != nil {
				w.err = err
				break
			}
		}
	}
}

func (l *Loader) markInitialFiles(abs string, files []fileAdmission) {
	if abs != l.entryPath {
		return
	}
	for i := range files {
		file := &files[i]
		doc := l.documents.cache[file.file]
		doc.initial = true
		l.documents.cache[file.file] = doc
	}
}

func (l *Loader) setEntry(abs, builder string) {
	if l.entry == entryUnset {
		l.entryPath = abs
		l.entry = entryPlain
		if builder != "" {
			l.entry = entryKustomize
			relative, _ := filepath.Rel(l.documents.bootstrap, abs)
			l.nativeEntry = manifest.NewEntryKustomization(
				manifest.BootstrapSourceID,
				filepath.ToSlash(relative),
				manifest.NamedResource{},
			)
		}
	}
}

func (l *Loader) declarationLocated(id manifest.NamedResource, objects admissionReader) bool {
	record, _ := l.Index.Lookup(id)
	return record.Path != "" || record.Builder != "" || objects.GetDeclaration(id) != nil ||
		selection.Root(l.roots.get(record.Parent).view()).Independent
}
