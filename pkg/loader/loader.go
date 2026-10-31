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
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

type options struct {
	// Projections shares source-free producer facts during paired planning.
	Projections *manifest.ProducerProjections
	// HistoryOwners supplies immutable positive owner reach from accepted source paths.
	HistoryOwners []manifest.NamedResource
	// Selection controls compact retention for restricted requests.
	Selection manifest.Selection
	// Changes supplies frozen source-qualified comparison facts.
	Changes *change.Set
	// Filter owns committed execution membership.
	Filter *change.Filter
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets  bool
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
	Store   *store.Store
	Options options
	// SourceRoot anchors slash-separated discovery coordinates and bounds
	// standalone input admission. Source artifacts supply their own FS.
	SourceRoot string

	// Index records static discovery facts and is initialized by New. Caller
	// replacements MUST initialize Records before Load. Mutable until FinalizeIndex returns.
	Index                 *manifest.DiscoveryIndex
	documents             documentReader
	facts                 *ParsedInputs
	scopes                *scopeConstruction
	entry                 entryKind
	entryPath             string
	candidates            map[manifest.NamedResource]store.Child
	admitted              map[manifest.NamedResource]manifest.BaseManifest
	roots                 map[manifest.NamedResource]bool
	locations             map[manifest.NamedResource]Occurrence
	bindings              map[manifest.NamedResource]*store.SourceArtifact
	inputReaders          map[manifest.NamedResource][]manifest.NamedResource
	inputObjects          map[manifest.NamedResource][]manifest.NamedResource
	templateTargets       map[manifest.NamedResource][]manifest.ProducerTarget
	producerFacts         atomic.Pointer[map[manifest.BaseManifest]producerFact]
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

// New initializes discovery state and configures the Loader to wipe secrets.
func New(s *store.Store) *Loader {
	return &Loader{
		Store: s, Options: options{WipeSecrets: true},
		Index:     &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		documents: documentReader{cache: map[string]document{}, views: map[string]fs.FS{}},
	}
}

// Sources yields eligible immutable source-artifact evidence in unspecified order.
func (l *Loader) Sources(ctx context.Context) iter.Seq2[store.Child, error] {
	return func(yield func(store.Child, error) bool) {
		for id, r := range l.Index.Files() {
			if !id.KindInfo().SourceArtifact {
				continue
			}
			if l.documents.cache[r.Path].eligible == fileInactive {
				continue
			}
			obj := l.IndexedObject(id, r)
			if l.documents.compact {
				var err error
				if occurrence, ok := l.locations[id]; ok && obj.Obj == nil {
					obj, err = l.exactObject(ctx, occurrence, nil)
				}
				if err != nil {
					yield(store.Child{}, err)
					return
				}
				if obj.Obj != nil {
					if l.candidates == nil {
						l.candidates = map[manifest.NamedResource]store.Child{}
					}
					l.candidates[id] = obj
				}
			}
			if obj.Obj != nil && !yield(obj, nil) {
				return
			}
		}
	}
}

// AdmitsRoot applies the source-qualified scope boundary to every indexed document.
func (l *Loader) AdmitsRoot(obj manifest.BaseManifest, file string) bool {
	if obj == nil || !manifest.AdmitsRoot(obj) {
		return false
	}
	id := obj.Named()
	r, ok := l.Index.Lookup(id)
	if !ok || r.Path != file || file != "" && l.terminalInput(file) {
		return false
	}
	if r.Path == "" {
		native := l.documents.compact && r.Parent.ReportHidden() && r.Parent == r.BuilderOwner
		return r.Authored == (manifest.NamedResource{}) && (r.Parent == (manifest.NamedResource{}) || native) &&
			l.candidates[id].Obj != nil
	}
	if admitted, known := l.roots[id]; known {
		return admitted
	}
	if doc := l.documents.cache[file]; !id.KindInfo().Discovery && doc.rootKnown {
		return doc.rootAdmitted
	}
	return l.rootAdmission(file, r.File, id.KindInfo().Structural).admits(l, id, r)
}

type fileEligibility uint8

const (
	fileUnknown fileEligibility = iota
	fileInactive
	fileEligible
	fileIndependent
)

type rootAdmission struct {
	denied bool
	owners []manifest.NamedResource
	peers  [][]manifest.NamedResource
}

func (l *Loader) rootAdmission(file, coordinate string, structural bool) rootAdmission {
	eligible := l.documents.cache[file].eligible
	scope := rootAdmission{denied: eligible == fileInactive}
	if scope.denied {
		return scope
	}
	l.fileOwners(file, coordinate, func(owner manifest.NamedResource) bool {
		scope.owners = appendUniqueProducer(scope.owners, owner)
		return true
	})
	if !structural {
		return scope
	}
	for _, group := range l.Index.Claims {
		if !l.documents.cache[file].bootstrap || slices.ContainsFunc(group, func(owner manifest.NamedResource) bool {
			record, _ := l.Index.Lookup(owner)
			return record.Path == file
		}) {
			scope.peers = append(scope.peers, group)
		}
	}
	return scope
}

func (l *Loader) fileOwners(file, coordinate string, yield func(manifest.NamedResource) bool) {
	if l.documents.cache[file].eligible == fileInactive {
		return
	}
	if origin := l.documents.localPath(file); origin != file {
		coordinate = l.fileCoordinate(origin)
	}
	if l.scopes != nil {
		for _, v := range l.scopes.visits[file] {
			if v.owner != (manifest.NamedResource{}) && v.role == kustomizeinput.Resource && !yield(v.owner) {
				return
			}
		}
	}
	if coordinate == "" || l.documents.cache[file].eligible == fileIndependent {
		return
	}
	if len(l.Index.Claims) != 0 {
		for _, id := range l.Index.Claims[coordinate+"/"] {
			if !yield(id) {
				return
			}
		}
		for end, group := range claimpath.Groups(l.Index.Claims, coordinate) {
			if end == 0 && (coordinate == ".." || strings.HasPrefix(coordinate, "../")) {
				continue
			}
			for _, id := range group {
				if !yield(id) {
					return
				}
			}
		}
	}
	for reader := range l.Index.ReadersOfFile(coordinate) {
		if !reader.Exact && !yield(reader.Reader) {
			return
		}
	}
}

func (a rootAdmission) admits(l *Loader, id manifest.NamedResource, record manifest.DiscoveryRecord) bool {
	if a.denied {
		return false
	}
	var peers [][]manifest.NamedResource
	if id.KindInfo().Structural {
		for _, group := range a.peers {
			if slices.ContainsFunc(group, func(peer manifest.NamedResource) bool { return l.selfClaim(peer, record.Authored) }) {
				peers = append(peers, group)
			}
		}
	}
	for _, owner := range a.owners {
		if owner == id || l.selfClaim(owner, record.Authored) {
			continue
		}
		peer, _ := l.Index.Lookup(owner)
		if l.documents.cache[record.Path].bootstrap && peer.Path != record.Path {
			return false
		}
		if !slices.ContainsFunc(peers, func(group []manifest.NamedResource) bool { return slices.Contains(group, owner) }) {
			return false
		}
	}
	return true
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
	return indexed && l.documents.cache[r.Path].eligible == fileInactive
}

func (l *Loader) kustomizations() []store.Child {
	objects := l.Store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
	out := make([]store.Child, 0, len(objects))
	l.eachKustomization(objects, func(child store.Child) { out = append(out, child) })
	slices.SortFunc(out, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	return out
}

func (l *Loader) eachKustomization(objects []manifest.BaseManifest, yield func(store.Child)) {
	for _, obj := range objects {
		if _, _, applies := obj.SourcePath(); !applies {
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
	for id, r := range l.Index.Files() {
		if id.Group != manifest.FluxKustomizeDomain || id.Kind != manifest.KindKustomization || l.Store.GetObject(id) != nil {
			continue
		}
		child := l.IndexedObject(id, r)
		if _, ok := child.Obj.(*manifest.Kustomization); ok {
			yield(child)
		}
	}
}

// IndexedObject returns the retained declaration under its contextual identity.
func (l *Loader) IndexedObject(id manifest.NamedResource, r manifest.DiscoveryRecord) store.Child {
	child := l.candidates[id]
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
	if occurrence, ok := l.occurrence(id); ok {
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
func (l *Loader) Load(ctx context.Context, root string) (int, error) {
	if l.Store == nil {
		return 0, errors.New("loader: Store is nil")
	}
	l.documents.compact = l.Options.Selection.Changed || len(l.Options.Selection.Roots) != 0
	l.mountSources()
	abs, err := filepath.Abs(root)
	if err != nil {
		return 0, err
	}
	inputRoot, err := filepath.Abs(cmp.Or(l.SourceRoot, abs))
	if err != nil {
		return 0, err
	}
	if l.documents.mountedRoot(abs) == "" && (l.SourceRoot == "" || l.documents.mountedRoot(inputRoot) == "") {
		location, err := gitrepo.Locate(ctx, inputRoot, inputRoot)
		if err != nil {
			return 0, err
		}
		input, err := gittree.AdmitDirectory(ctx, location)
		if err != nil {
			if l.IgnoreFile != "" && errors.Is(err, fs.ErrNotExist) {
				return 0, nil
			}
			return 0, err
		}
		l.documents.views[inputRoot] = input.FS.(*gittree.FS).OperationView()
		l.documents.edgesDirty = true
	}
	if l.documents.bootstrap == "" {
		l.documents.bootstrap = inputRoot
	}
	l.ensureScopes()
	_, builder := l.documents.read(abs)
	if l.entry == entryUnset {
		l.entryPath = abs
		l.entry = entryPlain
		if builder != "" {
			l.entry = entryKustomize
		}
	}
	if _, scanned := l.scopes.scanned[abs]; scanned {
		return 0, nil
	}
	l.scopes.complete = false
	var ignore *ignoreSet
	if l.IgnoreFile != "" {
		input, name := l.documents.input(abs)
		info, statErr := fs.Stat(input, name)
		l.documents.rememberStat(abs, info, statErr)
		if errors.Is(statErr, fs.ErrNotExist) || statErr == nil && !info.IsDir() {
			return 0, nil
		}
		if statErr != nil {
			return 0, statErr
		}
		ignore, err = l.documents.loadIgnoreFile(ctx, l.IgnoreFile, abs)
	} else {
		ignore, err = l.documents.loadIgnore(abs)
	}
	if err != nil {
		return 0, err
	}
	w := traversal{
		documents: l.documents, policy: traversalAdmission, loader: l,
		ignore: ignore, directories: map[string]struct{}{},
		build: builder != "", ignoreCache: map[ignoreKey]bool{},
	}
	if l.documents.compact {
		source := l.sourceFacts(abs)
		key := loadContext{path: abs, ignore: l.IgnoreFile}
		if facts, ok := source.loads[key]; ok {
			for file, directory := range facts.directories {
				l.documents.restore(file)
				doc := l.documents.cache[file]
				doc.adHoc, doc.traversed = doc.adHoc || directory.adHoc, doc.traversed || directory.traversed
				l.documents.cache[file] = doc
			}
			for _, file := range facts.files {
				if !w.proceed(ctx) {
					break
				}
				n, err := l.replayFile(ctx, file)
				w.count += n
				if err != nil {
					w.err = err
					break
				}
			}
		} else {
			files := []fileAdmission{}
			w.files = &files
			w.walk(ctx, graphVisit{root: abs, file: abs}, nil)
			if w.proceed(ctx) {
				facts := loadFacts{files: files, directories: map[string]loadDirectory{}}
				for file := range w.directories {
					doc := l.documents.cache[file]
					facts.directories[file] = loadDirectory{adHoc: doc.adHoc, traversed: doc.traversed}
				}
				source.loads[key] = facts
			}
		}
	} else {
		w.walk(ctx, graphVisit{root: abs, file: abs}, nil)
	}
	if w.proceed(ctx) {
		l.scopes.scanned[abs] = struct{}{}
		if l.documents.compact {
			l.construction++
		}
	}
	return w.count, w.err
}

// Authored input replaces synthetic aliases and their ready artifacts.
func (l *Loader) acceptObject(id manifest.NamedResource) bool {
	if !l.PreferExisting {
		return true
	}
	if l.Index.Records[id].Path != "" {
		return false
	}
	if l.Store.GetObject(id) == nil {
		return true
	}
	artifact, _ := l.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if artifact == nil || !artifact.InputAlias {
		return false
	}
	l.Store.DeleteObject(id)
	return true
}

func (l *Loader) loadFile(ctx context.Context, path string) (int, error) {
	if l.documents.parsed != nil {
		l.documents.restore(path)
	}
	if l.documents.cache[path].loaded || l.documents.cache[path].selected {
		return 0, nil
	}
	reader := l.documents
	reader.regular = true
	var objs []parsedObject
	var err error
	if reader.compact {
		objs, err = reader.censusFile(ctx, path, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
	} else {
		objs, err = reader.fileObjects(path, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, child := range objs {
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
	cached := l.documents.cache[path]
	cached.objects, cached.loaded = objs, true
	l.documents.cache[path] = cached
	return count, nil
}

// recordDataFile keeps Component data available for lazy promotion without
// publishing untemplated Component resources into the Store.
func (l *Loader) recordDataFile(ctx context.Context, absPath string) error {
	if l.documents.cache[absPath].selected {
		return nil
	}
	if l.documents.compact {
		facts, err := l.documents.censusFile(ctx, absPath, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
		if err != nil {
			return err
		}
		for _, child := range facts {
			if child.outcome.Err != nil {
				continue
			}
			if child.fact.Identity.KindInfo().Data {
				l.recordOccurrence(absPath, child.fact)
			}
		}
		return nil
	}
	objs, err := l.documents.parseFile(ctx, absPath, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets, DefaultNamespace: ""})
	if err != nil {
		return err
	}
	for _, child := range objs {
		obj := child.Obj
		id := obj.Named()
		if !manifest.KindOf(obj).Data {
			// Non-data kinds (most commonly templated Flux CRs in a
			// Component subtree) intentionally stay invisible — the
			// producer-index use case is data-only.
			continue
		}
		if !l.acceptObject(id) {
			continue
		}
		l.recordObject(obj, absPath)
	}
	return nil
}

func (l *Loader) recordOccurrence(path string, o *Occurrence) {
	if o.facts.uncertain || !o.facts.tracked && len(o.facts.targets) == 0 && !o.facts.needsCandidates {
		return
	}
	if !l.acceptObject(o.Identity) {
		return
	}
	if l.locations == nil {
		l.locations = map[manifest.NamedResource]Occurrence{}
	}
	l.locations[o.Identity] = *o
	r := manifest.DiscoveryRecord{Authored: o.Authored, Path: path, File: l.fileCoordinate(path)}
	for _, ref := range o.facts.refs {
		if discoveryReference(ref) {
			r.Refs = append(r.Refs, ref.NamedResource)
		}
	}
	l.Index.Records[o.Identity] = r
}

// recordObject retains raw locations and authored source references for deferred admission.
func (l *Loader) recordObject(obj manifest.BaseManifest, absPath string) {
	id := obj.Named()
	if l.candidates[id].Obj != nil {
		if record, exists := l.Index.Lookup(id); exists && record.Path == "" {
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
		if discoveryReference(ref) {
			refs = append(refs, ref.NamedResource)
		}
	}
	return refs
}

func discoveryReference(ref manifest.Reference) bool {
	return ref.Use == "valuesFrom" || ref.Use == "substituteFrom" && ref.Group == "" && ref.Kind == manifest.KindConfigMap ||
		(ref.Use == "source" || ref.Use == "chart" || ref.Use == "artifact") && ref.Kind != "" && ref.Name != ""
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

func (l *Loader) replayFile(ctx context.Context, file fileAdmission) (int, error) {
	if file.kind == admissionWarning {
		l.Store.AddWarning(file.warning)
		return 0, nil
	}
	if file.kind == admissionBootstrap {
		l.documents.restore(file.file)
		for _, child := range l.documents.cache[file.file].objects {
			if child.fact == nil || child.fact.Declaration != file.coordinate || !l.acceptObject(child.fact.Identity) {
				continue
			}
			l.recordOccurrence(file.file, child.fact)
			doc := l.documents.cache[file.file]
			doc.bootstrap = true
			l.documents.cache[file.file] = doc
			return 1, nil
		}
		return 0, nil
	}
	var n int
	var err error
	if file.kind == admissionData {
		err = l.recordDataFile(ctx, file.file)
	} else {
		n, err = l.loadFile(ctx, file.file)
	}
	if err != nil && (file.kind == admissionPackage || file.kind == admissionData) {
		message := "component data file failed to parse"
		if file.kind == admissionPackage {
			message = "kustomization file failed to parse"
		}
		l.Store.AddWarning(manifest.Warning{Category: manifest.WarnInputParseFailed, Message: message, Detail: []string{file.file, err.Error()}})
		return n, nil
	}
	if err != nil {
		return n, fmt.Errorf("loader: %w", err)
	}
	return n, nil
}
