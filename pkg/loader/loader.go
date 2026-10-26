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

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Options tunes the Loader.
type Options struct {
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
	Options Options
	// SourceRoot anchors slash-separated discovery coordinates and bounds
	// standalone input admission. Source artifacts supply their own FS.
	SourceRoot string

	// Index records static discovery facts and is initialized by New. Caller
	// replacements MUST initialize Records before Load. Mutable until FinalizeIndex returns.
	Index      *manifest.DiscoveryIndex
	documents  documentReader
	scopes     *scopeConstruction
	entry      entryKind
	entryPath  string
	candidates map[manifest.NamedResource]store.Child
	admitted   map[manifest.NamedResource]manifest.BaseManifest
	roots      map[manifest.NamedResource]bool

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
		Store: s, Options: Options{WipeSecrets: true},
		Index:     &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		documents: documentReader{cache: map[string]document{}, views: map[string]fs.FS{}},
	}
}

// Sources yields eligible immutable source-artifact evidence in unspecified order.
func (l *Loader) Sources() iter.Seq[store.Child] {
	return func(yield func(store.Child) bool) {
		for id, r := range l.Index.Files() {
			if !id.KindInfo().SourceArtifact {
				continue
			}
			if l.documents.cache[r.Path].eligible == fileInactive {
				continue
			}
			obj := l.indexedObject(id, r)
			if obj.Obj != nil && !yield(obj) {
				return
			}
		}
	}
}

// AdmitsRoot applies the source-qualified scope boundary to every indexed document.
func (l *Loader) AdmitsRoot(id manifest.NamedResource, file string) bool {
	r, ok := l.Index.Lookup(id)
	if !ok || r.Path != file || file != "" && l.terminalInput(file) {
		return false
	}
	if r.Path == "" {
		return r.Authored == (manifest.NamedResource{}) && r.Parent == (manifest.NamedResource{}) &&
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
	if l.scopes != nil {
		for _, v := range l.scopes.visits[file] {
			if v.owner != (manifest.NamedResource{}) && v.role == kustomizeinput.Resource {
				scope.owners = appendUniqueProducer(scope.owners, v.owner)
			}
		}
	}
	if origin := l.documents.localPath(file); origin != file {
		coordinate = l.fileCoordinate(origin)
	}
	if eligible != fileIndependent {
		owners, ancestors := l.Index.OwnersOfFile(coordinate)
		for _, owner := range append(owners, ancestors...) {
			scope.owners = appendUniqueProducer(scope.owners, owner)
		}
	}
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
		ks := child.Obj.(*manifest.Kustomization)
		r, indexed := l.Index.Lookup(ks.Named())
		return indexed && l.documents.cache[r.Path].eligible == fileInactive
	})
}

func (l *Loader) kustomizations() []store.Child {
	objects := l.Store.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)
	out := make([]store.Child, 0, len(objects))
	for _, obj := range objects {
		_, doc, emission := l.Store.GetEmission(obj.Named())
		emission.Finish()
		out = append(out, store.Child{Obj: obj, Doc: doc})
	}
	for id, r := range l.Index.Files() {
		if id.Group != manifest.FluxKustomizeDomain || id.Kind != manifest.KindKustomization || l.Store.GetObject(id) != nil {
			continue
		}
		child := l.indexedObject(id, r)
		if _, ok := child.Obj.(*manifest.Kustomization); ok {
			out = append(out, child)
		}
	}
	slices.SortFunc(out, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	return out
}

func (l *Loader) indexedObject(id manifest.NamedResource, r manifest.DiscoveryRecord) store.Child {
	if child := l.candidates[id]; child.Obj != nil {
		return child
	}
	child := l.authoredObject(r.Path, r.Authored)
	if child.Obj != nil && child.Obj.Named().Namespace != id.Namespace {
		child.Obj = child.Obj.WithNamespace(id.Namespace)
	}
	return child
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
	w.walk(ctx.Err, graphVisit{root: abs, file: abs}, nil)
	if w.proceed(ctx.Err) {
		l.scopes.scanned[abs] = struct{}{}
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

// Decode errors retain their file coordinate until scope attribution.
func (l *Loader) loadFile(path string) (int, error) {
	if l.documents.parsed != nil {
		l.documents.restore(path)
	}
	if l.documents.cache[path].loaded || l.documents.cache[path].selected {
		return 0, nil
	}
	reader := l.documents
	reader.regular = true
	objs := []parsedObject{}
	collect := func(child store.Child, ordinal int, version string) {
		objs = append(objs, parsedObject{child, ordinal, version})
	}
	var err error
	if cached := l.documents.cache[path]; cached.objects != nil {
		objs = cached.objects
	} else {
		err = reader.decodeFileObjects(path, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets, DefaultNamespace: ""}, true, collect)
	}
	if err != nil {
		if !errors.Is(err, manifest.ErrInput) {
			return 0, err
		}
		if l.Index.Errors == nil {
			l.Index.Errors = make(map[string]error)
		}
		file := l.fileCoordinate(path)
		if _, seen := l.Index.Errors[file]; !seen {
			l.Index.Errors[file] = fmt.Errorf("loader: %w", err)
		}
		return 0, nil
	}
	count := 0
	for _, child := range objs {
		obj := child.Obj
		info := manifest.KindOf(obj)
		if !info.Tracked {
			if len(manifest.ProducerTargets(obj)) != 0 {
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
func (l *Loader) recordDataFile(absPath string) error {
	if l.documents.cache[absPath].selected {
		return nil
	}
	objs, err := l.documents.parseFile(absPath, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets, DefaultNamespace: ""})
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
	r.Refs = discoveryReferences(obj)
	l.Index.Records[id] = r
}

func discoveryReferences(obj manifest.BaseManifest) []manifest.NamedResource {
	var buf [16]manifest.NamedResource
	refs := buf[:0]
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if discoveryReference(ref) {
			refs = append(refs, ref.NamedResource)
		}
	}
	if len(refs) != 0 {
		return slices.Clone(refs)
	}
	return nil
}

func discoveryReference(ref manifest.Reference) bool {
	return ref.Use == "valuesFrom" || ref.Use == "substituteFrom" && ref.Content && ref.Group == "" && ref.Kind == manifest.KindConfigMap ||
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
