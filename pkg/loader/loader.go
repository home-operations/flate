// Package loader hydrates a Store from Flux source filesystems.
package loader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Options tunes the Loader.
type Options struct {
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets  bool
	RemoteInputs *source.HTTPInputs
	RemoteRetry  cache.RetryConfig
}

// Loader collects source-qualified discovery evidence before admitting roots.
// Source membership MUST remain stable across sequential Load calls. The first
// Load classifies the entry for all subsequent loads; independent entry policies
// require separate loaders.
type Loader struct {
	Store   *store.Store
	Options Options
	// DisableChartDigestTracking records inline instance patches before admission.
	// Load calls sharing a Loader MUST be sequential.
	DisableChartDigestTracking bool

	// SourceRoot anchors slash-separated discovery coordinates and bounds
	// standalone input admission. Source artifacts supply their own FS.
	SourceRoot string

	// Index records static discovery facts and is initialized by New. Caller
	// replacements MUST initialize Records before Load. Mutable until FinalizeIndex returns.
	Index      *manifest.DiscoveryIndex
	documents  documentReader
	scopes     *scopeConstruction
	entry      entryKind
	candidates map[manifest.NamedResource]manifest.BaseManifest
	admitted   map[manifest.NamedResource]manifest.BaseManifest

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

// KustomizeRoot reports recognized builder presence in a previously loaded directory.
func (l *Loader) KustomizeRoot(root string) bool {
	_, file := l.documents.read(root)
	return file != ""
}

// Sources yields immutable source-artifact evidence in unspecified order.
func (l *Loader) Sources() iter.Seq[manifest.BaseManifest] {
	return func(yield func(manifest.BaseManifest) bool) {
		for id, r := range l.Index.Files() {
			if !id.KindInfo().SourceArtifact {
				continue
			}
			obj := l.indexedObject(id, r)
			if obj != nil && !yield(obj) {
				return
			}
		}
	}
}

// AdmitsRoot applies the source-qualified scope boundary to every indexed document.
func (l *Loader) AdmitsRoot(id manifest.NamedResource, file string) bool {
	r, ok := l.Index.Lookup(id)
	if !ok || r.Path != file {
		return false
	}
	if r.Path == "" {
		return r.Authored == (manifest.NamedResource{}) && l.candidates[id] != nil
	}
	if l.entry != entryKustomize && id.KindInfo().Discovery && l.candidates[id] != nil && l.documents.cache[file].loaded && !l.documents.cache[file].bootstrap {
		return true
	}
	return l.rootAdmission(file, r.File, id.KindInfo().Structural).admits(l, id, r)
}

type rootAdmission struct {
	owners []manifest.NamedResource
	peers  [][]manifest.NamedResource
}

func (l *Loader) rootAdmission(file, coordinate string, structural bool) rootAdmission {
	var scope rootAdmission
	if l.scopes != nil {
		for _, v := range l.scopes.visits[file] {
			if v.owner != (manifest.NamedResource{}) {
				scope.owners = appendUniqueProducer(scope.owners, v.owner)
			}
		}
	}
	if origin := l.documents.localPath(file); origin != file {
		coordinate = l.fileCoordinate(origin)
	}
	owners, ancestors := l.Index.OwnersOfFile(coordinate)
	for _, owner := range append(owners, ancestors...) {
		scope.owners = appendUniqueProducer(scope.owners, owner)
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

// Kustomizations returns admitted and reachable source-qualified owner evidence.
func (l *Loader) Kustomizations() []*manifest.Kustomization {
	out := l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	for id, r := range l.Index.Files() {
		if id.Group != manifest.FluxKustomizeDomain || id.Kind != manifest.KindKustomization || l.Store.GetObject(id) != nil {
			continue
		}
		obj := l.indexedObject(id, r)
		if ks, ok := obj.(*manifest.Kustomization); ok {
			out = append(out, ks)
		}
	}
	slices.SortFunc(out, func(a, b *manifest.Kustomization) int { return a.Named().Compare(b.Named()) })
	return out
}

func (l *Loader) indexedObject(id manifest.NamedResource, r manifest.DiscoveryRecord) manifest.BaseManifest {
	if obj := l.candidates[id]; obj != nil {
		return obj
	}
	obj := l.authoredObject(r.Path, r.Authored)
	if obj != nil && obj.Named().Namespace != id.Namespace {
		obj = obj.WithNamespace(id.Namespace)
	}
	return obj
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
	w := walker{
		loader:      l,
		ignore:      ignore,
		visited:     map[string]struct{}{},
		scanRoot:    abs,
		build:       builder != "",
		ignoreCache: map[ignoreKey]bool{},
	}
	n, err := w.descend(ctx, abs)
	if err == nil {
		l.scopes.scanned[abs] = struct{}{}
	}
	return n, err
}

// walker carries the per-Load state — ignore matcher, visited-dir
// dedup, loader back-reference — so the recursive functions don't
// need to thread the same args through every call.
type walker struct {
	loader *Loader
	ignore *ignoreSet
	// scanRoot is the original Load() root that ignore patterns are
	// authored against. Every ignore.matches call must pass scanRoot
	// (not the current sub-package dir), otherwise patterns like
	// `apps/junk/**` silently fail to match anything inside a
	// descended sub-package — the rel path would be sub-tree-relative
	// instead of scan-root-relative.
	scanRoot string
	build    bool
	visited  map[string]struct{}

	// ignoreCache memoizes ignore.matches results for the duration of
	// one Load. matches does a filepath.Rel + slash-split + gitignore
	// match per call, and the file path / isDir tuple are the only
	// inputs — same path always yields the same answer. The cache
	// turns a per-file matcher walk into a per-unique-path matcher
	// walk; deep package trees with thousands of resources gain the
	// most. scanRoot isn't part of the key because it's fixed for
	// the walker's lifetime; mixing different roots in one cache
	// would silently poison results.
	//
	// Unsynchronized: a walker is single-threaded per Load (same
	// pattern as visited above). Concurrent Loads each construct
	// their own walker.
	ignoreCache map[ignoreKey]bool
}

// ignoreKey identifies one ignore.matches lookup. isDir matters
// because gitignore's trailing-slash dirOnly patterns evaluate
// differently for files vs. directories — see ignoreSet.matches.
type ignoreKey struct {
	path  string
	isDir bool
}

// ignoreMatches is the walker's memoized wrapper around
// ignoreSet.matches. Routes every call through ignoreCache so
// repeated checks against the same path (the common case: the same
// file gets stat'd and walked) cost one map probe instead of a fresh
// filepath.Rel + gitignore matcher walk.
func (w *walker) ignoreMatches(path string, isDir bool) bool {
	if w.ignore == nil || w.ignore.matcher == nil {
		// Mirrors ignoreSet.matches's nil short-circuit so we don't
		// pollute the cache with cold-path entries that would never
		// be hit again.
		return false
	}
	key := ignoreKey{path: path, isDir: isDir}
	if v, ok := w.ignoreCache[key]; ok {
		return v
	}
	v := w.ignore.matches(path, w.scanRoot, isDir)
	w.ignoreCache[key] = v
	return v
}

// descend dispatches on the kind of directory dir is:
//   - Kustomize Component: skipped entirely (transforms applied at
//     render time, not loaded as standalone Flux CRs).
//   - Kustomize package (has kustomization.yaml, kind != Component):
//     follow the resource graph via walkKustomize.
//   - Ad-hoc directory (no kustomization.yaml): walk the filesystem,
//     loading every YAML and switching to walkKustomize on encounter
//     of a sub-package.
//   - Already-visited: no-op (cycle protection for circular
//     `resources:` references).
func (w *walker) descend(ctx context.Context, dir string) (int, error) {
	if cerr := ctx.Err(); cerr != nil {
		return 0, cerr
	}
	if _, seen := w.visited[dir]; seen {
		return 0, nil
	}
	w.visited[dir] = struct{}{}

	k, kpath := w.loader.documents.read(dir)
	if kpath != "" && k == nil {
		return 0, nil
	}
	if k != nil {
		if isKustomizeComponent(k) {
			slog.Debug("loader: skipping kustomize Component directory", "dir", dir)
			// Component data must retain producer evidence without admitting
			// templated Flux resources before their parent's render.
			return w.walkComponentData(ctx, dir)
		}
		return w.walkKustomize(ctx, dir, kpath)
	}
	return w.walkAdHoc(ctx, dir)
}

func (w *walker) walkKustomize(ctx context.Context, dir, kpath string) (int, error) {
	cached := w.loader.documents.cache[dir]
	cached.traversed = true
	w.loader.documents.cache[dir] = cached
	count := 0
	if kpath != "" && !w.ignoreMatches(kpath, false) {
		n, err := w.loader.loadFile(kpath)
		if err != nil {
			w.loader.Store.AddWarning(manifest.NamedResource{}, manifest.Warning{
				Category: manifest.WarnInputParseFailed, Message: "kustomization file failed to parse", Detail: []string{kpath, err.Error()},
			})
		}
		count += n
	}
	spec, _ := w.loader.documents.read(dir)
	for ref := range kustomizeinput.Kustomization(spec) {
		if ref.Role != kustomizeinput.Resource && ref.Role != kustomizeinput.Component {
			continue
		}
		if err := ctx.Err(); err != nil {
			return count, err
		}
		abs, ok := kustpath.Resolve(dir, ref.Path, kustpath.Directory)
		if !ok {
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		w.loader.documents.rememberStat(abs, info, err)
		if err != nil {
			continue
		}
		if info.IsDir() {
			n, err := w.descend(ctx, abs)
			if err != nil {
				return count, err
			}
			count += n
		} else if ref.Role == kustomizeinput.Resource {
			if _, ok := kustpath.Resolve(dir, ref.Path, kustpath.File); !ok {
				continue
			}
			if !isManifestFile(abs) || w.ignoreMatches(abs, false) {
				continue
			}
			n, err := w.loader.loadFile(abs)
			if err != nil {
				return count, fmt.Errorf("loader: %w", err)
			}
			count += n
		}
	}
	if !w.build {
		count += w.scanBootstrapFluxKS(dir, kpath)
	}
	return count, nil
}

func (w *walker) scanBootstrapFluxKS(dir, kpath string) int {
	seen := map[string]struct{}{kpath: {}}
	spec, _ := w.loader.documents.read(dir)
	for ref := range kustomizeinput.Kustomization(spec) {
		if ref.Inline {
			continue
		}
		if abs, ok := kustpath.Resolve(dir, ref.Path, kustpath.Dependency); ok {
			seen[abs] = struct{}{}
		}
	}
	input, name := w.loader.documents.input(dir)
	entries, err := fs.ReadDir(input, name)
	if err != nil {
		w.loader.Store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnInputReadFailed, Message: "bootstrap-sibling readdir failed", Detail: []string{dir, err.Error()},
		})
		return 0
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		abs := filepath.Join(dir, e.Name())
		if !isManifestFile(abs) || w.ignoreMatches(abs, false) {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		reader := w.loader.documents
		reader.regular = e.Type().IsRegular()
		objs, err := reader.parseFile(abs, manifest.ParseDocOptions{WipeSecrets: w.loader.Options.WipeSecrets, DefaultNamespace: ""})
		if err != nil {
			w.loader.Store.AddWarning(manifest.NamedResource{}, manifest.Warning{
				Category: manifest.WarnInputParseFailed, Message: "bootstrap-sibling parse failed", Detail: []string{abs, err.Error()},
			})
			continue
		}
		for _, obj := range objs {
			ks, ok := obj.(*manifest.Kustomization)
			if !ok {
				continue
			}
			// A Flux-GVK patch fragment with neither spec.path nor sourceRef
			// must never become a reconcilable Kustomization. A sourceRef alone
			// is sufficient for a bootstrap entry that renders the source root.
			if ks.IsFragment() {
				continue
			}
			if !w.loader.acceptObject(obj.Named()) {
				continue
			}
			w.loader.recordObject(obj, abs)
			cached := w.loader.documents.cache[abs]
			cached.bootstrap = true
			w.loader.documents.cache[abs] = cached
			count++
		}
	}
	return count
}

// Component resources retain data evidence without admitting templated Flux objects.
func (w *walker) walkComponentData(ctx context.Context, dir string) (int, error) {
	cached := w.loader.documents.cache[dir]
	cached.traversed = true
	w.loader.documents.cache[dir] = cached
	spec, _ := w.loader.documents.read(dir)
	for ref := range kustomizeinput.Kustomization(spec) {
		if ref.Role != kustomizeinput.Resource && ref.Role != kustomizeinput.Component {
			continue
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		abs, ok := kustpath.Resolve(dir, ref.Path, kustpath.Directory)
		if !ok {
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		w.loader.documents.rememberStat(abs, info, err)
		if err != nil {
			continue
		}
		if ref.Role == kustomizeinput.Component {
			if !info.IsDir() {
				continue
			}
			if _, seen := w.visited[abs]; seen {
				continue
			}
			w.visited[abs] = struct{}{}
			nested, _ := w.loader.documents.read(abs)
			if !isKustomizeComponent(nested) {
				continue
			}
			if _, err := w.walkComponentData(ctx, abs); err != nil {
				return 0, err
			}
		} else {
			if _, ok := kustpath.Resolve(dir, ref.Path, kustpath.File); !ok || info.IsDir() || w.ignoreMatches(abs, false) || !isManifestFile(abs) {
				continue
			}
			if err := w.loader.recordDataFile(abs); err != nil {
				w.loader.Store.AddWarning(manifest.NamedResource{}, manifest.Warning{
					Category: manifest.WarnInputParseFailed, Message: "component data file failed to parse", Detail: []string{abs, err.Error()},
				})
			}
		}
	}
	return 0, nil
}

// walkAdHoc handles entry points that aren't themselves kustomize
// packages: walks the filesystem tree, loading every YAML, and
// switching to walkKustomize when it encounters a sub-directory
// that IS a kustomize package (the package's subtree is then
// graph-walked and the filesystem walk skips it via SkipDir).
//
// This preserves flate's pre-#346 behavior for "bare directory of
// flux CRs" entry shapes — e.g. a --path that doesn't have a
// kustomization.yaml at its root.
func (w *walker) walkAdHoc(ctx context.Context, root string) (int, error) {
	count := 0
	input, name := w.loader.documents.input(root)
	walkRoot := filepath.FromSlash(name)
	err := fs.WalkDir(input, name, func(name string, d fs.DirEntry, walkErr error) error {
		rel, _ := filepath.Rel(walkRoot, filepath.FromSlash(name))
		path := filepath.Join(root, rel)
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if w.shouldSkipDir(d.Name(), path) {
				return filepath.SkipDir
			}
			if path == root {
				return nil
			}
			if _, kpath := w.loader.documents.read(path); kpath != "" {
				n, err := w.descend(ctx, path)
				if err != nil {
					return err
				}
				count += n
				return filepath.SkipDir
			}
			return nil
		}
		if !isManifestFile(path) {
			return nil
		}
		if w.ignoreMatches(path, false) {
			return nil
		}
		n, err := w.loader.loadFile(path)
		if err != nil {
			return fmt.Errorf("loader: %w", err)
		}
		count += n
		return nil
	})
	return count, err
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
	artifact, _ := l.Store.GetArtifact(id).(*store.SourceArtifact)
	if artifact == nil || !artifact.InputAlias {
		return false
	}
	l.Store.DeleteObject(id)
	return true
}

// Decode errors retain their file coordinate until scope attribution.
func (l *Loader) loadFile(path string) (int, error) {
	if l.documents.cache[path].loaded || l.documents.cache[path].selected {
		return 0, nil
	}
	reader := l.documents
	reader.regular = true
	objs := []manifest.BaseManifest{}
	collect := func(obj manifest.BaseManifest) {
		if raw, ok := obj.(*manifest.RawObject); ok && !manifest.KindOf(raw).Tracked && len(manifest.ProducerTargets(raw)) == 0 {
			return
		}
		objs = append(objs, obj)
	}
	var err error
	if cached := l.documents.cache[path].objects; cached != nil {
		for _, obj := range cached {
			collect(obj)
		}
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
	for _, obj := range objs {
		info := manifest.KindOf(obj)
		if !info.Tracked {
			l.recordObject(obj, path)
			continue
		}
		if hr, ok := obj.(*manifest.HelmRelease); ok {
			l.DisableChartDigestTracking = l.DisableChartDigestTracking || disablesChartDigestTracking(hr)
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
	l.documents.cache[path] = document{objects: objs, loaded: true}
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
	for _, obj := range objs {
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
	if l.candidates[id] != nil {
		if record, exists := l.Index.Lookup(id); exists && record.Path == "" {
			return
		}
	}
	delete(l.candidates, id)
	r := manifest.DiscoveryRecord{Authored: id, Path: absPath, File: l.fileCoordinate(absPath)}
	var buf [16]manifest.NamedResource
	refs := buf[:0]
	for ref := range manifest.References(obj) {
		if !discoveryReference(ref) {
			continue
		}
		refs = append(refs, ref.NamedResource)
	}
	if len(refs) > 0 {
		r.Refs = slices.Clone(refs)
	}
	l.Index.Records[id] = r
}

func discoveryReference(ref manifest.Reference) bool {
	return ref.Use == "valuesFrom" || ref.Use == "substituteFrom" && ref.Content && ref.Group == "" && ref.Kind == manifest.KindConfigMap ||
		(ref.Use == "source" || ref.Use == "chart") && ref.Kind != "" && ref.Name != ""
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

// Explicit resources bypass scan pruning; negated descendants MUST remain reachable.
func (w *walker) shouldSkipDir(name, full string) bool {
	if sourceignore.PruneDir(name, true) {
		return true
	}
	if w.ignore != nil && w.ignore.hasNegation {
		return false
	}
	return w.ignoreMatches(full, true)
}
