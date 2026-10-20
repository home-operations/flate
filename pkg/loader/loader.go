// Package loader hydrates a Store from Flux source filesystems.
package loader

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Options tunes the Loader.
type Options struct {
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets bool
}

// Loader walks source views and adds Flux objects to a Store. Source membership
// MUST remain stable across its Load calls.
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
	Index     *manifest.DiscoveryIndex
	documents documentReader
	scopes    *scopeConstruction

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

// New initializes discovery state and configures the Loader to wipe secrets.
func New(s *store.Store) *Loader {
	return &Loader{
		Store: s, Options: Options{WipeSecrets: true},
		Index:     &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		documents: documentReader{cache: map[string]document{}, views: map[string]fs.FS{}},
	}
}

// Load discovers Flux objects under root by walking the kustomize
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
	l.scopes.complete = false
	if _, scanned := l.scopes.scanned[abs]; scanned {
		return 0, nil
	}
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
	count := 0
	if kpath != "" && !w.ignoreMatches(kpath, false) {
		n, err := w.loader.loadFile(kpath)
		if err != nil {
			slog.Warn("loader: kustomization file failed to parse", "path", kpath, "err", err)
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
	return count + w.scanBootstrapFluxKS(dir, kpath), nil
}

func (w *walker) scanBootstrapFluxKS(dir, kpath string) int {
	// A genuine bootstrap *entry* KS is a root — no other Kustomization's
	// spec.path covers its directory. A KS commented-out of (excluded from)
	// a kustomization.yaml lives inside a tree another KS already renders
	// (e.g. cluster-apps' spec.path covers apps/<cluster>/<ns>/); real Flux
	// renders that dir solely from its kustomization.yaml `resources:`, so a
	// sibling KS left out of them is a disabled app, not a bootstrap entry.
	// Surfacing it here is a false positive. Skip the scan when dir is already
	// claimed by another loaded KS's spec.path — the same coverage predicate
	// promoteOrphans applies via manifest.StrictParent.
	if w.loader.dirCoveredByOtherKS(dir) {
		return 0
	}
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
		slog.Warn("loader: bootstrap-sibling readdir failed", "dir", dir, "err", err)
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
			slog.Warn("loader: bootstrap-sibling parse failed", "path", abs, "err", err)
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
			w.loader.addObject(obj, abs)
			count++
		}
	}
	return count
}

// dirCoveredByOtherKS reports whether dir (an absolute kustomize-package
// directory) sits under some loaded Flux Kustomization's spec.path — i.e. it
// is part of a tree another KS renders, where a sibling KS excluded from the
// local kustomization.yaml is a disabled app rather than a bootstrap entry.
// Claims MUST use the scanned source view, never another alias or pin.
// Without SourceRoot, discovery retains direct-input bootstrap admission.
//
// A KS being surfaced by scanBootstrapFluxKS is not yet in the Store at its
// own dir's first scan, so a true entry KS reads as uncovered and is still
// surfaced; only a strictly-enclosing (or equal) prefix from an already-loaded
// KS marks the dir covered.
func (l *Loader) dirCoveredByOtherKS(dir string) bool {
	if l.SourceRoot == "" {
		return false
	}
	root := l.documents.mountedRoot(dir)
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return false
	}
	dirRel := filepath.ToSlash(rel) + "/"
	for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
		if ks.IsFragment() {
			continue
		}
		sourceRoot, _, _ := SourceView(l.Store, ks.SourceID(), l.SourceRoot)
		if sourceRoot != root {
			continue
		}
		base := l.documents.claimBase(root, ks.Path)
		if strings.HasPrefix(dirRel, base+"/") {
			return true
		}
	}
	return false
}

// Component resources retain data evidence without admitting templated Flux objects.
func (w *walker) walkComponentData(ctx context.Context, dir string) (int, error) {
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
				slog.Warn("loader: component data file failed to parse", "path", abs, "err", err)
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
			// Subdirectory: if it's a kustomize package, switch to
			// graph walk and SkipDir to keep filepath.WalkDir from
			// descending again. Pull the file path out of the same
			// read so walkKustomize doesn't open kustomization.yaml a
			// second time.
			if k, kpath := w.loader.documents.read(path); k != nil {
				if isKustomizeComponent(k) {
					if _, err := w.walkComponentData(ctx, path); err != nil {
						return err
					}
					return filepath.SkipDir
				}
				w.visited[path] = struct{}{}
				n, err := w.walkKustomize(ctx, path, kpath)
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
	if !l.PreferExisting || l.Store.GetObject(id) == nil {
		return true
	}
	artifact, _ := l.Store.GetArtifact(id).(*store.SourceArtifact)
	if artifact == nil || !artifact.InputAlias || l.Index.Records[id].Path != "" {
		return false
	}
	l.Store.DeleteObject(id)
	return true
}

// Decode errors retain their file coordinate until scope attribution.
func (l *Loader) loadFile(path string) (int, error) {
	if l.documents.cache[path].loaded {
		return 0, nil
	}
	reader := l.documents
	reader.regular = true
	objs := []manifest.BaseManifest{}
	collect := func(obj manifest.BaseManifest) {
		if raw, ok := obj.(*manifest.RawObject); ok && len(manifest.ProducerTargets(raw)) == 0 {
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
		if !info.Discovery {
			// Under render-driven discovery, HRs / ConfigMaps /
			// Secrets / raw manifests stay out of the Store at file-
			// load time. They reach the Store via KS render
			// emission (emitRenderedChildren), depwait's lazy-
			// promotion fallback, or the orchestrator's orphan
			// sweep. The discovery index records the {id, path}
			// pair so the depwait fallback can re-parse the file
			// on demand without deadlocking the producing KS.
			l.recordObject(obj, path)
			continue
		}
		l.addObject(obj, path)
		count++
	}
	l.documents.cache[path] = document{objects: objs, loaded: true}
	return count, nil
}

// addObject publishes admitted objects and their immutable discovery coordinates.
func (l *Loader) addObject(obj manifest.BaseManifest, path string) {
	l.Store.AddObject(obj)
	l.recordObject(obj, path)
}

// recordDataFile keeps Component data available for lazy promotion without
// publishing untemplated Component resources into the Store.
func (l *Loader) recordDataFile(absPath string) error {
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
