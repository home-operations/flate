// Package loader hydrates a Store from Flux source filesystems.
//
// The loader's discovery model mirrors `kustomize build` (and
// flux-local): the kustomize resource graph is the source of truth.
// A directory with a kustomization.yaml defines a kustomize package
// — the loader follows its `resources:` entries (files load,
// directories recurse) and ignores everything else in the directory.
// Files outside the resource graph are invisible by construction;
// there is no "tree walk + filter" post-pass, no orphan-skip rule,
// no reachability set computed up front.
//
// Entry points without a kustomization.yaml use a fall-back tree
// walk that loads every YAML it finds and switches into graph-walk
// mode when it encounters a subdirectory that IS a kustomize
// package. This keeps the bootstrap-style "bare directory of CRs"
// shape working without forcing every user to wrap their entry
// point in a kustomization.yaml.
//
// Each loader.Load call is one independent graph root. The
// orchestrator's iterative discovery — a Flux KS's spec.path
// triggers another Load — composes naturally: each spec.path is its
// own graph root with its own resource graph.
package loader

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Options tunes the Loader.
type Options struct {
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets bool

	// DiscoveryOnly publishes discovery drivers and sources immediately. Other
	// resources retain raw locations for render emission, orphan promotion or
	// depwait admission; publishing them early would bypass parent transforms.
	DiscoveryOnly bool
}

// Loader walks a directory tree and adds Flux objects to a Store.
type Loader struct {
	Store   *store.Store
	Options Options
	// DisableChartDigestTracking records inline instance patches before admission.
	// Load calls sharing a Loader MUST be sequential.
	DisableChartDigestTracking bool

	// SourceRoot, when non-empty, is the directory used as the
	// reference point for discovery file coordinates. Recorded paths are
	// slash-separated and relative to this root, which matches the
	// shape change.Detect produces.
	SourceRoot string

	// Index optionally records static discovery facts. Its maps MUST be initialized
	// before Load and are mutable only until FinalizeIndex returns.
	Index     *manifest.DiscoveryIndex
	documents documentReader

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

// New returns a Loader configured to wipe secrets.
func New(s *store.Store) *Loader {
	return &Loader{Store: s, Options: Options{WipeSecrets: true}}
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
	if l.Index != nil && l.documents.cache == nil {
		l.documents.cache = map[string]document{}
	}
	l.documents.mountSources(l.Store)
	abs, err := filepath.Abs(root)
	if err != nil {
		return 0, err
	}
	var ignore *ignoreSet
	if l.IgnoreFile != "" {
		input, name := l.documents.input(abs)
		info, statErr := fs.Stat(input, name)
		if errors.Is(statErr, fs.ErrNotExist) || statErr == nil && !info.IsDir() {
			return 0, nil
		}
		if statErr != nil {
			return 0, statErr
		}
		ignore, err = l.documents.loadIgnoreFile(l.IgnoreFile, abs)
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
	return w.descend(ctx, abs)
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
			// DiscoveryOnly callers still need the change filter's
			// producer index to see ConfigMap/Secret data files that
			// happen to live inside a Component subtree — a sibling
			// KS's substituteFrom dep can't resolve to its producing
			// KS otherwise. walkComponentData records those files
			// without publishing Component-housed Flux CRs (which are
			// frequently `${VAR}`-templated and not renderable
			// standalone).
			if w.loader.Options.DiscoveryOnly {
				return w.walkComponentData(ctx, dir, k)
			}
			return 0, nil
		}
		return w.walkKustomize(ctx, dir, k, kpath)
	}
	return w.walkAdHoc(ctx, dir)
}

// walkKustomize traverses the kustomize resource graph rooted at dir.
// File resources load via loadFile; directory resources recurse via
// descend. configMapGenerator / secretGenerator data files are
// excluded since they're consumed at render time, not loaded as
// Flux manifests.
//
// kustomization.yaml itself is loadFile'd so parseFile can decide
// whether it's a Flux Kustomization CR (different apiVersion than
// kustomize's own Kustomization). If not, parseFile returns no
// objects and the count is unchanged.
//
// patches / replacements / transformers / generators (the rest of
// kustomize's directive fields) reference YAMLs that are kustomize
// directives, NOT Flux manifests — they're not in `resources:` so
// they don't load. Matches `kustomize build` precisely.
func (w *walker) walkKustomize(ctx context.Context, dir string, k *kustomizationDocument, kpath string) (int, error) {
	dataFiles := dataFilesFor(dir, k)
	count := 0

	// Load the kustomization.yaml itself — preserves source-file
	// visibility for any consumer that inspects on-disk shape, and
	// lets parseFile recognize a Flux Kustomization that happens to
	// be authored at the `kustomization.yaml` filename (rare but
	// permitted). The cached document supplies kpath without another stat.
	if kpath != "" && !w.ignoreMatches(kpath, false) {
		n, err := w.loader.loadFile(kpath)
		if err != nil {
			slog.Warn("loader: kustomization file failed to parse", "path", kpath, "err", err)
		}
		count += n
	}

	for _, r := range k.Resources {
		if cerr := ctx.Err(); cerr != nil {
			return count, cerr
		}
		// Resolve with the escape-permitting resolver so directory
		// includes that point outside the package (overlays' ../base,
		// Flux repos' ../../../deploy/...) are followed — kustomize
		// follows them too. The file branch below re-imposes
		// resolveDataPath's stricter opened-path policy.
		abs, ok := resolvePath(dir, r)
		if !ok {
			// URLs, absolute paths, malformed entries — kustomize
			// handles these at render time; the loader's job is to
			// ignore them at discovery time.
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		if err != nil {
			// Resource pointer absent from the selected source view. Don't
			// surface here; kustomize.RenderFlux will produce a
			// clearer error with the right context at render time.
			continue
		}
		if info.IsDir() {
			n, err := w.descend(ctx, abs)
			if err != nil {
				return count, err
			}
			count += n
			continue
		}
		// File resource: re-impose the in-base constraint, since a
		// file gets opened (resolvePath only guards descent).
		if _, ok := resolveDataPath(dir, r); !ok {
			continue
		}
		if !isManifestFile(abs) {
			continue
		}
		if _, isData := dataFiles[abs]; isData {
			continue
		}
		if w.ignoreMatches(abs, false) {
			continue
		}
		n, err := w.loader.loadFile(abs)
		if err != nil {
			return count, fmt.Errorf("loader: %w", err)
		}
		count += n
	}

	// Walk `components:` entries too. A kustomize Component contributes
	// resources (and patches) to its parent's render, and any Flux CRs
	// inside a component's subtree must be discoverable by flate.
	// parent.go already reads `components:` for ownership-prefix
	// claiming; without walking them here, the loader's discovery and
	// parent.go's claim graph disagree.
	for _, comp := range k.Components {
		if cerr := ctx.Err(); cerr != nil {
			return count, cerr
		}
		abs, ok := resolvePath(dir, comp)
		if !ok {
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		if err != nil || !info.IsDir() {
			continue
		}
		n, err := w.descend(ctx, abs)
		if err != nil {
			return count, err
		}
		count += n
	}

	if w.loader.Options.DiscoveryOnly {
		count += w.scanBootstrapFluxKS(dir, k, kpath)
	}
	return count, nil
}

// scanBootstrapFluxKS surfaces Flux Kustomization CRs authored as
// sibling YAML files of dir's kustomization.yaml but not listed in its
// `resources:`. Real Flux pattern: the entry KS is kubectl-applied
// outside any kustomize tree while its spec.path points at the
// kustomize package beside it; without this scan the KS — and the
// change-attribution it provides for files under spec.path — stay
// invisible. Scope is intentionally narrow to *Flux Kustomization*
// kinds; broadening here would silently undo the "kustomize package
// = resources only" rule the loader otherwise enforces for HRs /
// sources / data files. Gated on DiscoveryOnly so non-DiscoveryOnly
// callers keep that strict semantic.
func (w *walker) scanBootstrapFluxKS(dir string, k *kustomizationDocument, kpath string) int {
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
	for _, r := range k.Resources {
		if abs, ok := resolveDataPath(dir, r); ok {
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
		objs, err := w.loader.documents.parseFile(abs, manifest.ParseDocOptions{WipeSecrets: w.loader.Options.WipeSecrets})
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
			if ks.Path == "" && ks.SourceKind == "" && ks.SourceName == "" {
				continue
			}
			if w.loader.skipExisting(obj.Named()) {
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
// No-ops (returns false) when SourceRoot is unset, so SDK / unit-test callers
// that set DiscoveryOnly without a repo root keep the legacy scan behavior.
//
// A KS being surfaced by scanBootstrapFluxKS is not yet in the Store at its
// own dir's first scan, so a true entry KS reads as uncovered and is still
// surfaced; only a strictly-enclosing (or equal) prefix from an already-loaded
// KS marks the dir covered.
func (l *Loader) dirCoveredByOtherKS(dir string) bool {
	if l.SourceRoot == "" {
		return false
	}
	rel, err := filepath.Rel(l.SourceRoot, dir)
	if err != nil {
		return false
	}
	dirRel := filepath.ToSlash(rel) + "/"
	for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		root := l.SourceRoot
		if strings.Contains(ks.Path, "..") {
			root, _, _ = SourceView(l.Store, ks, root)
		}
		base := l.documents.claimBase(root, ks.Path)
		if strings.HasPrefix(dirRel, base+"/") {
			return true
		}
	}
	return false
}

// walkComponentData records direct ConfigMap/Secret resources from
// kustomize Components during DiscoveryOnly loads without publishing
// templated Flux CRs as standalone objects. The change filter's
// producer index needs the data files source-recorded so a downstream
// substituteFrom consumer can resolve its producing KS, but a
// Component's own `${VAR}`-templated Flux CRs are NOT renderable
// standalone — publishing them would corrupt discovery.
//
// Component-of-Component graphs recurse via k.Components; w.visited
// terminates Component cycles (same primitive descend uses for the
// non-Component graph). We deliberately do NOT call descend() for
// nested Components — that would re-enter the regular walk and load
// any non-Component YAMLs from the Component subtree, defeating the
// whole point of the skip. Stay limited to CM/Secret data-file
// recording.
func (w *walker) walkComponentData(ctx context.Context, dir string, k *kustomizationDocument) (int, error) {
	for _, r := range k.Resources {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		abs, ok := resolveDataPath(dir, r)
		if !ok || w.ignoreMatches(abs, false) || !isManifestFile(abs) {
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		if err != nil || info.IsDir() {
			// Directories under a Component's `resources:` are sibling
			// kustomize packages, not bare data files; the producer
			// index is fed by files, so skip rather than recurse. A
			// nested kustomize package authored inside a Component is
			// vanishingly rare and would be loaded via its enclosing
			// Flux KS spec.path instead.
			continue
		}
		if err := w.loader.recordDataFile(abs); err != nil {
			slog.Warn("loader: component data file failed to parse", "path", abs, "err", err)
		}
	}
	for _, comp := range k.Components {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		abs, ok := resolvePath(dir, comp)
		if !ok {
			continue
		}
		input, name := w.loader.documents.input(abs)
		info, err := fs.Stat(input, name)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, seen := w.visited[abs]; seen {
			continue
		}
		w.visited[abs] = struct{}{}
		nested, _ := w.loader.documents.read(abs)
		if nested == nil || !isKustomizeComponent(nested) {
			// A Component that points at a non-Component dir is
			// malformed for kustomize purposes; skip rather than
			// descend into walkKustomize, which would publish Flux
			// CRs we deliberately keep hidden under Component subtrees.
			continue
		}
		if _, err := w.walkComponentData(ctx, abs, nested); err != nil {
			return 0, err
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
					// Mirror descend's DiscoveryOnly gate: harvest
					// CM/Secret data files from Component subtrees so
					// the producer index can resolve substituteFrom
					// deps even when the entry was the ad-hoc walk.
					if w.loader.Options.DiscoveryOnly {
						if _, err := w.walkComponentData(ctx, path, k); err != nil {
							return err
						}
					}
					return filepath.SkipDir
				}
				w.visited[path] = struct{}{}
				n, err := w.walkKustomize(ctx, path, k, kpath)
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

// skipExisting reports whether id should be left untouched because
// PreferExisting is set and the Store already holds it — the
// orchestrator's recursive spec.path discovery wants the initial scan's
// data to win over downstream paths that may point into a different tree.
func (l *Loader) skipExisting(id manifest.NamedResource) bool {
	return l.PreferExisting && l.Store.GetObject(id) != nil
}

// Indexed discovery defers decode errors; Load without an index remains strict.
func (l *Loader) loadFile(path string) (int, error) {
	objs := []manifest.BaseManifest{}
	err := l.documents.decodeFileObjects(path, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets}, l.documents.cache != nil, func(obj manifest.BaseManifest) {
		if raw, ok := obj.(*manifest.RawObject); ok && len(manifest.ProducerTargets(raw)) == 0 {
			return
		}
		objs = append(objs, obj)
	})
	if err != nil {
		if l.Index == nil || !errors.Is(err, manifest.ErrInput) {
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
		if _, raw := obj.(*manifest.RawObject); raw {
			continue
		}
		if hr, ok := obj.(*manifest.HelmRelease); ok {
			l.DisableChartDigestTracking = l.DisableChartDigestTracking || disablesChartDigestTracking(hr)
		}
		id := obj.Named()
		if l.skipExisting(id) {
			continue
		}
		if l.Options.DiscoveryOnly && !isDiscoveryKind(obj) {
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
	if l.documents.cache != nil {
		objects := slices.DeleteFunc(objs, func(obj manifest.BaseManifest) bool {
			switch obj.(type) {
			case *manifest.ConfigMap, *manifest.Secret, *manifest.Kustomization, *manifest.RawObject:
				return false
			default:
				return true
			}
		})
		l.documents.cache[path] = document{objects: objects}
	}
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
	objs, err := l.documents.parseFile(absPath, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
	if err != nil {
		return err
	}
	for _, obj := range objs {
		id := obj.Named()
		if id.Kind != manifest.KindConfigMap && id.Kind != manifest.KindSecret {
			// Non-data kinds (most commonly templated Flux CRs in a
			// Component subtree) intentionally stay invisible — the
			// producer-index use case is data-only.
			continue
		}
		if l.skipExisting(id) {
			continue
		}
		l.recordObject(obj, absPath)
	}
	return nil
}

// recordObject retains raw locations and authored source references for deferred admission.
func (l *Loader) recordObject(obj manifest.BaseManifest, absPath string) {
	if l.Index == nil {
		return
	}
	id := obj.Named()
	r := manifest.DiscoveryRecord{Path: absPath, File: l.fileCoordinate(absPath)}
	switch o := obj.(type) {
	case *manifest.HelmRelease:
		if o.Chart.RepoKind != "" && o.Chart.RepoName != "" {
			r.Refs = make([]manifest.NamedResource, 1, 1+len(o.ValuesFrom))
			r.Refs[0] = manifest.NamedResource{Kind: o.Chart.RepoKind, Namespace: o.Chart.RepoNamespace, Name: o.Chart.RepoName}
		}
		for _, ref := range o.ValuesFrom {
			r.Refs = append(r.Refs, manifest.NamedResource{Kind: ref.Kind, Namespace: o.Namespace, Name: ref.Name})
		}
	case *manifest.Kustomization:
		if o.SourceKind != "" && o.SourceName != "" {
			count := 1
			for _, ref := range o.PostBuildSubstituteFrom {
				if manifest.IsHardConfigMapEdge(ref) {
					count++
				}
			}
			r.Refs = make([]manifest.NamedResource, 1, count)
			r.Refs[0] = manifest.NamedResource{Kind: o.SourceKind, Namespace: o.SourceNamespace, Name: o.SourceName}
		}
		for _, ref := range o.PostBuildSubstituteFrom {
			if manifest.IsHardConfigMapEdge(ref) {
				r.Refs = append(r.Refs, manifest.NamedResource{Kind: ref.Kind, Namespace: o.Namespace, Name: ref.Name})
			}
		}
	}
	l.Index.Records[id] = r
}

func (l *Loader) fileCoordinate(absPath string) string {
	if l.SourceRoot != "" {
		if rel, err := filepath.Rel(l.SourceRoot, absPath); err == nil {
			return filepath.ToSlash(rel)
		}
	}
	return filepath.ToSlash(absPath)
}

// isDiscoveryKind reports whether obj belongs to the discovery-meta
// kind set the loader keeps in the Store under DiscoveryOnly:
//
//   - Kustomization, ResourceSet, ResourceSetInputProvider — the
//     reconcile driver and its meta-discovery hooks (RS renders to
//     more KSes; RSIPs feed RS selectors).
//   - Sources (GitRepository, OCIRepository, HelmRepository,
//     HelmChartSource, Bucket, ExternalArtifact) — sources are
//     rarely patched at render time and need to be visible at
//     discovery for the bootstrap-alias pass to recognize them as
//     already-known (otherwise every GitRepository gets aliased to
//     the working tree, which silently redirects every KS's render
//     to the wrong source root).
//
// HelmReleases, ConfigMaps, Secrets, and other reconcilables flow
// from KS render output via emitRenderedChildren — or, when no KS
// would render them, through the orchestrator's post-discovery
// orphan-promotion sweep.
func isDiscoveryKind(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization,
		*manifest.ResourceSet,
		*manifest.ResourceSetInputProvider,
		*manifest.GitRepository,
		*manifest.OCIRepository,
		*manifest.HelmRepository,
		*manifest.HelmChartSource,
		*manifest.Bucket,
		*manifest.ExternalArtifact:
		return true
	}
	return false
}

func isManifestFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

// shouldSkipDir applies the ad-hoc walk's directory-prune rules.
// Not used by walkKustomize — that path follows explicit resource
// entries and trusts the user's kustomize manifests.
//
// Lives on walker so the final ignore check routes through the
// per-Load matches cache — without it, every directory in the tree
// re-runs filepath.Rel + gitignore.Match for the same path.
func (w *walker) shouldSkipDir(name, full string) bool {
	switch name {
	case "node_modules":
		return true
	case "templates", "crds":
		// These directories typically contain Helm template fragments
		// with Go-template syntax that isn't valid YAML.
		return true
	}
	if strings.HasPrefix(name, ".") && name != "." {
		return true
	}
	// With `!` re-includes in play, an ignored directory can still hold
	// re-included files, so descend and let the per-file checks filter —
	// matching sourceignore's per-file evaluation of spec.ignore.
	if w.ignore != nil && w.ignore.hasNegation {
		return false
	}
	return w.ignoreMatches(full, true)
}
