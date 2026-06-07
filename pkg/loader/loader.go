// Package loader hydrates a Store from on-disk Flux manifests.
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
	"cmp"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Options tunes the Loader.
type Options struct {
	// WipeSecrets controls Secret cleartext replacement. Default true.
	WipeSecrets bool

	// DiscoveryOnly restricts file-loaded kinds that reach the Store
	// to the discovery-meta set: Kustomization, ResourceSet, and
	// ResourceSetInputProvider. Every other Flux CR (HelmRelease,
	// sources, ConfigMap, Secret) is recorded in Existence instead of
	// AddObject'd, matching real Flux's render-driven discovery
	// model where only the bootstrap KS is static and the rest of
	// the cluster materializes through KS reconciles. depwait
	// consults the existence index on missing deps; orchestrator
	// orphan-promotes any index entry not under a KS's spec.path
	// before reconcile starts.
	//
	// Why RS + RSIP stay in-scope: the discovery loop renders
	// ResourceSets to discover further KSes (RSIPs feed selectors,
	// RSes produce KSes/RSIPs). There is no ResourceSet controller
	// yet, so render-emitted RSes would never be processed; keeping
	// them file-loaded preserves the meta-discovery fixed point.
	DiscoveryOnly bool
}

// Loader walks a directory tree and adds Flux objects to a Store.
type Loader struct {
	Store   *store.Store
	Options Options

	// SourceRoot, when non-empty, is the directory used as the
	// reference point for SourceFiles. Paths recorded there are
	// slash-separated and relative to this root, which matches the
	// shape change.Detect produces.
	SourceRoot string

	// SourceFiles is populated as each manifest is added. Keyed by
	// the parsed resource's NamedResource. Nil disables tracking.
	SourceFiles map[manifest.NamedResource]string

	// SourceRefs maps each loaded consumer (HelmRelease / Kustomization)
	// to the source resources it references — an HR's chart source, a
	// KS's spec.sourceRef. Captured at parse time because under
	// DiscoveryOnly an HR never reaches the Store, yet the change
	// filter's reverse edge needs to know which HelmReleases a changed
	// source feeds (so bumping a centralized OCIRepository's tag
	// re-renders its consumers). Nil disables tracking. Keyed by the
	// consumer's NamedResource; mirrors SourceFiles' lifecycle.
	SourceRefs map[manifest.NamedResource][]manifest.NamedResource

	// PreferExisting suppresses overwrites of resources already in
	// the store (and their SourceFiles entries). Used by the
	// orchestrator's recursive spec.path discovery so the initial
	// --path scan's data wins over downstream paths that may point
	// into a different tree.
	PreferExisting bool

	// Existence captures every file-loaded object that DiscoveryOnly
	// keeps out of the Store. Nil disables the bookkeeping (the
	// default for non-DiscoveryOnly callers).
	Existence *ExistenceIndex

	// ComponentCache, when non-nil, is the shared component-file
	// cache used by FinalizeGenerators' KSPathPrefixes call. Wired
	// by the orchestrator at Bootstrap so the loader, discovery's
	// parent-index passes, the orphan-promotion pass, the orchestrator's
	// finalize detectOrphans, and change.buildOwnership all read each
	// kustomization.yaml's `components:` field at most once per
	// Bootstrap. nil falls back to per-call caches with no cross-call
	// sharing.
	ComponentCache *manifest.ComponentCache

	// generators accumulates configMapGenerator/secretGenerator
	// entries observed during the walk. After Load returns, the
	// orchestrator finalizes them via FinalizeGenerators — the
	// effective namespace depends on which Flux Kustomization
	// encloses each kustomization.yaml, and that's only known once
	// the full walk completes.
	generatorsMu sync.Mutex
	generators   []generatorRecord
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
	abs, err := filepath.Abs(root)
	if err != nil {
		return 0, err
	}
	ignore, err := loadIgnore(abs)
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

	// readKustomizationAt returns the resolved file path alongside the
	// parsed body so descend can hand both forward to walkKustomize /
	// walkComponentData without re-opening the same kustomization.yaml.
	k, kpath := readKustomizationAt(dir)
	if k != nil {
		// Harvest configMapGenerator/secretGenerator entries
		// regardless of whether this is a Kustomization or Component
		// — both can declare generators, and downstream depwait
		// lookups for substituteFrom CMs need the synthesized
		// records either way.
		if kpath != "" {
			w.loader.recordGenerators(collectGeneratorRecords(k, kpath))
		}
		if k.isKustomizeComponent() {
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
func (w *walker) walkKustomize(ctx context.Context, dir string, k *kustomization, kpath string) (int, error) {
	dataFiles := dataFilesFor(dir, k)
	count := 0

	// Load the kustomization.yaml itself — preserves source-file
	// visibility for any consumer that inspects on-disk shape, and
	// lets parseFile recognize a Flux Kustomization that happens to
	// be authored at the `kustomization.yaml` filename (rare but
	// permitted). kpath comes from the descend-side readKustomizationAt
	// (or the walkAdHoc fallback) so we don't re-stat the same file
	// the caller already opened.
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
		abs, ok := resolveResourcePath(dir, r)
		if !ok {
			// URLs, absolute paths, malformed entries — kustomize
			// handles these at render time; the loader's job is to
			// ignore them at discovery time.
			continue
		}
		info, err := os.Stat(abs)
		if err != nil {
			// Resource pointer that doesn't exist on disk. Don't
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
		// file gets opened (resolveResourcePath only guards descent).
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
			slog.Warn("loader: file failed to parse", "path", abs, "err", err)
			continue
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
		abs, ok := resolveComponentPath(dir, comp)
		if !ok {
			continue
		}
		info, err := os.Stat(abs)
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
func (w *walker) scanBootstrapFluxKS(dir string, k *kustomization, kpath string) int {
	// A genuine bootstrap *entry* KS is a root — no other Kustomization's
	// spec.path covers its directory. A KS commented-out of (excluded from)
	// a kustomization.yaml lives inside a tree another KS already renders
	// (e.g. cluster-apps' spec.path covers apps/<cluster>/<ns>/); real Flux
	// renders that dir solely from its kustomization.yaml `resources:`, so a
	// sibling KS left out of them is a disabled app, not a bootstrap entry.
	// Surfacing it here is a false positive. Skip the scan when dir is already
	// claimed by another loaded KS's spec.path — the same coverage predicate
	// promoteOrphans applies via LongestParent.
	if w.loader.dirCoveredByOtherKS(dir) {
		return 0
	}
	seen := map[string]struct{}{kpath: {}}
	for _, r := range k.Resources {
		if abs, ok := resolveDataPath(dir, r); ok {
			seen[abs] = struct{}{}
		}
	}
	entries, err := os.ReadDir(dir)
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
		objs, err := parseFile(abs, manifest.ParseDocOptions{WipeSecrets: w.loader.Options.WipeSecrets})
		if err != nil {
			slog.Warn("loader: bootstrap-sibling parse failed", "path", abs, "err", err)
			continue
		}
		for _, obj := range objs {
			ks, ok := obj.(*manifest.Kustomization)
			if !ok {
				continue
			}
			// A genuine bootstrap entry KS always carries a spec.path (and a
			// sourceRef). A kind: Kustomization with neither is a kustomize
			// patch fragment — a patch.yaml referenced via `patches:` whose
			// body happens to use the Flux Kustomization GVK as the patch
			// target — never a reconcilable Flux Kustomization. Skipping it
			// is order-independent (it reads only the parsed object), unlike
			// the dir-coverage guard above.
			if ks.Path == "" && ks.SourceKind == "" && ks.SourceName == "" {
				continue
			}
			id := obj.Named()
			if w.loader.PreferExisting && w.loader.Store.GetObject(id) != nil {
				continue
			}
			w.loader.Store.AddObject(obj)
			w.loader.recordSource(id, abs)
			w.loader.recordSourceRefs(obj)
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
	for _, ks := range store.ListAs[*manifest.Kustomization](l.Store, manifest.KindKustomization) {
		if ks.Path == "" {
			continue
		}
		if strings.HasPrefix(dirRel, NormalizePrefix(ks.Path)) {
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
func (w *walker) walkComponentData(ctx context.Context, dir string, k *kustomization) (int, error) {
	for _, r := range k.Resources {
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		abs, ok := resolveDataPath(dir, r)
		if !ok || w.ignoreMatches(abs, false) || !isManifestFile(abs) {
			continue
		}
		info, err := os.Stat(abs)
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
		abs, ok := resolveComponentPath(dir, comp)
		if !ok {
			continue
		}
		info, err := os.Stat(abs)
		if err != nil || !info.IsDir() {
			continue
		}
		if _, seen := w.visited[abs]; seen {
			continue
		}
		w.visited[abs] = struct{}{}
		nested, _ := readKustomizationAt(abs)
		if nested == nil || !nested.isKustomizeComponent() {
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
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
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
			if k, kpath := readKustomizationAt(path); k != nil {
				if k.isKustomizeComponent() {
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
			slog.Warn("loader: file failed to parse", "path", path, "err", err)
			return nil
		}
		count += n
		return nil
	})
	return count, err
}

func (l *Loader) loadFile(path string) (int, error) {
	objs, err := parseFile(path, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
	if err != nil {
		return 0, err
	}
	count := 0
	for _, obj := range objs {
		id := obj.Named()
		if l.PreferExisting && l.Store.GetObject(id) != nil {
			continue
		}
		if l.Options.DiscoveryOnly && !isDiscoveryKind(obj) {
			// Under render-driven discovery, HRs / ConfigMaps /
			// Secrets / raw manifests stay out of the Store at file-
			// load time. They reach the Store via KS render
			// emission (emitRenderedChildren), depwait's lazy-
			// promotion fallback, or the orchestrator's orphan
			// sweep. The Existence index records the {id, path}
			// pair so the depwait fallback can re-parse the file
			// on demand without deadlocking the producing KS.
			l.Existence.Record(id, path)
			l.recordSource(id, path)
			l.recordSourceRefs(obj)
			continue
		}
		l.Store.AddObject(obj)
		l.recordSource(id, path)
		l.recordSourceRefs(obj)
		count++
	}
	return count, nil
}

// recordDataFile parses absPath and records any ConfigMap/Secret
// objects into the Existence index and SourceFiles map without
// adding them to the Store. Used by walkComponentData so the change
// filter's producer index can resolve substituteFrom data deps to
// their renderer KS without publishing untemplated Component-subtree
// resources. Mirrors loadFile's PreferExisting + Existence + source
// bookkeeping but skips AddObject and skips non-data kinds.
func (l *Loader) recordDataFile(absPath string) error {
	objs, err := parseFile(absPath, manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets})
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
		if l.PreferExisting && l.Store.GetObject(id) != nil {
			continue
		}
		if l.Existence != nil {
			l.Existence.Record(id, absPath)
		}
		l.recordSource(id, absPath)
	}
	return nil
}

// recordGenerators appends generator records observed during a walk.
// The orchestrator's Bootstrap calls FinalizeGenerators after Load
// returns to resolve effective namespaces and inject the synthesized
// CMs/Secrets into the store.
func (l *Loader) recordGenerators(records []generatorRecord) {
	if len(records) == 0 {
		return
	}
	l.generatorsMu.Lock()
	defer l.generatorsMu.Unlock()
	l.generators = append(l.generators, records...)
}

// FinalizeGenerators materializes every harvested configMapGenerator/
// secretGenerator entry into the store. Effective namespace is
// resolved per kustomize precedence: the entry's own namespace wins,
// then the kustomization.yaml's namespace, then the enclosing Flux
// Kustomization's namespace (looked up via KSPathPrefixes against
// the source file).
//
// Synthesized objects honor PreferExisting: if an explicit CM/Secret
// with the same id already came from a real on-disk YAML, we don't
// overwrite it with the generated placeholder.
//
// repoRoot is the filesystem root used to compute slash-relative
// paths for the parent lookup; pass the same root the change filter
// is keyed against (the Flux KS spec.path prefixes are
// repoRoot-relative).
func (l *Loader) FinalizeGenerators(repoRoot string) {
	l.generatorsMu.Lock()
	records := l.generators
	l.generators = nil
	l.generatorsMu.Unlock()
	if len(records) == 0 {
		return
	}
	// Build the prefix list once for the entire generator loop.
	// parentNamespaceFor needs it for each record's LongestParent
	// lookup; building it per record would walk every Kustomization
	// and re-read every kustomization.yaml's `components:` once per
	// record — O(M×K) where M is records and K is KSes. The shared
	// ComponentCache (when present) drops the per-KS cost too, so a
	// repeat finalize pass within the same Bootstrap doesn't re-stat
	// disk.
	prefixes := KSPathPrefixesWithCache(l.Store, repoRoot, l.ComponentCache)
	seen := make(map[manifest.NamedResource]struct{}, len(records))
	for _, r := range records {
		parentNS := parentNamespaceFor(prefixes, l.Store, r.file, repoRoot)
		obj := r.materialize(parentNS)
		id := obj.Named()
		if _, dup := seen[id]; dup {
			// Iterative discovery re-walks the same kustomization.yaml
			// across passes; the second occurrence is a duplicate.
			continue
		}
		seen[id] = struct{}{}
		if l.Store.GetObject(id) != nil {
			// Real CM/Secret YAML already won; don't overwrite.
			continue
		}
		l.Store.AddObject(obj)
		l.recordSource(id, r.file)
		if l.Existence != nil {
			l.Existence.Record(id, r.file)
		}
	}
}

// parentNamespaceFor resolves the enclosing Flux Kustomization's
// namespace for the file at absPath. Falls back to "" when the file
// sits outside any KS spec.path / component — depwait's name-only
// fallback may still match in that case.
func parentNamespaceFor(prefixes []KSPathPrefix, s *store.Store, absPath, repoRoot string) string {
	rel, err := filepath.Rel(repoRoot, absPath)
	if err != nil {
		return ""
	}
	owner, ok := LongestParent(prefixes, rel, manifest.NamedResource{})
	if !ok {
		return ""
	}
	ks, _ := store.GetByName[*manifest.Kustomization](s, manifest.KindKustomization, owner.Namespace, owner.Name)
	if ks == nil {
		return ""
	}
	// Flux's render-time namespace precedence: spec.targetNamespace
	// (the resource's effective namespace post-render) wins over the
	// KS's own namespace (where the KS CR lives). For generated CMs
	// we want the post-render namespace because that's what
	// substituteFrom in downstream KSes references.
	return cmp.Or(ks.TargetNamespace, ks.Namespace)
}

// recordSource maps a resource id back to the on-disk file it was
// loaded from, with the path made relative to SourceRoot and
// slash-normalized to match change.Detect's keys.
func (l *Loader) recordSource(id manifest.NamedResource, absPath string) {
	if l.SourceFiles == nil {
		return
	}
	rel := absPath
	if l.SourceRoot != "" {
		if r, err := filepath.Rel(l.SourceRoot, absPath); err == nil {
			rel = r
		}
	}
	l.SourceFiles[id] = filepath.ToSlash(rel)
}

// recordSourceRefs captures the source resources obj references — a
// HelmRelease's chart source, a Kustomization's spec.sourceRef — so the
// change filter can resolve the reverse edge from a changed source back
// to its consumers. No-op for non-consumer kinds or when tracking is
// disabled. The chart ref is read from the parse-time projection
// (chartFromHelmRelease), which is populated whether or not the HR
// reaches the Store.
func (l *Loader) recordSourceRefs(obj manifest.BaseManifest) {
	if l.SourceRefs == nil {
		return
	}
	var refs []manifest.NamedResource
	switch o := obj.(type) {
	case *manifest.HelmRelease:
		if o.Chart.RepoKind != "" && o.Chart.RepoName != "" {
			refs = append(refs, manifest.NamedResource{
				Kind: o.Chart.RepoKind, Namespace: o.Chart.RepoNamespace, Name: o.Chart.RepoName,
			})
		}
	case *manifest.Kustomization:
		if o.SourceKind != "" && o.SourceName != "" {
			refs = append(refs, manifest.NamedResource{
				Kind: o.SourceKind, Namespace: o.SourceNamespace, Name: o.SourceName,
			})
		}
	}
	if len(refs) > 0 {
		l.SourceRefs[obj.Named()] = refs
	}
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
	case ".git", "node_modules", ".cache":
		return true
	case "templates", "crds":
		// These directories typically contain Helm template fragments
		// with Go-template syntax that isn't valid YAML.
		return true
	}
	if strings.HasPrefix(name, ".") && name != "." {
		return true
	}
	return w.ignoreMatches(full, true)
}
