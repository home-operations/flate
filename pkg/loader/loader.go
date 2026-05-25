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
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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
		loader:  l,
		ignore:  ignore,
		visited: map[string]struct{}{},
	}
	return w.descend(ctx, abs)
}

// walker carries the per-Load state — ignore matcher, visited-dir
// dedup, loader back-reference — so the recursive functions don't
// need to thread the same args through every call.
type walker struct {
	loader  *Loader
	ignore  *ignoreSet
	visited map[string]struct{}
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

	k := readKustomization(dir)
	if k != nil {
		if k.Kind == "Component" {
			slog.Debug("loader: skipping kustomize Component directory", "dir", dir)
			return 0, nil
		}
		return w.walkKustomize(ctx, dir, k)
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
func (w *walker) walkKustomize(ctx context.Context, dir string, k *kustomization) (int, error) {
	dataFiles := dataFilesFor(dir, k)
	count := 0

	// Load the kustomization.yaml itself — preserves source-file
	// visibility for any consumer that inspects on-disk shape, and
	// lets parseFile recognize a Flux Kustomization that happens to
	// be authored at the `kustomization.yaml` filename (rare but
	// permitted).
	if kpath, ok := kustomizationFilePath(dir); ok && !w.ignore.matches(kpath, dir) {
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
		abs, ok := resolveDataPath(dir, r)
		if !ok {
			// URL resources, paths escaping the package, malformed
			// entries — kustomize handles these at render time;
			// the loader's job is to ignore them at discovery time.
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
		if !isManifestFile(abs) {
			continue
		}
		if _, isData := dataFiles[abs]; isData {
			continue
		}
		if w.ignore.matches(abs, dir) {
			continue
		}
		n, err := w.loader.loadFile(abs)
		if err != nil {
			slog.Warn("loader: file failed to parse", "path", abs, "err", err)
			continue
		}
		count += n
	}
	return count, nil
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
			if shouldSkipDir(d.Name(), path, root, w.ignore) {
				return filepath.SkipDir
			}
			if path == root {
				return nil
			}
			// Subdirectory: if it's a kustomize package, switch to
			// graph walk and SkipDir to keep filepath.WalkDir from
			// descending again.
			if k := readKustomization(path); k != nil {
				if k.Kind == "Component" {
					return filepath.SkipDir
				}
				w.visited[path] = struct{}{}
				n, err := w.walkKustomize(ctx, path, k)
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
		if w.ignore.matches(path, root) {
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
			continue
		}
		l.Store.AddObject(obj)
		l.recordSource(id, path)
		count++
	}
	return count, nil
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

var manifestExtensions = map[string]struct{}{
	".yaml": {},
	".yml":  {},
	".json": {},
}

func isManifestFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	_, ok := manifestExtensions[ext]
	return ok
}

// shouldSkipDir applies the ad-hoc walk's directory-prune rules.
// Not used by walkKustomize — that path follows explicit resource
// entries and trusts the user's kustomize manifests.
func shouldSkipDir(name, full, root string, ignore *ignoreSet) bool {
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
	if ignore.matchesDir(full, root) {
		return true
	}
	return false
}

// kustomizationFilePath returns the absolute path of dir's
// kustomization.{yaml,yml,json} (first match wins, matching kustomize's
// own filename precedence). Returns ("", false) when none exists.
func kustomizationFilePath(dir string) (string, bool) {
	for _, name := range kustomizationFileNames {
		p := filepath.Join(dir, name)
		if info, err := os.Stat(p); err == nil && info.Mode().IsRegular() {
			return p, true
		}
	}
	return "", false
}
