package loader

import (
	"context"
	"errors"
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

// Load walks root recursively, decoding every .yaml/.yml/.json document
// and adding recognized Flux objects to the Store. Returns the count of
// added objects.
//
// Honors ctx cancellation between directory entries — a stuck NFS
// mount or symlink loop aborts cleanly instead of blocking the whole
// orchestrator.
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

	// Pre-pass: decode every kustomization.yaml in the tree and
	// collect (a) data files declared as generator inputs, (b) files
	// declared as `resources:`, and (c) the directories that have a
	// kustomization.yaml at all. The main walk applies three rules
	// from this scan:
	//
	//   - skip generator data files (issue #192)
	//   - skip "orphan" YAML files: a YAML in a directory governed by
	//     a kustomization.yaml is loaded only when explicitly
	//     referenced via `resources:`. Closes #342 — toggle stubs
	//     left lying around next to a kustomization.yaml (a common
	//     pattern: comment a `./vrising.yaml` line in resources to
	//     disable that wrapper) were being discovered and reconciled
	//     against the wrong namespace, since the parent overlay's
	//     transforms never applied. kustomize build follows the
	//     same graph; flate now matches that behavior.
	scan, err := collectKustomizationScan(ctx, abs, ignore)
	if err != nil {
		return 0, err
	}

	count := 0
	err = filepath.WalkDir(abs, func(path string, d fs.DirEntry, walkErr error) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name(), path, abs, ignore) {
				return filepath.SkipDir
			}
			return nil
		}
		if !isManifestFile(path) {
			return nil
		}
		if ignore.matches(path, abs) {
			return nil
		}
		if _, isData := scan.DataFiles[path]; isData {
			// Declared as configMapGenerator/secretGenerator data by
			// a kustomization.yaml in the tree. krusty handles the
			// file correctly at render time; the loader's job is to
			// stay out of the way.
			slog.Debug("loader: skipping generator data file", "path", path)
			return nil
		}
		if orphan := isOrphanYAML(path, scan); orphan {
			// Orphan: this YAML lives in a directory governed by a
			// kustomization.yaml but isn't referenced via the
			// `resources:` list. kustomize build wouldn't include it;
			// flate now matches that. Common shape: a disabled toggle
			// stub like `./vrising.yaml` left in the directory after
			// a maintainer commented out the line in `resources:`.
			slog.Debug("loader: skipping orphan YAML not referenced in parent kustomization.yaml", "path", path)
			return nil
		}
		n, err := l.loadFile(path)
		if err != nil {
			// `templates/`, `crds/`, and ignore-matched paths never
			// reach here — they're SkipDir'd in shouldSkipDir. A YAML
			// syntax error at a path the loader DID try to parse is a
			// real user-side problem (typo'd manifest, half-edited
			// CRD); promote to WARN so it isn't invisible at default
			// log level. The per-doc kind-mismatch case below stays at
			// Debug because raw k8s manifests interspersed with Flux
			// CRs are a legitimate pattern.
			slog.Warn("loader: file failed to parse", "path", path, "err", err)
			return nil
		}
		count += n
		return nil
	})
	if err != nil {
		return count, err
	}
	return count, nil
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
	// A `kind: Component` kustomization.yaml means everything below is a
	// template fragment that real Flux only materializes via a parent
	// Kustomization's spec.components reference. Standalone-loading the
	// children would surface literal `${APP}` placeholders in metadata
	// names as bogus Kustomization / HelmRelease objects. The parent's
	// kustomize render still picks them up — it follows spec.components
	// directly without going through flate's standalone loader.
	return isKustomizeComponent(full)
}

// isKustomizeComponent reports whether dir contains a kustomization
// file declaring `kind: Component`. Catches YAML, JSON, and terse
// no-space-after-colon shapes that a substring check would miss.
func isKustomizeComponent(dir string) bool {
	k := readKustomization(dir)
	return k != nil && k.Kind == "Component"
}

// isOrphanYAML reports whether path should be skipped by the main
// loader walk because it's an unreferenced YAML in a directory
// governed by a kustomization.yaml. The rule:
//
//   - If the path's directory does NOT have a kustomization.yaml,
//     the file is loaded normally (e.g. --path entry points, the
//     bootstrap dir before the first overlay).
//   - The kustomization.yaml file itself always loads.
//   - Files explicitly listed in the kustomization.yaml's
//     `resources:` always load.
//   - Everything else in the same directory is "orphan" — toggle
//     stubs, kustomize patches, `replacements:` payloads — and is
//     skipped. Patches/replacements would parse as RawObject and
//     get filtered downstream anyway; toggle stubs (the #342 case)
//     would parse as a Flux Kustomization and reconcile against
//     the wrong overlay state.
//
// Mirrors kustomize build's graph traversal: only the transitive
// closure of `resources:` (+ generators) ends up rendered.
func isOrphanYAML(path string, scan *kustomizationScan) bool {
	dir := filepath.Dir(path)
	if _, ok := scan.KustomizationDirs[dir]; !ok {
		return false
	}
	if slices.Contains(kustomizationFileNames, filepath.Base(path)) {
		return false
	}
	_, claimed := scan.ClaimedResources[path]
	return !claimed
}
