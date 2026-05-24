package loader

import (
	"context"
	"errors"
	"fmt"
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
	// collect the set of files referenced as configMapGenerator /
	// secretGenerator data sources. The main walk skips those — they
	// are valid YAML data files, not Flux manifests, and would
	// otherwise trip the generic decode-as-map fallback with noisy
	// WARN logs that look like real failures (issue #192).
	dataFiles, err := collectGeneratorDataFiles(ctx, abs, ignore)
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
		if _, isData := dataFiles[path]; isData {
			// Declared as configMapGenerator/secretGenerator data by
			// a kustomization.yaml in the tree. krusty handles the
			// file correctly at render time; the loader's job is to
			// stay out of the way.
			slog.Debug("loader: skipping generator data file", "path", path)
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
	f, err := os.Open(path) //nolint:gosec // path is a tree-walk result under the cluster scan root
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f)
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", path, err)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: l.Options.WipeSecrets}
	count := 0
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			// Don't break on a single bad doc; flux-local skips and logs.
			slog.Debug("loader: doc skipped", "path", path, "err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			// We only persist objects we explicitly understand.
			continue
		}
		id := obj.Named()
		// Skip resources whose name or namespace contains a literal
		// `${VAR}` reference — those are templates the user expects a
		// parent Kustomization's postBuild.substitute(From) to resolve
		// at render time. Real Flux never sees them as in-cluster CRs
		// (the K8s API would reject `$` in metadata.name) and flate
		// shouldn't try to reconcile them either; the substituted copy
		// emitted by the parent KS's render is the reconcilable one.
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			slog.Debug("loader: skipped template file (unresolved envsubst in name/namespace)",
				"path", path, "id", id.String())
			continue
		}
		if l.PreferExisting && l.Store.GetObject(id) != nil {
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
