package kustomize

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// BuildMutex serializes every krusty/kustomize build flate runs in a
// process. kustomize's krusty pipeline mutates package-global state
// (the openapi schema registry + builtin-plugin/transformer factories)
// that is NOT goroutine-safe — fluxcd/pkg/kustomize guards its own
// Build with an internal mutex for exactly this reason, but that mutex
// does not extend to OTHER krusty entrypoints in the same process
// (flate's helm postRenderer runs krusty.Run directly). Two concurrent
// builds — one KS Build, one HR postRender — race on the shared globals
// and produce nondeterministic corruption: empty / torn rendered output
// surfacing as "missing metadata.name" decode errors, dropped resources,
// or cascade failures that flip run-to-run. Every flate-owned krusty
// invocation MUST hold this lock.
var BuildMutex sync.Mutex

// RenderFlux renders a Flux kustomize.toolkit.fluxcd.io Kustomization
// entirely in memory, using the same merge + build the kustomize-controller
// performs.
//
// sourceRoot is resolved and wrapped in a secure on-disk FS once per root
// (memoized in cache.diskRootFor); each render derives its own private
// memory-over-disk overlay from that shared disk FS, writes the merged
// kustomization.yaml + any pre-fetched remote resources into the overlay's
// in-memory layer, and builds with fluxkustomize.Build. The source tree is
// never copied or mutated and no two renders share mutable state, so renders
// run fully parallel with no staging lock. The secure disk layer confines reads
// to the root (a path escaping it simply does not resolve), giving
// SecureBuild's security posture for free.
//
// applyIgnore selects whether source-controller's default file exclusions
// (.sops.yaml, binaries, CI dirs, in-tree .sourceignore) are applied while
// auto-generating a kustomization — true for working-tree / self-referential
// sources that never passed through a fetcher's artifact filtering, false for
// already-filtered fetched artifacts. spec.commonMetadata is applied post-build
// (the Generator does not handle it, mirroring kustomize-controller's
// apply-time pass).
//
// ctx is honored at coarse boundaries (entry, before build) because
// fluxkustomize.Build does not itself accept a ctx.
func RenderFlux(ctx context.Context, cache *TreeCache, sourceRoot string, applyIgnore bool, subPath string, rawSpec map[string]any) ([]byte, error) {
	if cache == nil {
		return nil, errors.New("kustomize: nil tree cache")
	}
	if sourceRoot == "" {
		return nil, fmt.Errorf("%w: empty source root", manifest.ErrInput)
	}
	if rawSpec == nil {
		return nil, fmt.Errorf("%w: nil flux Kustomization spec", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Resolve the source root and build its secure on-disk FS once per root.
	// Both are pure functions of sourceRoot and the secure FS is an immutable,
	// goroutine-safe value, so cache.diskRootFor memoizes them across every KS
	// that shares this root — sparing each render a duplicate EvalSymlinks +
	// CleanedAbs syscall pair.
	dr, err := cache.diskRootFor(sourceRoot)
	if err != nil {
		return nil, err
	}
	sourceRoot = dr.root

	// Memory-over-disk overlay: source files are read from the secure on-disk
	// FS rooted at sourceRoot (no real-FS reach beyond root; symlinks
	// evaluated), while the merged kustomization.yaml + any pre-fetched remote
	// resources are written to an in-memory layer that shadows disk. The source
	// tree is never copied or mutated, and renders stay fully parallel (each
	// gets its own overlay). Reading source from disk also sidesteps the
	// in-memory fs's filename restriction, so trees with exotic names (spaces,
	// etc.) render.
	memFS := newOverlayFS(dr.diskFS)

	subDir := filepath.Join(sourceRoot, subPath)
	if info, statErr := os.Stat(subDir); statErr != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, subPath)
	}

	// Pre-fetch any HTTP/HTTPS entries and remote git bases in kustomization
	// resources: into the memory layer so the build only ever sees local files
	// and never reaches the network (kustomize's git/HTTP fallback would
	// otherwise run outside the fs sandbox). Scoped to subDir. See preflight.go.
	if err := preflightRemoteResources(ctx, cache, memFS, subDir); err != nil {
		return nil, err
	}

	// For working-tree / self-referential sources (applyIgnore), exclude
	// source-controller-ignored files while auto-generating a kustomization, so
	// the tree renders like a fetched artifact would. Fetched artifacts were
	// already filtered by their fetcher, so they pass nil.
	var ignore *sourceignore.Matcher
	if applyIgnore {
		m, ierr := sourceignore.New(subDir, nil, true)
		if ierr != nil {
			return nil, ierr
		}
		ignore = m
	}

	// Merge spec.patches/images/components/targetNamespace/namePrefix/nameSuffix
	// into the kustomization.yaml exactly as flux's Generator does, then write it
	// into the memory layer for the build to consume. See generate.go.
	data, kfile, err := generateManifest(memFS, subDir, rawSpec, ignore)
	if err != nil {
		return nil, fmt.Errorf("flux kustomize generator: %w", err)
	}
	if err := memFS.WriteFile(kfile, data); err != nil {
		return nil, fmt.Errorf("write kustomization.yaml: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rm, err := func() (resmap.ResMap, error) {
		BuildMutex.Lock()
		defer BuildMutex.Unlock()
		return fluxkustomize.Build(memFS, subDir)
	}()
	if err != nil {
		return nil, fmt.Errorf("kustomize build %s: %w", subPath, err)
	}
	// Owner labels first so user-supplied spec.commonMetadata wins on a key
	// collision. Matches kustomize-controller's ordering: SetOwnerLabels runs at
	// reconcile setup, SetCommonMetadata runs at apply time and overwrites.
	if err := applyOwnerLabels(rm, rawSpec); err != nil {
		return nil, fmt.Errorf("apply owner labels %s: %w", subPath, err)
	}
	if err := applyCommonMetadata(rm, rawSpec); err != nil {
		return nil, fmt.Errorf("apply commonMetadata %s: %w", subPath, err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("kustomize render %s: %w", subPath, err)
	}
	return out, nil
}

// diskRoot holds the per-sourceRoot invariants RenderFlux reuses across every
// Kustomization that targets the same root: the symlink-resolved absolute root
// and its secure on-disk FS. diskFS is an immutable fsSecure value over a
// stateless MakeFsOnDisk, so it is safe to share read-only across goroutines.
type diskRoot struct {
	root   string
	diskFS filesys.FileSystem
}

// diskRootFor returns the cached diskRoot for sourceRoot, computing and
// memoizing it on first use. The (EvalSymlinks, MakeFsOnDiskSecure) pair it
// performs is a pure function of sourceRoot, so the result is shared across the
// run's renders of that root. Errors are not cached — they are rare and the
// caller wants each to surface — and carry the same wrapping the inline path
// used. Keyed by the raw input sourceRoot (already validated non-empty).
func (c *TreeCache) diskRootFor(sourceRoot string) (*diskRoot, error) {
	if v, ok := c.diskRoots.Load(sourceRoot); ok {
		return v.(*diskRoot), nil
	}
	resolved := sourceRoot
	if r, err := filepath.EvalSymlinks(sourceRoot); err == nil {
		resolved = r
	}
	diskFS, err := fluxfilesys.MakeFsOnDiskSecure(resolved)
	if err != nil {
		return nil, fmt.Errorf("kustomize: secure fs %s: %w", resolved, err)
	}
	dr := &diskRoot{root: resolved, diskFS: diskFS}
	actual, _ := c.diskRoots.LoadOrStore(sourceRoot, dr)
	return actual.(*diskRoot), nil
}

// applyCommonMetadata merges spec.commonMetadata.labels and
// spec.commonMetadata.annotations into every rendered resource —
// mirroring kustomize-controller's ssautil.SetCommonMetadata pass,
// which fluxcd/pkg/kustomize.Generator does NOT perform.
func applyCommonMetadata(rm resmap.ResMap, rawSpec map[string]any) error {
	spec, _ := rawSpec["spec"].(map[string]any)
	cm, _ := spec["commonMetadata"].(map[string]any)
	labels := stringMap(cm["labels"])
	annotations := stringMap(cm["annotations"])
	if len(labels) == 0 && len(annotations) == 0 {
		return nil
	}
	for _, r := range rm.Resources() {
		if len(labels) > 0 {
			if err := r.SetLabels(overlayStringMap(r.GetLabels(), labels)); err != nil {
				return err
			}
		}
		if len(annotations) > 0 {
			if err := r.SetAnnotations(overlayStringMap(r.GetAnnotations(), annotations)); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyOwnerLabels stamps every rendered resource with the parent's
// "kustomize.toolkit.fluxcd.io/name" + "/namespace" labels — matching
// what kustomize-controller injects via ssa.ResourceManager.SetOwnerLabels
// before apply. These labels are how real Flux tracks ownership for
// pruning + selection (kubectl get -l kustomize.toolkit.fluxcd.io/name=...).
//
// Inject during render so flate's output matches what lands in-cluster
// rather than what's on disk.
func applyOwnerLabels(rm resmap.ResMap, rawSpec map[string]any) error {
	md, _ := rawSpec["metadata"].(map[string]any)
	name, _ := md["name"].(string)
	if name == "" {
		return nil
	}
	namespace, _ := md["namespace"].(string)
	const group = "kustomize.toolkit.fluxcd.io"
	// Build owner overlay with only non-empty values so a cluster-scoped
	// Kustomization (no namespace) doesn't clobber an existing namespace label.
	owner := make(map[string]string, 2)
	owner[group+"/name"] = name
	if namespace != "" {
		owner[group+"/namespace"] = namespace
	}
	for _, r := range rm.Resources() {
		if err := r.SetLabels(overlayStringMap(r.GetLabels(), owner)); err != nil {
			return err
		}
	}
	return nil
}

// overlayStringMap returns a copy of base with every entry from overlay
// merged in. overlay wins on key collisions. RNode.GetLabels /
// GetAnnotations always return a non-nil map, so base is never nil here.
func overlayStringMap(base, overlay map[string]string) map[string]string {
	out := maps.Clone(base)
	maps.Copy(out, overlay)
	return out
}

func stringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}
