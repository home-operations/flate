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
// The source tree at sourceRoot is materialized once per run into an immutable
// byte snapshot (cache.snapshot); each render derives its own private in-memory
// filesystem from it, writes the merged kustomization.yaml + any pre-fetched
// remote resources into that private fs, and builds with fluxkustomize.Build.
// Nothing touches disk and no two renders share mutable state, so renders run
// fully parallel with no staging lock. The in-memory fs is inherently sandboxed
// (no real-FS reach; a path escaping the root simply does not exist), giving
// SecureBuild's security posture for free.
//
// applyIgnore selects whether source-controller's default file exclusions
// (.sops.yaml, binaries, CI dirs, in-tree .sourceignore) are applied while
// materializing — true for working-tree / self-referential sources that never
// passed through a fetcher's artifact filtering, false for already-filtered
// fetched artifacts. spec.commonMetadata is applied post-build (the Generator
// does not handle it, mirroring kustomize-controller's apply-time pass).
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

	if r, err := filepath.EvalSymlinks(sourceRoot); err == nil {
		sourceRoot = r
	}

	// Memory-over-disk overlay: source files are read from a secure on-disk FS
	// rooted at sourceRoot (no real-FS reach beyond root; symlinks evaluated),
	// while the merged kustomization.yaml + any pre-fetched remote resources are
	// written to an in-memory layer that shadows disk. The source tree is never
	// copied or mutated, and renders stay fully parallel (each gets its own
	// overlay). Reading source from disk also sidesteps the in-memory fs's
	// filename restriction, so trees with exotic names (spaces, etc.) render.
	diskFS, err := fluxfilesys.MakeFsOnDiskSecure(sourceRoot)
	if err != nil {
		return nil, fmt.Errorf("kustomize: secure fs %s: %w", sourceRoot, err)
	}
	memFS := newOverlayFS(diskFS)

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
