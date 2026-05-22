package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kustomize/api/resmap"

	"github.com/home-operations/flate/pkg/manifest"
)

// pathLocks serialize concurrent renders against the same staged path —
// Flux's Generator mutates kustomization.yaml in place, so parallel
// builds against the same path race.
var (
	pathLocksMu sync.Mutex
	pathLocks   = map[string]*sync.Mutex{}
)

func lockPath(path string) *sync.Mutex {
	pathLocksMu.Lock()
	defer pathLocksMu.Unlock()
	if l, ok := pathLocks[path]; ok {
		return l
	}
	l := &sync.Mutex{}
	pathLocks[path] = l
	return l
}

// RenderFlux renders a Flux kustomize.toolkit.fluxcd.io Kustomization
// using the same library that Flux's kustomize-controller uses
// (`github.com/fluxcd/pkg/kustomize`).
//
// The Generator merges spec.patches / spec.images / spec.components /
// spec.targetNamespace / spec.namePrefix / spec.nameSuffix into the
// kustomization.yaml before krusty runs. spec.commonMetadata is
// applied post-build (see applyCommonMetadata) because the Generator
// does not handle it — kustomize-controller does it after build via
// ssautil.SetCommonMetadata.
//
// ctx is honored at coarse boundaries — between path validation,
// after acquiring the per-path lock, and before/after SecureBuild —
// because fluxcd/pkg/kustomize.NewGenerator/SecureBuild do not
// themselves accept a ctx. A cancelled ctx returns ctx.Err() rather
// than completing the (potentially expensive) build.
//
// The source tree at sourceRoot is never modified — staging is handled
// by `cache` which produces a writable copy. rawSpec must be the
// original Flux Kustomization document (the Contents field on
// manifest.Kustomization). subPath is the spec.path value relative to
// sourceRoot.
func RenderFlux(ctx context.Context, cache *StagingCache, sourceRoot, subPath string, rawSpec map[string]any) ([]byte, error) {
	if cache == nil {
		return nil, errors.New("kustomize: nil staging cache")
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
	if err := validatePath(filepath.Join(sourceRoot, subPath)); err != nil {
		return nil, err
	}

	staged, err := cache.Stage(sourceRoot)
	if err != nil {
		return nil, err
	}
	if r, err := filepath.EvalSymlinks(staged); err == nil {
		staged = r
	}

	stagedSub := filepath.Join(staged, subPath)
	if err := validatePath(stagedSub); err != nil {
		return nil, err
	}

	// Serialize concurrent reconciles of the same path. Flux's
	// Generator merges patches / images / components into the
	// kustomization file at the staged path — restoring the source
	// baseline + writing the Generator output must happen atomically
	// per Kustomization.
	lock := lockPath(stagedSub)
	lock.Lock()
	defer lock.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Restore the source kustomization.yaml before each Generator
	// run so repeat reconciles (e.g. when a parent renders and
	// re-emits a child) don't accumulate appended patches / images /
	// components from a previous merge.
	if err := restoreKustomizationFile(sourceRoot, stagedSub, subPath); err != nil {
		return nil, err
	}

	u := &unstructured.Unstructured{Object: rawSpec}
	gen := fluxkustomize.NewGenerator(staged, *u)
	if _, err := gen.WriteFile(stagedSub); err != nil {
		return nil, fmt.Errorf("flux kustomize generator: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rm, err := fluxkustomize.SecureBuild(staged, stagedSub, false)
	if err != nil {
		return nil, fmt.Errorf("kustomize build %s: %w", subPath, err)
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
			merged := maps.Clone(r.GetLabels())
			if merged == nil {
				merged = map[string]string{}
			}
			maps.Copy(merged, labels)
			if err := r.SetLabels(merged); err != nil {
				return err
			}
		}
		if len(annotations) > 0 {
			merged := maps.Clone(r.GetAnnotations())
			if merged == nil {
				merged = map[string]string{}
			}
			maps.Copy(merged, annotations)
			if err := r.SetAnnotations(merged); err != nil {
				return err
			}
		}
	}
	return nil
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

// restoreKustomizationFile copies the source kustomization.yaml (if
// any) over the staged one so each Flux Generator run sees a clean
// baseline. A no-op when the source has none — Generator will create
// one from scratch.
func restoreKustomizationFile(sourceRoot, stagedSub, subPath string) error {
	srcDir := filepath.Join(sourceRoot, subPath)
	for _, name := range kustomizationFilenames {
		srcPath := filepath.Join(srcDir, name)
		info, err := os.Stat(srcPath)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(srcPath) //nolint:gosec // srcPath inside our cluster source root
		if err != nil {
			return fmt.Errorf("restore kustomization.yaml: %w", err)
		}
		// Remove every other variant from the stage so Generator
		// writes to the canonical filename.
		for _, other := range kustomizationFilenames {
			if other != name {
				_ = os.Remove(filepath.Join(stagedSub, other))
			}
		}
		return os.WriteFile(filepath.Join(stagedSub, name), data, info.Mode().Perm()) //nolint:gosec // stagedSub is our own tempdir
	}
	// No source kustomization.yaml — remove any stale staged copy
	// so Generator starts cleanly.
	for _, name := range kustomizationFilenames {
		_ = os.Remove(filepath.Join(stagedSub, name))
	}
	return nil
}

// kustomizationFilenames is the canonical set kustomize looks for at
// any directory it builds.
var kustomizationFilenames = []string{"kustomization.yaml", "kustomization.yml", "Kustomization"}

// validatePath returns a clean ErrInput when p is missing or isn't a
// directory.
func validatePath(p string) error {
	info, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: kustomization path does not exist: %s", manifest.ErrInput, p)
		}
		return fmt.Errorf("%w: stat kustomization path %s: %v", manifest.ErrInput, p, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, p)
	}
	return nil
}
