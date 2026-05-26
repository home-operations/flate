package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kustomize/api/resmap"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
)

// stageLocks serialize concurrent RenderFlux calls that share a
// staged tree. The lock is keyed by the stage ROOT (one per source
// directory), not the subPath, because Flux's Generator writes
// kustomization.yaml at stagedSub AND SecureBuild walks downward
// from stagedSub reading nested kustomization.yaml files. When two
// calls hold ancestor/descendant subPaths under the same staged tree
// — common in repos with a root KS that traverses nested apps — A's
// SecureBuild reads B's kustomization.yaml mid-write. The per-subPath
// locking that existed before couldn't see the overlap and produced
// intermittent "patches accumulated" / wrong-rendered output that
// disappeared on retry.
//
// The cost is reduced render parallelism within a single source
// tree: KSes sharing a sourceRoot now serialize. For repos with a
// single cluster root this is the bulk of renders. Sibling subPaths
// that could otherwise have run in parallel are now sequenced.
// Correctness is the priority — the previous parallelism was incorrect.
var stageLocks = keylock.New[string]()

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

	// Serialize concurrent reconciles within the same staged tree.
	// See stageLocks docstring for why this is the stage root and
	// not stagedSub. ctx cancellation interrupts the acquire.
	release, err := stageLocks.Acquire(ctx, staged)
	if err != nil {
		return nil, err
	}
	defer release()

	// Restore the source kustomization.yaml before each Generator
	// run so repeat reconciles (e.g. when a parent renders and
	// re-emits a child) don't accumulate appended patches / images /
	// components from a previous merge.
	if err := restoreKustomizationFile(sourceRoot, stagedSub, subPath); err != nil {
		return nil, err
	}

	// Pre-fetch any HTTP/HTTPS entries in kustomization `resources:`
	// so kustomize.Build sees local files only and never falls back
	// to `exec.Command("git", "fetch", ...)`. See preflight.go for
	// the why. Scoped to stagedSub (not the whole stage) so a URL
	// failure in one Kustomization's tree fails only that
	// Kustomization's reconcile, not unrelated sibling KSes that
	// share the source root. Components reaching `../` paths outside
	// stagedSub are an acknowledged blind spot.
	if err := preflightRemoteResources(ctx, cache, stagedSub); err != nil {
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
	// Owner labels first so user-supplied spec.commonMetadata wins on a
	// key collision. Matches kustomize-controller's ordering:
	// SetOwnerLabels runs at reconcile setup
	// (internal/controller/kustomization_controller.go:460), then
	// SetCommonMetadata runs at apply time (:844) and overwrites.
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
	owner := map[string]string{
		group + "/name":      name,
		group + "/namespace": namespace,
	}
	for _, r := range rm.Resources() {
		merged := maps.Clone(r.GetLabels())
		if merged == nil {
			merged = map[string]string{}
		}
		for k, v := range owner {
			if v != "" {
				merged[k] = v
			}
		}
		if err := r.SetLabels(merged); err != nil {
			return err
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
//
// Surfaces os.Remove errors via errors.Join: a failed remove leaves
// two kustomization variants in stagedSub, which makes SecureBuild's
// readdir-order-dependent precedence non-deterministic. Symptom is
// repeat reconciles producing different output. fs.ErrNotExist is
// the expected miss and is filtered.
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
		var rmErrs []error
		for _, other := range kustomizationFilenames {
			if other == name {
				continue
			}
			if err := os.Remove(filepath.Join(stagedSub, other)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				rmErrs = append(rmErrs, fmt.Errorf("remove staged %s: %w", other, err))
			}
		}
		// Break any hardlink to source before writing — copyFile may
		// have linked the staged path to the source inode (so renders
		// that don't mutate the file share storage), but an O_TRUNC
		// write would clobber the source's bytes. os.Remove drops just
		// the staged directory entry; the source's link survives.
		stagedPath := filepath.Join(stagedSub, name)
		if err := os.Remove(stagedPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			rmErrs = append(rmErrs, fmt.Errorf("unlink staged %s: %w", name, err))
		}
		if err := os.WriteFile(stagedPath, data, info.Mode().Perm()); err != nil { //nolint:gosec // stagedSub is our own tempdir
			rmErrs = append(rmErrs, fmt.Errorf("write staged %s: %w", name, err))
		}
		return errors.Join(rmErrs...)
	}
	// No source kustomization.yaml — remove any stale staged copy
	// so Generator starts cleanly.
	var rmErrs []error
	for _, name := range kustomizationFilenames {
		if err := os.Remove(filepath.Join(stagedSub, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			rmErrs = append(rmErrs, fmt.Errorf("remove staged %s: %w", name, err))
		}
	}
	return errors.Join(rmErrs...)
}

// kustomizationFilenames is the canonical set kustomize looks for
// at any directory it builds. Aliased here so the restoreKustomizationFile
// loop reads as a local; the canonical declaration is in
// manifest.KustomizeBuilderFilenames so other packages don't
// duplicate the list.
var kustomizationFilenames = manifest.KustomizeBuilderFilenames

// validatePath returns a clean ErrInput when p is missing or isn't a
// directory.
func validatePath(p string) error {
	info, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: kustomization path does not exist: %s", manifest.ErrInput, p)
		}
		return fmt.Errorf("%w: stat kustomization path %s: %w", manifest.ErrInput, p, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, p)
	}
	return nil
}
