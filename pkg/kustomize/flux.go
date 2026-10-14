package kustomize

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"time"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"sigs.k8s.io/kustomize/api/resmap"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
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

// RenderRequest carries the authoritative source and its mount label. Label is
// metadata and need not name an existing host path.
type RenderRequest struct {
	// Source.FS supplies content; nil Source or FS is unavailable.
	// Empty Source.Key denotes mutable content validated on every cache hit.
	Source *store.SourceArtifact
	// ApplyIgnore selects source-controller exclusions for admitted local aliases.
	ApplyIgnore bool
	// Label mounts source content in kyaml; empty uses Source.LocalPath.
	Label string
	// SubPath selects the source directory for this Kustomization.
	SubPath string
	// Spec is the complete Flux Kustomization document.
	Spec map[string]any
}

// RenderFlux builds through one common filesystem and a private render overlay.
// Generated files never mutate the source. Cancellation is checked around the
// upstream build, which does not accept a context.
func RenderFlux(ctx context.Context, cache *TreeCache, req RenderRequest) ([]byte, error) {
	if cache == nil || req.Spec == nil {
		return nil, fmt.Errorf("%w: incomplete render request", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Source == nil || req.Source.FS == nil {
		return nil, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
	}
	lower, ignoreKey, err := source.ReadView(ctx, req.Source.FS, req.ApplyIgnore)
	if err != nil {
		return nil, err
	}
	root := cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source")
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: source label: %w", manifest.ErrInput, err)
	}
	key := ""
	if cache.render != nil {
		key = renderKey(req.Spec, root, req.SubPath, ignoreKey)
		if req.Source.Key != "" {
			key = manifest.Fingerprint([]string{key, req.Source.Key})
		}
	}
	if rc := cache.render; rc != nil && key != "" {
		if snap, output, ok := rc.get(key); ok {
			if req.Source.Key != "" {
				return output, nil
			}
			if snap.stillValidFS(lower) {
				return output, nil
			}
		}
	}
	var rec *readSet
	if cache.render != nil && key != "" {
		rec = newReadSet(root)
	}
	var mutableRec *readSet
	if req.Source.Key == "" {
		mutableRec = rec
	}
	memFS := newTreeFS(lower, root, mutableRec)
	spec, _ := req.Spec["spec"].(map[string]any)
	interval, _ := spec["interval"].(string)
	memFS.maxAge, _ = time.ParseDuration(interval)
	subDir := root + string(filepath.Separator) + strings.TrimLeft(req.SubPath, "/")
	canonicalDir, file, err := memFS.CleanedAbs(subDir)
	if err != nil {
		return nil, fmt.Errorf("%w: kustomization path is not a directory: %s: %w", manifest.ErrInput, req.SubPath, err)
	}
	if file != "" {
		return nil, fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, req.SubPath)
	}
	subDir = canonicalDir.String()
	rawSpec, subPath := req.Spec, req.SubPath

	// Pre-fetch any HTTP/HTTPS entries and remote git bases in kustomization
	// resources: into the memory layer so the build only ever sees local files
	// and never reaches the network (kustomize's git/HTTP fallback would
	// otherwise run outside the fs sandbox). Scoped to subDir. See preflight.go.
	injected, err := preflightRemoteResources(ctx, cache, memFS, subDir)
	if err != nil {
		return nil, err
	}

	// Merge spec.patches/images/components/targetNamespace/namePrefix/nameSuffix
	// into the kustomization.yaml exactly as flux's Generator does, then write it
	// into the memory layer for the build to consume. See generate.go.
	data, kfile, err := generateManifest(memFS, subDir, rawSpec)
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
		built := fmt.Errorf("kustomize build %s: %w", subPath, err)
		// On a duplicate-resource-id failure, name every accumulated path that
		// emits the colliding resource so a localized duplicate is locatable.
		if strings.Contains(err.Error(), "already registered id") {
			if attr := duplicateProducers(memFS, subDir, subPath, data); attr != "" {
				return nil, fmt.Errorf("%w\n%s", built, attr)
			}
		}
		return nil, built
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
	// Remote inputs have no identity in the source read set.
	if rec != nil && !injected {
		cache.render.put(key, rec.snapshot(), out)
	}
	return out, nil
}

// renderKeyVersion MUST change whenever rendered bytes or cache framing change.
const renderKeyVersion = "render-fs-v4"

// renderKey includes source options and the immutable content pin. Alias
// content is validated by the read set rather than a whole-checkout hash.
func renderKey(rawSpec map[string]any, sourceRoot, subPath, ignoreKey string) string {
	return manifest.Fingerprint(struct {
		Spec       map[string]any
		SourceRoot string
		SubPath    string
		Ignore     string
		Version    string
	}{rawSpec, sourceRoot, subPath, ignoreKey, renderKeyVersion})
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
	const group = manifest.FluxKustomizeDomain
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
