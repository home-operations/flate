package kustomize

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"time"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
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
	// Source is authoritative; nil selects the disk input at Label.
	Source *store.SourceArtifact
	// ApplyIgnore filters path-only aliases with source-controller exclusions.
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
	if req.Source == nil {
		req.Source = &store.SourceArtifact{LocalPath: req.Label}
	}
	if req.Source.FS == nil && req.Source.Key != "" {
		return nil, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
	}
	if req.Source.FS != nil && req.Source.Key == "" {
		return nil, fmt.Errorf("%w: source artifact has no identity", manifest.ErrInput)
	}
	lower, root := req.Source.FS, cmp.Or(req.Label, req.Source.LocalPath)
	var disk filesys.FileSystem
	ignoreKey := ""
	if lower == nil {
		if root == "" {
			return nil, fmt.Errorf("%w: empty source root", manifest.ErrInput)
		}
		dr, err := cache.diskRootFor(root)
		if err != nil {
			return nil, err
		}
		disk, root = dr.diskFS, dr.root
		if req.ApplyIgnore {
			ig, err := dr.ignored()
			if err != nil {
				return nil, err
			}
			disk, ignoreKey = ig.fs, ig.key
		}
		lower = diskInput{FileSystem: disk, root: root, info: make(map[string]fs.FileInfo)}
	} else {
		root = cmp.Or(root, string(filepath.Separator)+"flate-source")
		var err error
		root, err = filepath.Abs(root)
		if err != nil {
			return nil, fmt.Errorf("%w: source label: %w", manifest.ErrInput, err)
		}
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
			if disk == nil {
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
	var diskRec *readSet
	if disk != nil {
		diskRec = rec
	}
	memFS := newTreeFS(lower, root, diskRec)
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
	// Persist for the next run, unless the build consumed inputs the disk
	// read-set can't validate: pre-fetched remote content (injected) or a read
	// outside the source root (rec.bypassed).
	if rec != nil && !injected && !rec.bypassed() {
		cache.render.put(key, rec.snapshot(), out)
	}
	return out, nil
}

// renderKeyVersion MUST change whenever rendered bytes or cache framing change.
const renderKeyVersion = "render-fs-v3"

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

// diskRoot holds the per-sourceRoot invariants RenderFlux reuses across every
// Kustomization that targets the same root: the symlink-resolved absolute root
// and its secure on-disk FS. diskFS is an immutable fsSecure value over a
// stateless MakeFsOnDisk, so it is safe to share read-only across goroutines.
// ignored lazily derives the working-tree view — diskFS behind the
// source-controller ignore matcher loaded from root — plus the .sourceignore
// fingerprint that keys it; built once per root because loading the matcher
// walks the whole tree.
type diskRoot struct {
	root    string
	diskFS  filesys.FileSystem
	ignored func() (ignoredRoot, error)
}

type ignoredRoot struct {
	fs  filesys.FileSystem
	key string
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
	dr.ignored = sync.OnceValues(func() (ignoredRoot, error) {
		matcher, err := sourceignore.New(resolved, nil, true)
		if err != nil {
			return ignoredRoot{}, err
		}
		key, err := sourceignore.Fingerprint(resolved)
		if err != nil {
			return ignoredRoot{}, err
		}
		return ignoredRoot{fs: newIgnoreFS(diskFS, resolved, matcher), key: key}, nil
	})
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
