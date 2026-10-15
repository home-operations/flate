package kustomize

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"time"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"github.com/go-git/go-git/v5/plumbing"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
	"sigs.k8s.io/kustomize/api/resmap"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

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

// RenderFlux consumes admitted sources through one filesystem and a private overlay.
// Generated files never mutate the source. Cancellation is checked around the
// upstream build, which does not accept a context.
func RenderFlux(ctx context.Context, cache *TreeCache, req RenderRequest, retry sourcecache.RetryConfig) ([]byte, error) {
	if cache == nil || req.Spec == nil {
		return nil, fmt.Errorf("%w: incomplete render request", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Source == nil || req.Source.FS == nil {
		return nil, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
	}
	lower, root := req.Source.FS, cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source")
	ignoreKey := ""
	if req.ApplyIgnore {
		input, err := cache.sourceRootFor(ctx, req.Source, root)
		if err != nil {
			return nil, err
		}
		lower, root = input.fs, input.root
	}
	var err error
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: source label: %w", manifest.ErrInput, err)
	}
	if req.ApplyIgnore {
		ignoreKey = "source"
	}
	key := renderKey(req.Spec, root, req.SubPath, ignoreKey)
	if req.Source.Key != "" {
		key = manifest.Fingerprint([]string{key, req.Source.Key})
	}
	blobKey := sourcecache.BlobKey{ID: key, PersistOnly: true}
	reader, _, err := cache.owner.Blob(ctx, blobKey, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if err != nil && !errors.Is(err, errdef.ErrNotFound) {
		return nil, fmt.Errorf("kustomize render cache lookup: %w", err)
	}
	if err == nil {
		raw, readErr := io.ReadAll(reader)
		if err := errors.Join(readErr, reader.Close()); err != nil {
			return nil, fmt.Errorf("%w: read kustomize render cache: %w", manifest.ErrFlux, err)
		}
		snap, output, err := decodeFrame(raw)
		if err != nil {
			slog.Debug("kustomize: decode render frame", "err", err)
		} else if req.Source.Key != "" || snap.stillValidFS(lower) {
			return output, nil
		}
	}
	rec := newReadSet(root)
	var mutableRec *readSet
	if req.Source.Key == "" {
		mutableRec = rec
	}
	memFS := newTreeFS(lower, root, mutableRec)
	memFS.retry = retry
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

	injected, err := preflightRemoteResources(ctx, cache, memFS, subDir)
	if err != nil {
		return nil, err
	}
	data, err = memFS.ReadFile(kfile)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rm, err := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
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
	if !injected {
		reader, _, err := cache.owner.Blob(ctx, blobKey, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
			frame, err := encodeFrame(rec.snapshot(), out)
			if err != nil {
				return ocispec.Descriptor{}, fmt.Errorf("encode kustomize render frame: %w", err)
			}
			_, err = io.Copy(w, bytes.NewReader(frame))
			return ocispec.Descriptor{}, err
		})
		if err != nil {
			return nil, fmt.Errorf("kustomize render cache publish: %w", err)
		}
		if err := reader.Close(); err != nil {
			return nil, fmt.Errorf("%w: close kustomize render frame: %w", manifest.ErrFlux, err)
		}
	}
	return out, nil
}

// renderKeyVersion MUST change whenever rendered bytes or cache framing change.
const renderKeyVersion = "render-fs-v5"

// Mutable source content is validated by the read set rather than a
// whole-checkout hash; immutable pins are folded into this domain key at lookup.
func renderKey(rawSpec map[string]any, sourceRoot, subPath, ignoreKey string) string {
	return manifest.Fingerprint(struct {
		Spec       map[string]any
		SourceRoot string
		SubPath    string
		Ignore     string
		Version    string
	}{rawSpec, sourceRoot, subPath, ignoreKey, renderKeyVersion})
}

type sourceRoot struct {
	root string
	fs   fs.FS
}

// Source admission belongs to the run owner. Every reader of a source MUST use
// the same view; the checkout MUST remain stable until all readers finish.
func (c *TreeCache) sourceRootFor(ctx context.Context, source *store.SourceArtifact, root string) (*sourceRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := struct {
		source *store.SourceArtifact
		root   string
	}{source, root}
	if cached, ok := c.sourceRoots.Load(key); ok {
		return cached.(func() (*sourceRoot, error))()
	}
	// Shared admission belongs to the cache lifetime, not the first reader's cancellation.
	load := sync.OnceValues(func() (*sourceRoot, error) {
		input, err := gittree.New(context.Background(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: source.FS})
		if err != nil {
			return nil, err
		}
		return &sourceRoot{root: root, fs: input}, nil
	})
	actual, _ := c.sourceRoots.LoadOrStore(key, load)
	return actual.(func() (*sourceRoot, error))()
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
