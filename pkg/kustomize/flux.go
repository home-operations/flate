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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"oras.land/oras-go/v2/errdef"
	"sigs.k8s.io/kustomize/api/resmap"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// RenderRequest carries the authoritative source and its mount label. Label is
// metadata and need not name an existing host path.
type RenderRequest struct {
	// HeldSlot is TreeCache's borrowed service when this render holds its slot.
	// Standalone callers leave it nil.
	HeldSlot *task.Service
	// Source.FS supplies content; nil Source or FS is unavailable.
	// Empty Source.Key denotes mutable content validated on every cache hit.
	Source *store.SourceArtifact
	// ApplyIgnore selects source-controller exclusions for admitted local aliases.
	ApplyIgnore bool
	// Label mounts source content in kyaml; empty uses Source.LocalPath.
	Label string
	// SubPath selects the source directory to build.
	SubPath string
	// Spec is the prepared Flux Kustomization; nil builds without a Flux owner.
	Spec *manifest.Kustomization
}

// Render returns unstamped renderer documents from an admitted source and a private overlay.
// Generated files never mutate the source. Cancellation is checked around the
// upstream build, which does not accept a context.
func Render(ctx context.Context, cache *TreeCache, req RenderRequest, retry sourcecache.RetryConfig) ([]map[string]any, error) {
	if cache == nil {
		return nil, fmt.Errorf("%w: incomplete render request", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Source == nil || req.Source.FS == nil {
		return nil, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
	}
	lower, root := req.Source.FS, cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source")
	var ignore any
	if req.ApplyIgnore {
		input, err := cache.sourceRootFor(ctx, req.Source, root)
		if err != nil {
			return nil, err
		}
		lower, ignore = input.fs, input.ignore
	}
	var err error
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("%w: source label: %w", manifest.ErrInput, err)
	}
	var maxAge time.Duration
	if req.Spec != nil {
		maxAge = req.Spec.Interval.Duration
	}
	key := renderKey(req, root, ignore)
	blobKey := sourcecache.BlobKey{Content: key, PersistOnly: true}
	reader, _, _, err := cache.owner.Blob(ctx, blobKey, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
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
		} else {
			valid, err := snap.stillValid(ctx, cache, &treeFS{lower: lower, retry: retry, maxAge: maxAge, slot: req.HeldSlot}, req.Source.Key != "")
			if err != nil {
				return nil, err
			}
			if valid {
				if docs, err := manifest.DecodeRender(output); err == nil {
					return docs, nil
				}
			}
		}
	}
	rec := newReadSet(root)
	rec.immutable = req.Source.Key != ""
	memFS := newTreeFS(lower, root, rec)
	memFS.retry = retry
	memFS.slot = req.HeldSlot
	memFS.maxAge = maxAge
	subDir := root + string(filepath.Separator) + strings.TrimLeft(req.SubPath, "/")
	canonicalDir, file, err := memFS.CleanedAbs(subDir)
	if err != nil {
		return nil, fmt.Errorf("%w: kustomization path is not a directory: %s: %w", manifest.ErrInput, req.SubPath, err)
	}
	if file != "" {
		return nil, fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, req.SubPath)
	}
	subDir = canonicalDir.String()
	var ks map[string]any
	var data []byte
	var kfile string
	if req.Spec != nil {
		ks, err = req.Spec.RenderDocument()
		if err != nil {
			return nil, err
		}
		data, kfile, err = generateManifest(memFS, subDir, ks)
		if err != nil {
			return nil, fmt.Errorf("flux kustomize generator: %w", err)
		}
		if err := memFS.WriteFile(kfile, data); err != nil {
			return nil, fmt.Errorf("write kustomization.yaml: %w", err)
		}
	} else {
		kfile, _ = kustomizationFileIn(memFS, subDir)
	}
	subPath := req.SubPath

	if err := preflightRemoteResources(ctx, cache, memFS, subDir); err != nil {
		return nil, err
	}
	if kfile != "" {
		data, err = memFS.ReadFile(kfile)
		if err != nil {
			return nil, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rm, err := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		return kustomizestate.Build(memFS, subDir)
	}()
	if err != nil {
		built := fmt.Errorf("%w: kustomize build %s: %w", manifest.ErrFlux, subPath, err)
		// On a duplicate-resource-id failure, name every accumulated path that
		// emits the colliding resource so a localized duplicate is locatable.
		if strings.Contains(err.Error(), "already registered id") {
			if attr := duplicateProducers(memFS, subDir, subPath, data); attr != "" {
				return nil, fmt.Errorf("%w\n%s", built, attr)
			}
		}
		return nil, built
	}
	// AsYAML converts through JSON, so label scalar spelling MUST be retained first.
	if req.Spec != nil && req.Spec.EmissionPolicy().StringLabels {
		for _, r := range rm.Resources() {
			if err := r.SetLabels(r.GetLabels()); err != nil {
				return nil, fmt.Errorf("%w: normalize labels: %w", manifest.ErrInput, err)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	out, err := rm.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("kustomize render %s: %w", subPath, err)
	}
	docs, err := manifest.SplitDocs(out)
	if err != nil {
		return nil, err
	}
	reader, _, _, err = cache.owner.Blob(ctx, blobKey, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		payload, err := manifest.EncodeRender(docs)
		if err != nil {
			return sourcecache.ProducerResult{}, err
		}
		frame, err := encodeFrame(rec.snapshot(), payload)
		if err != nil {
			return sourcecache.ProducerResult{}, fmt.Errorf("encode kustomize render frame: %w", err)
		}
		_, err = io.Copy(w, bytes.NewReader(frame))
		return sourcecache.ProducerResult{}, err
	})
	if err != nil {
		return nil, fmt.Errorf("kustomize render cache publish: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("%w: close kustomize render frame: %w", manifest.ErrFlux, err)
	}
	return docs, nil
}

// Mutable files are validated by the read set; immutable generations are part
// of the render content. Filesystem handles and retry/storage policy are never encoded.
func renderKey(req RenderRequest, root string, ignore any) string {
	var immutable any
	if req.Source.Key != "" {
		immutable = struct{ Key, Revision, Digest string }{req.Source.Key, req.Source.Revision, req.Source.Digest}
	}
	var spec any
	if req.Spec != nil {
		spec = req.Spec.Content(nil)
	}
	return manifest.Fingerprint(struct {
		Spec        any
		SourceRoot  string
		SubPath     string
		ApplyIgnore bool
		Ignore      any
		Immutable   any
	}{spec, root, req.SubPath, req.ApplyIgnore, ignore, immutable})
}

type sourceRoot struct {
	fs     fs.FS
	ignore any
}

// Source admission belongs to the run owner. Every reader of a source MUST use
// the same view; the checkout MUST remain stable until all readers finish.
func (c *TreeCache) sourceRootFor(ctx context.Context, artifact *store.SourceArtifact, root string) (*sourceRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := struct {
		source *store.SourceArtifact
		root   string
	}{artifact, root}
	if cached, ok := c.sourceRoots.Load(key); ok {
		return cached.(func() (*sourceRoot, error))()
	}
	// Shared admission belongs to the cache lifetime, not the first reader's cancellation.
	load := sync.OnceValues(func() (*sourceRoot, error) {
		input, _, err := gittree.New(context.Background(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS})
		if err != nil {
			return nil, err
		}
		return &sourceRoot{fs: input, ignore: input.PolicyEvidence()}, nil
	})
	actual, _ := c.sourceRoots.LoadOrStore(key, load)
	return actual.(func() (*sourceRoot, error))()
}
