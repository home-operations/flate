package kustomize

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"sigs.k8s.io/kustomize/api/resmap"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

var errUncacheableRender = errors.New("uncacheable kustomize render")

// RenderRequest carries the authoritative source and its mount label. Label is
// metadata and need not name an existing host path.
type RenderRequest struct {
	// HeldSlot is TreeCache's borrowed service when this render holds its slot.
	// Standalone callers leave it nil.
	HeldSlot *task.Service
	// Source.FS supplies content; nil Source or FS is unavailable.
	// Empty Source.Content denotes mutable content validated on every cache hit.
	Source *store.SourceArtifact
	// ApplyIgnore selects source-controller exclusions for admitted local aliases.
	ApplyIgnore bool
	// Label mounts source content in kyaml; empty uses Source.LocalPath.
	Label string
	// SubPath selects the source directory to build.
	SubPath string
	// Spec is the prepared Flux Kustomization; nil builds without a Flux owner.
	Spec *manifest.Kustomization
	// WipeSecrets binds the parse policy to accepted history.
	WipeSecrets bool
	// StrictSubstitutions binds undefined-variable handling to accepted history.
	StrictSubstitutions bool
	// SkipKinds binds the effective emitted-kind policy to accepted history.
	SkipKinds []string
	// DependencySubstitutions binds emitted dependency projection to accepted history.
	DependencySubstitutions values.DependencySubstitutions
}

// Render returns unstamped renderer documents from an admitted source and a private overlay.
// Generated files never mutate the source. Cancellation is checked around the
// upstream build, which does not accept a context.
func Render(ctx context.Context, cache *TreeCache, req RenderRequest, retry sourcecache.RetryConfig) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	prepared, err := cache.prepareRender(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	lower, root := prepared.lower, prepared.root
	var maxAge time.Duration
	if req.Spec != nil {
		maxAge = req.Spec.Interval.Duration
	}
	key := prepared.key
	var docs []manifest.DocOccurrence
	var receipt *manifest.RenderReceipt
	blobKey := sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending, Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
		receipt = nil
		raw, err := fs.ReadFile(generation, "content")
		if err != nil {
			return false, fmt.Errorf("%w: read kustomize render cache: %w", manifest.ErrFlux, err)
		}
		frame, err := decodeFrame(raw)
		if err != nil || frame.header.Key != key {
			slog.Debug("kustomize: decode render frame", "err", err)
			return false, nil
		}
		valid, err := frame.header.Reads.stillValid(ctx, cache, &treeFS{lower: lower, retry: retry, maxAge: maxAge}, req.Source.Content != "")
		if err != nil || !valid {
			return false, err
		}
		docs, err = manifest.DecodeRender(frame.output)
		if err != nil {
			return false, nil
		}
		frame.binding = prepared.binding
		binding := manifest.Fingerprint(prepared.binding)
		if frame.header.Binding != binding {
			frame.header.Binding, frame.encoded, frame.raw = binding, nil, ""
			frame.accepted, frame.acceptedKey, frame.identity = nil, "", ""
		}
		frame.identity = sourcecache.AcquisitionIdentity(generation)
		receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
		return true, nil
	}}
	acquire := func() {
		_, _, _, err = cache.owner.Blob(ctx, blobKey, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(ctx context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			rec := newReadSet(root)
			rec.immutable = req.Source.Content != ""
			memFS := newTreeFS(lower, root, rec)
			memFS.retry = retry
			memFS.maxAge = maxAge
			subDir := root + string(filepath.Separator) + strings.TrimLeft(req.SubPath, "/")
			canonicalDir, file, err := memFS.CleanedAbs(subDir)
			if err != nil {
				return sourcecache.ProducerResult{}, fmt.Errorf("%w: kustomization path is not a directory: %s: %w", manifest.ErrInput, req.SubPath, err)
			}
			if file != "" {
				return sourcecache.ProducerResult{}, fmt.Errorf("%w: kustomization path is not a directory: %s", manifest.ErrInput, req.SubPath)
			}
			subDir = canonicalDir.String()
			var ks map[string]any
			var data []byte
			var kfile string
			if req.Spec != nil {
				ks, err = req.Spec.RenderDocument()
				if err != nil {
					return sourcecache.ProducerResult{}, err
				}
				data, kfile, err = generateManifest(memFS, subDir, ks)
				if err != nil {
					return sourcecache.ProducerResult{}, fmt.Errorf("flux kustomize generator: %w", err)
				}
				if err := memFS.WriteFile(kfile, data); err != nil {
					return sourcecache.ProducerResult{}, fmt.Errorf("write kustomization.yaml: %w", err)
				}
			} else {
				kfile, _ = kustomizationFileIn(memFS, subDir)
			}
			subPath := req.SubPath

			if err := preflightRemoteResources(ctx, cache, memFS, subDir); err != nil {
				return sourcecache.ProducerResult{}, err
			}
			if err := ctx.Err(); err != nil {
				return sourcecache.ProducerResult{}, err
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
					data, readErr := memFS.ReadFile(kfile)
					if readErr != nil {
						return sourcecache.ProducerResult{}, readErr
					}
					if attr := duplicateProducers(memFS, subDir, subPath, data); attr != "" {
						return sourcecache.ProducerResult{}, fmt.Errorf("%w\n%s\nselect an individual overlay with -p", built, attr)
					}
				}
				return sourcecache.ProducerResult{}, built
			}
			// AsYAML converts through JSON, so label scalar spelling MUST be retained first.
			if req.Spec != nil && req.Spec.EmissionPolicy().StringLabels {
				for _, r := range rm.Resources() {
					if err := r.SetLabels(r.GetLabels()); err != nil {
						return sourcecache.ProducerResult{}, fmt.Errorf("%w: normalize labels: %w", manifest.ErrInput, err)
					}
				}
			}
			if err := ctx.Err(); err != nil {
				return sourcecache.ProducerResult{}, err
			}

			out, err := rm.AsYaml()
			if err != nil {
				return sourcecache.ProducerResult{}, fmt.Errorf("kustomize render %s: %w", subPath, err)
			}
			docs, err = manifest.DecodeDocs(bytes.NewReader(out), manifest.RequiredDocuments)
			if err != nil {
				return sourcecache.ProducerResult{}, err
			}
			if manifest.DocumentFailure(docs) != nil {
				return sourcecache.ProducerResult{}, errUncacheableRender
			}
			payload, err := manifest.EncodeRender(docs)
			if err != nil {
				return sourcecache.ProducerResult{}, err
			}
			data, err = encodeFrame(renderHeader{Key: key, Binding: manifest.Fingerprint(prepared.binding), Reads: rec.snapshot()}, payload)
			if err == nil {
				_, err = w.Write(data)
			}
			return sourcecache.ProducerResult{}, err
		})
	}
	if req.HeldSlot != nil {
		req.HeldSlot.YieldSlot(acquire)
	} else {
		acquire()
	}
	if errors.Is(err, errUncacheableRender) {
		return docs, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return docs, receipt, nil
}

type renderRequest struct {
	lower     fs.FS
	root, key string
	binding   renderBinding
}

func (c *TreeCache) prepareRender(ctx context.Context, req RenderRequest) (renderRequest, error) {
	if c == nil {
		return renderRequest{}, fmt.Errorf("%w: incomplete render request", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return renderRequest{}, err
	}
	if req.Source == nil || req.Source.FS == nil {
		return renderRequest{}, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
	}
	lower, root := req.Source.FS, cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source")
	var ignore any
	if req.ApplyIgnore {
		input, err := c.sourceRootFor(ctx, req.Source, root)
		if err != nil {
			return renderRequest{}, err
		}
		lower, ignore = input.fs, input.ignore
	}
	var err error
	root, err = filepath.Abs(root)
	if err != nil {
		return renderRequest{}, fmt.Errorf("%w: source label: %w", manifest.ErrInput, err)
	}
	key := renderKey(req, root, ignore)
	source := manifest.BootstrapSourceID
	if req.Spec != nil {
		source = req.Spec.SourceID()
	}
	return renderRequest{lower: lower, root: root, key: key,
		binding: renderBinding{key, source, req.Source.Kind, req.Source.URL, req.Source.LocalPath, req.Source.LocalRoot, req.Source.Key, req.Source.Revision, req.Source.Digest, req.Source.InputAlias, req.WipeSecrets, req.StrictSubstitutions, slices.Clone(req.SkipKinds), req.DependencySubstitutions},
	}, nil
}

// Mutable files are validated by the read set; immutable generations are part
// of the render content. Filesystem handles and retry/storage policy are never encoded.
func renderKey(req RenderRequest, root string, ignore any) string {
	var immutable any
	if req.Source.Content != "" {
		immutable = req.Source.Input(store.ArtifactContent)
	}
	var spec any
	if req.Spec != nil {
		prepared := *req.Spec
		prepared.KustomizationSpec = req.Spec.KustomizationSpec
		prepared.DependsOn = nil
		spec = prepared.Content(nil)
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

// RenderInputs returns source-relative observed paths and whether each read excludes descendants.
func RenderInputs(receipt *manifest.RenderReceipt) map[string]bool {
	if receipt == nil {
		return nil
	}
	frame := receipt.Value().(renderFrame)
	inputs := make(map[string]bool, len(frame.header.Reads.Files)+len(frame.header.Reads.Dirs)+len(frame.header.Reads.Nodes))
	for _, item := range frame.header.Reads.Files {
		inputs[item.Path] = true
	}
	for _, item := range frame.header.Reads.Nodes {
		inputs[item.Path] = item.Kind == kindFile
	}
	for _, item := range frame.header.Reads.Dirs {
		inputs[item.Path] = false
	}
	return inputs
}
