package kustomize

import (
	"bytes"
	"cmp"
	"context"
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

// RenderRequest carries the authoritative source and its mount label. Label is
// metadata and need not name an existing host path.
type RenderRequest struct {
	// Work borrows stage counters from this render execution.
	Work *manifest.RenderWork `json:"-"`
	// HeldSlot is TreeCache's borrowed service when this render holds its slot.
	// Standalone callers leave it nil.
	HeldSlot *task.Service `json:"HeldSlot"`
	// Source.FS supplies content; nil Source or FS is unavailable.
	// Empty Source.Content denotes mutable content validated on every cache hit.
	Source *store.SourceArtifact `json:"Source"`
	// ApplyIgnore selects source-controller exclusions for admitted local aliases.
	ApplyIgnore bool `json:"ApplyIgnore"`
	// Label mounts source content in kyaml; empty uses Source.LocalPath.
	Label string `json:"Label"`
	// SubPath selects the source directory to build.
	SubPath string `json:"SubPath"`
	// Spec is the prepared Flux Kustomization; nil builds without a Flux owner.
	Spec *manifest.Kustomization `json:"Spec"`

	// StrictSubstitutions binds undefined-variable handling to accepted history.
	StrictSubstitutions bool `json:"StrictSubstitutions"`
	// SkipKinds binds the effective emitted-kind policy to accepted history.
	SkipKinds []string `json:"SkipKinds"`
	// DependencySubstitutions binds emitted dependency projection to accepted history.
	DependencySubstitutions values.DependencySubstitutions `json:"DependencySubstitutions"`
}

// Render returns unstamped renderer documents from an admitted source and a private overlay.
// Generated files never mutate the source. Cancellation is checked around the
// upstream build, which does not accept a context.
//
//nolint:gocritic // Preserve the existing value API and its snapshot semantics.
func Render(
	ctx context.Context,
	cache *TreeCache,
	req RenderRequest,
	retry sourcecache.RetryConfig,
) ([]manifest.DocOccurrence, *manifest.RenderReceipt, error) {
	prepared, err := cache.prepareRender(ctx, &req)
	if err != nil {
		return nil, nil, err
	}
	prepared.request, prepared.retry = req, retry
	if req.Spec != nil {
		prepared.maxAge = req.Spec.Interval.Duration
	}
	var result renderResult

	blobKey := sourcecache.BlobKey{
		Content:  prepared.key,
		Accepted: prepared.inventory,
		Render:   sourcecache.RenderPending,
		Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
			return prepared.validate(ctx, cache, &result, generation)
		},
	}
	acquire := func() {
		_, _, _, err = cache.owner.Blob(
			ctx,
			blobKey,
			&sourcecache.Policy{MaxAge: sourcecache.Forever, Work: req.Work.Pending(), HistoryWork: req.Work.History()},
			func(ctx context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				return prepared.produce(ctx, cache, &result, w)
			},
		)
	}
	if req.HeldSlot != nil {
		req.HeldSlot.YieldSlot(acquire)
	} else {
		acquire()
	}
	if err != nil {
		return nil, nil, err
	}
	return result.docs, result.receipt, nil
}

type renderRequest struct {
	request   RenderRequest
	retry     sourcecache.RetryConfig
	maxAge    time.Duration
	lower     fs.FS
	root, key string
	inventory string
	binding   renderBinding
}

func (c *TreeCache) prepareRender(ctx context.Context, incoming *RenderRequest) (renderRequest, error) {
	req := *incoming
	work := req.Work.Preparation()
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	if c == nil {
		err := manifest.NewError(manifest.ErrInput, nil, "incomplete render request")
		work.Finish(err)
		return renderRequest{}, err
	}
	if err := ctx.Err(); err != nil {
		work.Finish(err)
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return renderRequest{}, err
	}
	if req.Source == nil || req.Source.FS == nil {
		err := manifest.NewError(manifest.ErrInput, nil, "source artifact content unavailable")
		work.Finish(err)
		return renderRequest{}, err
	}
	lower, root := req.Source.FS, cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source")
	var ignore any
	if req.ApplyIgnore {
		input, err := c.sourceRootFor(ctx, req.Source, root)
		if err != nil {
			work.Finish(err)
			return renderRequest{}, err
		}
		lower, ignore = input.fs, input.ignore
	}
	var err error
	root, err = filepath.Abs(root)
	if err != nil {
		work.Finish(err)
		return renderRequest{}, manifest.NewError(manifest.ErrInput, fmt.Errorf("source label: %w", err), "")
	}
	key := renderKey(&req, root, ignore)
	inventory := key
	if req.Spec != nil && req.Spec.Named().ReportHidden() {
		entry := req
		entry.Spec = manifest.NewEntryKustomization(req.Spec.SourceID(), req.Spec.Name, manifest.NamedResource{})
		entry.SubPath = entry.Spec.Path
		inventory = renderKey(&entry, root, ignore)
	}
	req.Work.Fingerprint(key)
	source := manifest.BootstrapSourceID
	if req.Spec != nil {
		source = req.Spec.SourceID()
	}
	work.Finish(nil)
	return renderRequest{
		lower: lower, root: root, key: key, inventory: inventory,
		binding: renderBinding{
			key,
			source,
			req.Source.Kind,
			req.Source.URL,
			req.Source.LocalPath,
			req.Source.LocalRoot,
			req.Source.Key,
			req.Source.Revision,
			req.Source.Digest,
			req.Source.InputAlias,

			req.StrictSubstitutions,
			slices.Clone(req.SkipKinds),
			req.DependencySubstitutions,
		},
	}, nil
}

// Mutable files are validated by the read set; immutable generations are part
// of the render content. Filesystem handles and retry/storage policy are never encoded.
func renderKey(incoming *RenderRequest, root string, ignore any) string {
	req := *incoming
	var immutable any
	if req.Source.Content != "" {
		immutable = req.Source.Input(store.ArtifactContent)
	}
	var spec any
	if req.Spec != nil {
		spec = req.Spec.Content(nil, nil, false)
	}
	return manifest.Fingerprint(struct {
		Spec        any    `json:"Spec"`
		SourceRoot  string `json:"SourceRoot"`
		SubPath     string `json:"SubPath"`
		ApplyIgnore bool   `json:"ApplyIgnore"`
		Ignore      any    `json:"Ignore"`
		Immutable   any    `json:"Immutable"`
	}{spec, root, req.SubPath, req.ApplyIgnore, ignore, immutable})
}

type sourceRoot struct {
	fs     fs.FS
	ignore any
}

// Source admission belongs to the run owner. Every reader of a source MUST use
// the same view; the checkout MUST remain stable until all readers finish.
func (c *TreeCache) sourceRootFor(
	ctx context.Context,
	artifact *store.SourceArtifact,
	root string,
) (*sourceRoot, error) {
	if err := ctx.Err(); err != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return nil, err
	}
	key := struct {
		source *store.SourceArtifact
		root   string
	}{artifact, root}
	if cached, ok := c.sourceRoots.Load(key); ok {
		//nolint:errcheck // This cache stores only the exact concrete type asserted at this boundary.
		return cached.(func() (*sourceRoot, error))()
	}
	// Shared admission belongs to the cache lifetime, not the first reader's cancellation.
	load := sync.OnceValues(func() (*sourceRoot, error) {
		input, _, err := gittree.New(
			context.Background(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS},
		)
		if err != nil {
			return nil, err
		}
		return &sourceRoot{fs: input, ignore: input.PolicyEvidence()}, nil
	})
	actual, _ := c.sourceRoots.LoadOrStore(key, load)
	//nolint:errcheck // This cache stores only the exact concrete type asserted at this boundary.
	return actual.(func() (*sourceRoot, error))()
}

// RenderInputs returns source-relative observed paths and whether each read excludes descendants.
func RenderInputs(receipt *manifest.RenderReceipt) map[string]bool {
	if receipt == nil {
		return nil
	}
	//nolint:errcheck // This cache stores only the exact concrete type asserted at this boundary.
	frame := receipt.Value().(renderFrame)
	inputs := make(
		map[string]bool,
		len(frame.header.Reads.Files)+len(frame.header.Reads.Dirs)+len(frame.header.Reads.Nodes),
	)
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

type renderResult struct {
	docs    []manifest.DocOccurrence
	receipt *manifest.RenderReceipt
}

func (r *renderRequest) validate(
	ctx context.Context,
	cache *TreeCache,
	result *renderResult,
	generation fs.FS,
) (bool, error) {
	result.receipt = nil
	raw, readFileErr := fs.ReadFile(generation, "content")
	if readFileErr != nil {
		return false, manifest.NewError(
			manifest.ErrFlux,
			fmt.Errorf("read kustomize render cache: %w", readFileErr),
			"",
		)
	}
	frame, readFileErr := decodeFrame(raw)
	if readFileErr != nil || frame.header.Key != r.key {
		slog.Debug("kustomize: decode render frame", "err", readFileErr)
		return false, nil
	}
	valid, readFileErr := frame.header.Reads.stillValid(
		ctx,
		cache,
		&treeFS{lower: r.lower, retry: r.retry, maxAge: r.maxAge, work: r.request.Work.Attribution()},
		r.request.Source.Content != "",
	)
	if readFileErr != nil || !valid {
		return false, readFileErr
	}
	result.docs, readFileErr = manifest.DecodeRender(frame.output)
	if readFileErr != nil {
		//nolint:nilerr // Incomplete render output must reject cache publication without failing the local result.
		return false, nil
	}
	frame.binding, frame.inventory = r.binding, r.inventory
	binding := manifest.Fingerprint(r.binding)
	if frame.header.Binding != binding {
		frame.header.Binding, frame.encoded, frame.raw = binding, nil, ""
		frame.accepted, frame.acceptedKey = nil, ""
	}
	result.receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
	return true, nil
}

func (r *renderRequest) produce(
	ctx context.Context,
	cache *TreeCache,
	result *renderResult,
	w sourcecache.ProducerInput,
) (sourcecache.ProducerResult, error) {
	rec := newReadSet(r.root)
	rec.immutable = r.request.Source.Content != ""
	memFS := newTreeFS(r.lower, r.root, rec)
	memFS.work = r.request.Work.Attribution()
	memFS.retry = r.retry
	memFS.maxAge = r.maxAge
	subDir := r.root + string(filepath.Separator) + strings.TrimLeft(r.request.SubPath, "/")
	canonicalDir, file, cleanedAbsErr := memFS.CleanedAbs(subDir)
	if cleanedAbsErr != nil {
		return sourcecache.ProducerResult{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("kustomization path is not a directory: %s: %w", r.request.SubPath, cleanedAbsErr),
			"",
		)
	}
	if file != "" {
		return sourcecache.ProducerResult{}, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("kustomization path is not a directory: %s", r.request.SubPath),
		)
	}
	subDir = canonicalDir.String()
	kfile, generateErr := r.generateFiles(ctx, memFS, subDir)
	if generateErr != nil {
		return sourcecache.ProducerResult{}, generateErr
	}

	if preflightRemoteResourcesErr := preflightRemoteResources(
		ctx,
		cache,
		memFS,
		subDir,
	); preflightRemoteResourcesErr != nil {
		return sourcecache.ProducerResult{}, preflightRemoteResourcesErr
	}
	result.docs, cleanedAbsErr = r.buildDocuments(ctx, memFS, subDir, kfile)
	if cleanedAbsErr != nil {
		return sourcecache.ProducerResult{}, cleanedAbsErr
	}
	if manifest.DocumentFailure(result.docs) != nil {
		// Incomplete renders MUST fail frame validation to preserve each caller's local output.
		//nolint:nilerr // Incomplete render output must reject cache publication without failing the local result.
		return sourcecache.ProducerResult{}, nil
	}
	payload, cleanedAbsErr := manifest.EncodeRender(result.docs)
	if cleanedAbsErr != nil {
		return sourcecache.ProducerResult{}, cleanedAbsErr
	}
	header := renderHeader{Key: r.key, Binding: manifest.Fingerprint(r.binding), Reads: rec.snapshot()}
	if r.request.Spec != nil && r.request.Spec.Named().ReportHidden() {
		header.EntrySpec = r.request.Spec.Clone()
		header.EntrySpec.EntryReceipt = nil
	}
	data, cleanedAbsErr := encodeFrame(
		&header,
		payload,
	)
	if cleanedAbsErr == nil {
		_, cleanedAbsErr = w.Write(data)
	}
	//nolint:wrapcheck // Cache production must preserve the writer's original error value.
	return sourcecache.ProducerResult{}, cleanedAbsErr
}

func (r *renderRequest) generateFiles(_ context.Context, memFS *treeFS, subDir string) (string, error) {
	var ks map[string]any
	var cleanedAbsErr error
	var data []byte
	var kfile string
	if r.request.Spec != nil {
		ks, cleanedAbsErr = r.request.Spec.RenderDocument()
		if cleanedAbsErr != nil {
			return "", cleanedAbsErr
		}
		generateManifestResult1, generateManifestResult1Err := generateManifest(memFS, subDir, ks)
		data, kfile, cleanedAbsErr = generateManifestResult1.data, generateManifestResult1.path, generateManifestResult1Err
		if cleanedAbsErr != nil {
			return "", fmt.Errorf("flux kustomize generator: %w", cleanedAbsErr)
		}
		if writeFileErr := memFS.WriteFile(kfile, data); writeFileErr != nil {
			return "", fmt.Errorf("write kustomization.yaml: %w", writeFileErr)
		}
	} else {
		kfile, _ = kustomizationFileIn(memFS, subDir)
	}

	return kfile, nil
}

func (r *renderRequest) buildDocuments(
	ctx context.Context,
	memFS *treeFS,
	subDir, kfile string,
) ([]manifest.DocOccurrence, error) {
	subPath := r.request.SubPath
	if errErr := ctx.Err(); errErr != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return nil, errErr
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("kustomize: build", "path", r.request.SubPath)
	}
	work := r.request.Work.Engine(manifest.WorkKustomize, r.key)
	work.Add(&manifest.WorkCounts{Attempts: 1})
	rm, cleanedAbsErr := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		return kustomizestate.Build(memFS, subDir)
	}()
	work.Finish(cleanedAbsErr)
	if cleanedAbsErr != nil {
		built := manifest.NewError(
			manifest.ErrFlux,
			fmt.Errorf("kustomize build %s: %w", subPath, cleanedAbsErr),
			"",
		)
		// Kustomize API v0.21.2 resmap/reswrangler.go Append returns an untyped formatted duplicate-ID error.
		if !strings.Contains(cleanedAbsErr.Error(), "already registered id") {
			return nil, built
		}

		readFileData, readErr := memFS.ReadFile(kfile)
		if readErr != nil {
			return nil, readErr
		}
		if attr := duplicateProducers(memFS, subDir, subPath, readFileData); attr != "" {
			return nil, fmt.Errorf(
				"%w\n%s\nselect an individual overlay with -p",
				built,
				attr,
			)
		}
		return nil, built
	}
	// AsYAML converts through JSON, so label scalar spelling MUST be retained first.
	if r.request.Spec != nil && r.request.Spec.EmissionPolicy().StringLabels {
		if err := normalizeRenderedLabels(rm); err != nil {
			return nil, err
		}
	}
	if errErr2 := ctx.Err(); errErr2 != nil {
		//nolint:wrapcheck // Cancellation must preserve the caller-visible sentinel identity.
		return nil, errErr2
	}

	out, cleanedAbsErr := rm.AsYaml()
	if cleanedAbsErr != nil {
		return nil, fmt.Errorf("kustomize render %s: %w", subPath, cleanedAbsErr)
	}
	docs, cleanedAbsErr := manifest.DecodeDocs(bytes.NewReader(out), manifest.RequiredDocuments)
	if cleanedAbsErr != nil {
		return nil, cleanedAbsErr
	}
	return docs, nil
}

func normalizeRenderedLabels(rm resmap.ResMap) error {
	for _, r := range rm.Resources() {
		if setLabelsErr := r.SetLabels(r.GetLabels()); setLabelsErr != nil {
			return manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("normalize labels: %w", setLabelsErr),
				"",
			)
		}
	}
	return nil
}
