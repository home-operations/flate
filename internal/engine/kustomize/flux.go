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
	"time"

	"sigs.k8s.io/kustomize/api/resmap"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	kustomizestate "github.com/home-operations/flate/internal/engine/kustomize/krusty"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/pkg/manifest"
)

// RenderRequest carries the authoritative source and its mount label. Label is
// metadata and need not name an existing host path.
type RenderRequest struct {
	Preparation      *Preparation      `json:"-"`
	EntryOccurrences []EntryOccurrence `json:"-"`
	LocalInputs      *LocalInputs      `json:"-"`
	// Work borrows stage counters from this render execution.
	Work *manifest.RenderWork `json:"-"`
	// HeldSlot is TreeCache's borrowed service when this render holds its slot.
	// Standalone callers leave it nil.
	HeldSlot *task.Service `json:"HeldSlot"`
	// Source.FS supplies content; nil Source or FS is unavailable.
	// Empty Source.Content denotes mutable content validated on every cache hit.
	Source *store.SourceArtifact `json:"Source"`
	// Label mounts source content in kyaml; empty uses Source.LocalPath.
	Label string `json:"Label"`
	// SubPath selects the source directory to build.
	SubPath string `json:"SubPath"`
	// Spec is the prepared Flux Kustomization; nil builds without a Flux owner.
	Spec                        *manifest.Kustomization       `json:"Spec"`
	UncertainSubstitutionInputs bool                          `json:"UncertainSubstitutionInputs"`
	UnreadableSubstitutions     map[string]manifest.Reference `json:"-"`

	// StrictSubstitutions binds undefined-variable handling to accepted history.
	StrictSubstitutions bool `json:"StrictSubstitutions"`
	// SkipKinds binds the effective emitted-kind policy to accepted history.
	SkipKinds []string `json:"SkipKinds"`
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
		Content: prepared.key,
		Render:  sourcecache.RenderPending,
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

// Preparation MUST remain owned by one logical render consumer until transfer or retirement.
type Preparation struct {
	decoded        *decodedRender
	shaping        *preparedShaping
	cache          *sourcecache.Cache
	spec           *manifest.Kustomization
	source         *store.SourceArtifact
	label, subPath string
	policyKey      string
	complete       bool
	lower          fs.FS
	root, key      string
	binding        sourcecache.RenderBinding[renderPolicy]
}

// Matches MUST NOT grant publication authority or validate current source bytes.
func (p *Preparation) Matches(cache *TreeCache, request *RenderRequest) bool {
	if p == nil || !p.complete {
		return false
	}
	owner := samePreparationDeclaration(p.spec, request.Spec)
	policy := p.binding.Policy.Strict == request.StrictSubstitutions &&
		slices.Equal(p.binding.Policy.Skip, request.SkipKinds)
	location := p.label == request.Label && p.subPath == request.SubPath
	if request.Source != nil {
		location = location && p.policyKey == gittree.PolicyKey(inputprojection.Authored(request.Source).FS)
	}
	return cache != nil && p.cache == cache.owner && owner && policy && location
}

func samePreparationDeclaration(previous, current *manifest.Kustomization) bool {
	if manifest.Equal(previous, current) {
		return true
	}
	if previous == nil || current == nil ||
		!manifest.LabelsEqual(previous, current, [2]map[string]any{}, manifest.IsOwnershipLabel) {
		return false
	}
	declaration := *current
	declaration.Labels = previous.Labels
	return manifest.Equal(previous, &declaration)
}

type renderRequest struct {
	Preparation

	request RenderRequest
	retry   sourcecache.RetryConfig
	maxAge  time.Duration
}

func (c *TreeCache) prepareRender(ctx context.Context, req *RenderRequest) (renderRequest, error) {
	work := req.Work.Preparation()
	var err error
	switch {
	case c == nil:
		err = manifest.NewError(manifest.ErrInput, nil, "incomplete render request")
	case ctx.Err() != nil:
		err = ctx.Err()
	case req.Source == nil || req.Source.FS == nil:
		err = manifest.NewError(manifest.ErrInput, nil, "source artifact content unavailable")
	}
	if err != nil {
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(err)
		return renderRequest{}, err
	}
	lower := req.Source.FS
	root, err := renderRoot(req)
	policyKey := gittree.PolicyKey(inputprojection.Authored(req.Source).FS)
	if err != nil {
		work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
		work.Finish(err)
		return renderRequest{}, manifest.NewError(manifest.ErrInput, err, "")
	}
	previous, key := req.Preparation, ""
	if previous.Matches(c, req) && previous.root == root && previous.policyKey == policyKey {
		if policyKey != "" && previous.source.Content == "" && req.Source.Content == "" ||
			previous.source.SameMaterial(
				req.Source,
				store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
			) {
			key = previous.key
		}
	}
	if key != "" && key == previous.key {
		req.Work.Fingerprint(key)
		previous.lower = lower
		previous.bindSource(req)
		return renderRequest{Preparation: *previous}, nil
	}
	if req.Preparation != nil {
		*req.Preparation = Preparation{}
	}
	work.Add(&manifest.WorkCounts{Attempts: 1, OwnerPrepares: 1})
	if key == "" {
		key = renderKey(req, root, policyKey)
	}
	req.Work.Fingerprint(key)
	work.Finish(nil)
	prepared := Preparation{
		cache: c.owner, label: req.Label, subPath: req.SubPath,
		policyKey: policyKey,
		lower:     lower, root: root, key: key,
	}
	prepared.bindSource(req)
	if req.Preparation != nil {
		*req.Preparation = prepared
	}
	return renderRequest{Preparation: prepared}, nil
}

func renderRoot(req *RenderRequest) (string, error) {
	root, err := filepath.Abs(cmp.Or(req.Label, req.Source.LocalPath, string(filepath.Separator)+"flate-source"))
	if err != nil {
		return "", fmt.Errorf("source label: %w", err)
	}
	return root, nil
}

func (p *Preparation) bindSource(req *RenderRequest) {
	source := manifest.BootstrapSourceID
	if req.Spec != nil {
		source = req.Spec.EffectiveSourceID()
	}
	skip := p.binding.Policy.Skip
	if !p.complete {
		skip = slices.Clone(req.SkipKinds)
	}
	p.binding.Policy = renderPolicy{Strict: req.StrictSubstitutions, Skip: skip}
	p.binding.Refresh(p.key, source, req.Source)
	p.spec, p.source = req.Spec, req.Source
	p.complete = true
}

// Mutable files are validated by the read set; immutable generations are part
// of the render content. Filesystem handles and retry/storage policy are never encoded.
func renderKey(incoming *RenderRequest, root, policyKey string) string {
	req := *incoming
	if req.Label == "" && req.Source.WorkingTree() && req.Source.Extent == store.ExtentInput {
		root = req.Source.OriginPath(root)
	}
	var immutable any
	if req.Source.Content != "" {
		immutable = req.Source.Input(store.ArtifactContent)
	}
	var spec any
	if req.Spec != nil {
		spec = req.Spec.Content(nil, nil, false)
	}
	return manifest.Fingerprint(struct {
		Spec       any    `json:"Spec"`
		SourceRoot string `json:"SourceRoot"`
		SubPath    string `json:"SubPath"`
		Policy     string `json:"Policy"`
		Immutable  any    `json:"Immutable"`
	}{spec, root, req.SubPath, policyKey, immutable})
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
		len(frame.Header.Reads.Files)+len(frame.Header.Reads.Dirs)+len(frame.Header.Reads.Nodes),
	)
	for _, item := range frame.Header.Reads.Files {
		inputs[item.Path] = true
	}
	for _, item := range frame.Header.Reads.Nodes {
		inputs[item.Path] = item.Kind == kindFile
	}
	for _, item := range frame.Header.Reads.Dirs {
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
	frame, decoded, readFileErr := r.request.Preparation.readFrame(ctx, generation)
	if readFileErr != nil || !decoded {
		return false, readFileErr
	}
	read := func(header renderHeader) (bool, error) {
		return header.Reads.stillValid(
			ctx,
			cache,
			&treeFS{lower: r.lower, retry: r.retry, maxAge: r.maxAge, work: r.request.Work.Attribution()},
			r.request.Source.Content != "",
		)
	}
	validated, err := renderDomain.Validate(ctx, &frame, &r.binding, sourcecache.RenderCheck[renderHeader]{
		Header: read,
	})
	if err != nil || !validated.Valid {
		return false, err
	}
	result.docs = validated.Documents
	result.receipt = renderDomain.Receipt(frame)
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
	files, err := prepareFilesAndPreflight(ctx, &r.request, memFS, cache)
	if err != nil {
		return sourcecache.ProducerResult{}, err
	}
	var cleanedAbsErr error
	result.docs, cleanedAbsErr = r.buildDocuments(ctx, memFS, files, false)
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
	frame := renderFrame{
		Key: r.key, Binding: r.binding.Fingerprint(), Output: payload,
		Header: renderHeader{Reads: rec.snapshot()},
	}
	return sourcecache.ProducerResult{}, renderDomain.WritePending(ctx, w, &frame)
}

type buildFiles struct {
	control, dir string
}

func prepareFilesAndPreflight(
	ctx context.Context, req *RenderRequest, memFS *treeFS, cache *TreeCache,
) (buildFiles, error) {
	if inputs := req.LocalInputs; inputs.matches(req, memFS.root) {
		if err := writeGeneratedManifest(memFS, &inputs.generated); err != nil {
			return buildFiles{}, err
		}
		w := inputWalk{cache: cache, fs: memFS, parsed: inputs.controls}
		return buildFiles{control: inputs.generated.path, dir: inputs.dir}, w.preflight(ctx, inputs.dir)
	}
	dir, err := buildDirectory(memFS, req.SubPath)
	if err != nil {
		return buildFiles{}, err
	}
	generated, err := generateFiles(req.Spec, memFS, dir)
	if err != nil {
		return buildFiles{}, err
	}
	return buildFiles{control: generated.path, dir: dir}, preflightRemoteResources(ctx, cache, memFS, dir)
}

func buildDirectory(memFS *treeFS, subPath string) (string, error) {
	dir := memFS.root + string(filepath.Separator) + strings.TrimLeft(subPath, "/")
	canonical, file, err := memFS.CleanedAbs(dir)
	if err != nil {
		return "", manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("kustomization path is not a directory: %s: %w", subPath, err),
			"",
		)
	}
	if file != "" {
		return "", manifest.NewError(
			manifest.ErrInput, nil, fmt.Sprintf("kustomization path is not a directory: %s", subPath),
		)
	}
	return canonical.String(), nil
}

func (r *renderRequest) buildDocuments(
	ctx context.Context,
	memFS *treeFS,
	files buildFiles,
	inputOrder bool,
) ([]manifest.DocOccurrence, error) {
	subPath := r.request.SubPath
	if errErr := ctx.Err(); errErr != nil {
		return nil, errErr
	}
	if !inputOrder && slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("kustomize: build", "path", subPath)
	}
	work := r.request.Work.Engine(manifest.WorkKustomize, r.key)
	work.Add(&manifest.WorkCounts{Attempts: 1})
	rm, cleanedAbsErr := func() (resmap.ResMap, error) {
		kustomizestate.BuildMutex.Lock()
		defer kustomizestate.BuildMutex.Unlock()
		return kustomizestate.Build(memFS, files.dir, inputOrder)
	}()
	work.Finish(cleanedAbsErr)
	if cleanedAbsErr != nil {
		cause := fmt.Errorf("kustomize build %s: %w", subPath, cleanedAbsErr)
		message := ""
		source := r.request.Source
		if origin := source.OriginPath(source.LocalPath); source.LocalPath != "" && origin != source.LocalPath {
			message = strings.ReplaceAll(
				cause.Error(),
				source.LocalPath+string(filepath.Separator),
				origin+string(filepath.Separator),
			)
		}
		built := manifest.NewError(manifest.ErrFlux, cause, message)
		if !isDuplicateIDError(cleanedAbsErr) {
			return nil, built
		}

		readFileData, readErr := memFS.ReadFile(files.control)
		if readErr != nil {
			return nil, readErr
		}
		if attr := duplicateProducers(memFS, files.dir, subPath, readFileData); attr != "" {
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

func isDuplicateIDError(err error) bool {
	// Kustomize's resWrangler.Append returns an untyped duplicate-ID error.
	return strings.Contains(err.Error(), "already registered id")
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
