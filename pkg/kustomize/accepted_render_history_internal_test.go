package kustomize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type historyCountingFS struct {
	fs.FS

	opens atomic.Int64
}

func (f *historyCountingFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

type acceptedHistoryCase struct {
	persistent bool
	root       string
	cacheDir   string
	cache      *TreeCache
	req        RenderRequest
	owner      manifest.NamedResource
	receipt    *manifest.RenderReceipt
	frame      renderFrame
	raw        []map[string]any
	key        sourcecache.BlobKey
	child      manifest.NamedResource
	records    map[manifest.NamedResource]manifest.DiscoveryRecord
	content    any
}

func TestRenderFlux_AcceptedHistory(t *testing.T) {
	t.Parallel()
	h := newAcceptedHistoryCase(t)
	t.Run("disabled preparation", func(t *testing.T) {
		t.Parallel()
		h.disabledPreparation(t)
	})
	h.unaccepted(t)
	t.Run("durable", func(t *testing.T) {
		t.Parallel()
		h.durable(t)
	})
}

func newAcceptedHistoryCase(t *testing.T) *acceptedHistoryCase {
	t.Helper()
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("original")},
	)
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	spec := &manifest.Kustomization{Name: "emitter", Path: "."}
	req := RenderRequest{
		Source:    localSource(t, root),
		Spec:      spec,
		SubPath:   ".",
		SkipKinds: []string{manifest.KindCustomResourceDefinition, manifest.KindSecret},
	}
	content := PrepareShape(&req, nil).Content()
	owner := spec.Named()
	owner.Namespace = "inherited"
	var absent *TreeCache
	{
		previous, _, complete := absent.PreviousRender(t.Context(), req)
		assert.FatalIf(t, previous != nil || complete, "%s", "nil renderer supplied history")
	}
	docs, receipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)

	assert.FatalIf(t, receipt == nil, "%s", "successful raw render supplied no receipt")

	frame := receipt.Value().(renderFrame)
	frame.Binding = manifest.Fingerprint(frame.binding)
	frame.Stored, err = frame.Encode(frameDomain, frame.header)
	assert.NoError(t, err)
	original := bytes.Clone(frame.Output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).Output, original), true)
	raw, err := testutil.DecodeRender(frame.Output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := sourcecache.BlobKey{Content: frame.Key, Render: sourcecache.RenderAccepted}
	child := owner
	child.Name = "child"
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {
		Refs:         []manifest.NamedResource{manifest.BootstrapSourceID},
		ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/red"}},
	}}
	return &acceptedHistoryCase{
		persistent: sourcecache.RenderPersistenceDisabledReason() == "",
		root:       root, cacheDir: cacheDir, cache: cache, req: req, owner: owner,
		receipt: receipt, frame: frame, raw: raw, key: key, child: child, records: records, content: content,
	}
}

func (h *acceptedHistoryCase) disabledPreparation(t *testing.T) {
	t.Helper()

	root, req := h.root, h.req
	counted := &historyCountingFS{FS: os.DirFS(root)}
	artifact := *req.Source
	artifact.FS = counted
	input := req
	input.Source, input.ApplyIgnore = &artifact, true
	fresh := newTestTreeCache(t, "")
	counted.opens.Store(0)
	{
		previous, _, complete := fresh.PreviousRender(t.Context(), input)
		assert.FatalIf(t, previous != nil || complete, "%s", "disabled renderer supplied history")
	}
	{
		count := counted.opens.Load()
		assert.FatalIf(t, count != 0, "disabled history prepared source: opens=%d", count)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		previous, _, complete := fresh.PreviousRender(canceled, input)
		assert.FatalIf(t, previous != nil || complete, "%s", "canceled disabled renderer supplied history")
	}
	assert.Equal(t, counted.opens.Load(), 0)
}

func (h *acceptedHistoryCase) unaccepted(t *testing.T) {
	t.Helper()
	cache, req, owner := h.cache, h.req, h.owner
	key, receipt := h.key, h.receipt
	_, _, _, err := cache.owner.Blob(t.Context(), key, &sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)

	assert.FatalIf(t, !errors.Is(err, errdef.ErrNotFound), "standalone render wrote durable output: %v", err)

	assert.NoError(t, cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner, Receipt: receipt, Records: nil, Content: h.content,
	}))
	assert.NoError(t, cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: nil,
		Records: emptyRenderInventory,
		Content: h.content,
	}))
	{
		records, complete := cache.previousHistory(t.Context(), &req, owner, h.content)
		assert.FatalIf(t, complete || records != nil, "%s", "unaccepted output supplied history")
	}
}

func (h *acceptedHistoryCase) durable(t *testing.T) {
	t.Helper()

	cache, cacheDir, req, owner := h.cache, h.cacheDir, h.req, h.owner
	receipt, records := h.receipt, h.records
	assert.NoError(t, cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: receipt,
		Records: maps.All(records),
		Content: h.content,
	}))
	fresh := newTestTreeCache(t, cacheDir)
	previous, complete := fresh.previousHistory(t.Context(), &req, owner, h.content)
	assert.Equal(t, complete, h.persistent)
	assert.Equal(t, previous != nil, h.persistent)
	if h.persistent {
		assert.Diff(t, maps.Collect(previous), records)
	}
	equal := req
	equal.SkipKinds = slices.Clone(equal.SkipKinds)
	equalPrevious, equalComplete := fresh.previousHistory(t.Context(), &equal, owner, h.content)
	assert.Equal(t, equalComplete, h.persistent)
	assert.Equal(t, equalPrevious != nil, h.persistent)
	if h.persistent {
		assert.Diff(t, maps.Collect(equalPrevious), records)
	}
	changed := equal
	changed.SkipKinds = []string{manifest.KindCustomResourceDefinition}
	_, changedComplete := fresh.previousHistory(t.Context(), &changed, owner, h.content)
	assert.Equal(t, changedComplete, false)
	_, acceptedReceipt, rawHit := fresh.PreviousRender(t.Context(), req)
	assert.Equal(t, rawHit, h.persistent)
	{
		_, complete := fresh.RenderHistory(owner, acceptedReceipt, "changed final inputs")
		assert.FatalIf(t, complete, "%s", "raw hit supplied history for different final inputs")
	}
	{
		_, complete := fresh.RenderHistory(owner, receipt, h.content)
		assert.FatalIf(t, complete, "%s", "pending receipt supplied accepted history")
	}
	t.Run("preparation", h.preparation)
	t.Run("request key", h.requestKey)
	t.Run("mutable history reads", h.mutableHistoryReads)
	t.Run("frozen preparation", h.frozenPreparation)
	t.Run("publication", h.publication)
	h.incompatiblePolicies(t)
	h.emptyAcceptance(t, fresh)
	h.remoteObservations(t, fresh)
}

func (h *acceptedHistoryCase) preparation(t *testing.T) {
	t.Helper()

	root, req, owner, records := h.root, h.req, h.owner, h.records
	counted := &historyCountingFS{FS: os.DirFS(root)}
	artifact := *req.Source
	artifact.FS = counted
	input := req
	input.Source, input.ApplyIgnore = &artifact, true
	directory := t.TempDir()
	publisher := newTestTreeCache(t, directory)
	_, pending, err := Render(t.Context(), publisher, input, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(t, publisher.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: pending,
		Records: maps.All(records),
		Content: h.content,
	}))
	reader := newTestTreeCache(t, directory)
	counted.opens.Store(0)
	previous, complete := reader.previousHistory(t.Context(), &input, owner, h.content)
	assert.Equal(t, complete, h.persistent)
	assert.Equal(t, previous != nil, h.persistent)
	if h.persistent {
		assert.Diff(t, maps.Collect(previous), records)
		assert.FatalIf(t, counted.opens.Load() == 0, "%s", "enabled history did not prepare source")
	} else {
		assert.Equal(t, counted.opens.Load(), int64(0))
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	counted.opens.Store(0)
	{
		previous, complete := reader.previousHistory(canceled, &input, owner, h.content)
		assert.FatalIf(t, previous != nil || complete, "%s", "canceled enabled renderer supplied history")
	}
	assert.Equal(t, counted.opens.Load(), 0)
	input.Source = nil
	{
		previous, complete := reader.previousHistory(t.Context(), &input, owner, h.content)
		assert.FatalIf(t, previous != nil || complete, "%s", "unavailable source supplied history")
	}
}

func (h *acceptedHistoryCase) requestKey(t *testing.T) {
	t.Helper()

	frame, key, owner, records := h.frame, h.key, h.owner, h.records
	req := h.req
	isolated := newTestTreeCache(t, t.TempDir())
	foreign := frame
	foreign.Key, foreign.Stored = "foreign", nil
	foreignReceipt, err := testEncodeFrame(t, &foreign)
	assert.NoError(t, err)
	foreignFrame, err := testDecodeFrame(t, foreignReceipt)
	assert.NoError(t, err)
	accepted, err := foreignFrame.testAcceptedFrame(t, owner, maps.All(records), h.content)
	assert.NoError(t, err)
	_, _, _, err = isolated.owner.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{MaxAge: 0},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, copyErr := io.Copy(w, bytes.NewReader(accepted))
			return sourcecache.ProducerResult{}, copyErr
		},
	)
	assert.NoError(t, err)

	{
		_, complete := isolated.previousHistory(t.Context(), &req, owner, h.content)
		assert.FatalIf(t, complete, "%s", "foreign request key supplied accepted history")
	}
}

func (h *acceptedHistoryCase) mutableHistoryReads(t *testing.T) {
	t.Helper()

	req, owner, records := h.req, h.owner, h.records
	directory := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("before"),
	})
	mutable := req
	mutable.Source = localSource(t, directory)
	isolated := newTestTreeCache(t, t.TempDir())
	_, current, err := testutil.RenderMaps(Render(t.Context(), isolated, mutable, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.NoError(t, isolated.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: current,
		Records: maps.All(records),
		Content: h.content,
	}))
	{
		_, complete := isolated.previousHistory(t.Context(), &mutable, owner, h.content)
		assert.Equal(t, complete, h.persistent)
	}
	before, err := isolated.prepareRender(t.Context(), &mutable)
	assert.NoError(t, err)
	testutil.WriteFile(t, directory, "cm.yaml", cmYAML("after"))
	after, err := isolated.prepareRender(t.Context(), &mutable)
	assert.NoError(t, err)

	assert.FatalIf(t, before.key != after.key, "%s", "mutable content changed the fixed request key")

	{
		_, complete := isolated.previousHistory(t.Context(), &mutable, owner, h.content)
		assert.FatalIf(t, complete, "%s", "changed mutable read supplied accepted history")
	}
}

func (h *acceptedHistoryCase) frozenPreparation(t *testing.T) {
	t.Helper()

	req, owner, records := h.req, h.owner, h.records
	renderer := newTestTreeCache(t, t.TempDir())
	input := req
	input.SkipKinds = []string{"Skipped"}
	_, pending, err := testutil.RenderMaps(Render(t.Context(), renderer, input, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	input.SkipKinds[0] = "Different"
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: pending,
		Records: maps.All(records),
		Content: h.content,
	}))
	before := input
	before.SkipKinds = []string{"Skipped"}
	{
		_, complete := renderer.previousHistory(t.Context(), &before, owner, h.content)
		assert.Equal(t, complete, h.persistent)
	}
	{
		_, complete := renderer.previousHistory(t.Context(), &input, owner, h.content)
		assert.FatalIf(t, complete, "%s", "mutated policy borrowed the prepared receipt")
	}
}

func (h *acceptedHistoryCase) incompatiblePolicies(t *testing.T) {
	t.Helper()
	owner, receipt, records, req, raw := h.owner, h.receipt, h.records, h.req, h.raw
	for _, tc := range []struct {
		name   string
		mutate func(*RenderRequest)
	}{
		{"effective owner", func(r *RenderRequest) {
			r.Spec = r.Spec.Clone()
			r.Spec.Name = "effective-owner"
		}},

		{"strict", func(r *RenderRequest) { r.StrictSubstitutions = true }},
		{"skip kinds", func(r *RenderRequest) { r.SkipKinds = []string{manifest.KindKustomization} }},
		{"source binding", func(r *RenderRequest) { source := *r.Source; source.URL = "different"; r.Source = &source }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			isolated := newTestTreeCache(t, t.TempDir())
			assert.NoError(t, isolated.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
				Sources: []manifest.SourceEvidence{},
				Owner:   owner,
				Receipt: receipt,
				Records: maps.All(records),
				Content: h.content,
			}))
			changed := req
			tc.mutate(&changed)
			{
				_, complete := isolated.previousHistory(t.Context(), &changed, owner, h.content)
				assert.FatalIf(t, complete, "%s", "incompatible policy or source borrowed history")
			}
			out, _, err := testutil.RenderMaps(Render(t.Context(), isolated, changed, sourcecache.RetryConfig{}))
			assert.NoError(t, err)
			assert.Diff(t, out, raw)
		})
	}
}

func (h *acceptedHistoryCase) emptyAcceptance(t *testing.T, fresh *TreeCache) {
	t.Helper()
	cache, req, owner, receipt, child := h.cache, h.req, h.owner, h.receipt, h.child
	foreign := owner
	foreign.Namespace = "foreign"
	{
		_, complete := fresh.previousHistory(t.Context(), &req, foreign, h.content)
		assert.FatalIf(t, complete, "%s", "foreign effective owner borrowed history")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		err := cache.owner.RecordRender(canceled, sourcecache.RenderAcceptance{
			Sources: []manifest.SourceEvidence{},
			Owner:   owner,
			Receipt: receipt,
			Records: emptyRenderInventory,
			Content: h.content,
		})
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled acceptance: %v", err)
	}
	previous, complete := fresh.previousHistory(t.Context(), &req, owner, h.content)
	assert.Equal(t, complete, h.persistent)
	if previous != nil {
		for id := range previous {
			assert.Equal(t, id, child)
		}
	}
	assert.NoError(t, cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: receipt,
		Records: emptyRenderInventory,
		Content: h.content,
	}))
	previous, complete = fresh.previousHistory(t.Context(), &req, owner, h.content)
	assert.Equal(t, complete, h.persistent)
	assert.Equal(t, previous != nil, h.persistent)
	if previous != nil {
		for range previous {
			t.Fatal("empty acceptance retained previous records")
		}
	}
}

func (h *acceptedHistoryCase) remoteObservations(t *testing.T, fresh *TreeCache) {
	t.Helper()
	frame, cache, owner, records, req, key := h.frame, h.cache, h.owner, h.records, h.req, h.key
	remote := frame.header
	remote.Reads.Remotes = []remoteEntry{{Input: remoteInput{URL: "https://example.invalid/remote"}}}
	shared := frame.RenderFrame
	shared.Stored = nil
	remoteFrame, err := shared.Encode(frameDomain, remote)
	assert.NoError(t, err)
	assert.NoError(t, cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
		Sources: []manifest.SourceEvidence{},
		Owner:   owner,
		Receipt: manifest.NewRenderReceipt(renderFrame{
			Key:     frame.Key,
			Binding: frame.Binding,
			Output:  frame.Output,
			Stored:  remoteFrame,
			header:  remote,
			binding: frame.binding,
		}, equalRenderFrame),
		Records: maps.All(records),
		Content: h.content,
	}))
	fresh.SetGitBaseFetcher(
		func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			t.Fatal("history acquired a remote")
			//nolint:nilnil // History must reject remote observations before this fetcher can return an artifact.
			return nil, nil
		},
	)
	{
		_, complete := fresh.previousHistory(t.Context(), &req, owner, h.content)
		assert.FatalIf(t, complete, "%s", "remote observations supplied history")
	}
	reader, _, _, err := cache.owner.Blob(t.Context(), key, &sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if h.persistent {
		assert.NoError(t, err)
		_, err = fs.ReadFile(reader, "content")
		assert.NoError(t, err)
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, reader == nil, true)
	}
}
