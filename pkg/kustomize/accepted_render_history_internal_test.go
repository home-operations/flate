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
	root     string
	cacheDir string
	cache    *TreeCache
	req      RenderRequest
	owner    manifest.NamedResource
	receipt  *manifest.RenderReceipt
	frame    renderFrame
	raw      []map[string]any
	key      sourcecache.BlobKey
	child    manifest.NamedResource
	records  map[manifest.NamedResource]manifest.DiscoveryRecord
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
	frame.header.Binding = manifest.Fingerprint(frame.binding)
	frame.stored, err = sourcecache.NewFrame(frameDomain, frame.header, frame.output)
	assert.NoError(t, err)
	original := bytes.Clone(frame.output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).output, original), true)
	raw, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := sourcecache.BlobKey{Content: frame.header.Key, Render: sourcecache.RenderAccepted}
	child := owner
	child.Name = "child"
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {
		Refs:         []manifest.NamedResource{manifest.BootstrapSourceID},
		ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/red"}},
	}}
	return &acceptedHistoryCase{
		root: root, cacheDir: cacheDir, cache: cache, req: req, owner: owner,
		receipt: receipt, frame: frame, raw: raw, key: key, child: child, records: records,
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

	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner: owner, Receipt: receipt, Records: nil, Content: acceptedInput,
	}))
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: nil,
		Records: emptyRenderInventory,
		Content: acceptedInput,
	}))
	{
		records, complete := previousHistory(t.Context(), cache, &req, owner)
		assert.FatalIf(t, complete || records != nil, "%s", "unaccepted output supplied history")
	}
}

func (h *acceptedHistoryCase) durable(t *testing.T) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	cache, cacheDir, req, owner := h.cache, h.cacheDir, h.req, h.owner
	receipt, records := h.receipt, h.records
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: receipt,
		Records: maps.All(records),
		Content: acceptedInput,
	}))
	fresh := newTestTreeCache(t, cacheDir)
	previous, complete := previousHistory(t.Context(), fresh, &req, owner)
	assert.Equal(t, complete, true)
	got := maps.Collect(previous)
	assert.Diff(t, got, records)
	equal := req
	equal.SkipKinds = slices.Clone(equal.SkipKinds)
	equalPrevious, equalComplete := previousHistory(t.Context(), fresh, &equal, owner)
	assert.Equal(t, equalComplete, true)
	assert.Diff(t, maps.Collect(equalPrevious), records)
	changed := equal
	changed.SkipKinds = []string{manifest.KindCustomResourceDefinition}
	_, changedComplete := previousHistory(t.Context(), fresh, &changed, owner)
	assert.Equal(t, changedComplete, false)
	_, acceptedReceipt, rawHit := fresh.PreviousRender(t.Context(), req)
	assert.Equal(t, rawHit, true)
	{
		_, complete := fresh.RenderHistory(owner, acceptedReceipt, "changed final inputs")
		assert.FatalIf(t, complete, "%s", "raw hit supplied history for different final inputs")
	}
	{
		_, complete := fresh.RenderHistory(owner, receipt, acceptedInput)
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
	assert.NoError(t, publisher.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: pending,
		Records: maps.All(records),
		Content: acceptedInput,
	}))
	reader := newTestTreeCache(t, directory)
	counted.opens.Store(0)
	previous, complete := previousHistory(t.Context(), reader, &input, owner)
	assert.Equal(t, complete && previous != nil, true)
	assert.Diff(t, maps.Collect(previous), records)

	assert.FatalIf(t, counted.opens.Load() == 0, "%s", "enabled history did not prepare source")

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	counted.opens.Store(0)
	{
		previous, complete := previousHistory(canceled, reader, &input, owner)
		assert.FatalIf(t, previous != nil || complete, "%s", "canceled enabled renderer supplied history")
	}
	assert.Equal(t, counted.opens.Load(), 0)
	input.Source = nil
	{
		previous, complete := previousHistory(t.Context(), reader, &input, owner)
		assert.FatalIf(t, previous != nil || complete, "%s", "unavailable source supplied history")
	}
}

func (h *acceptedHistoryCase) requestKey(t *testing.T) {
	t.Helper()

	frame, key, owner, records := h.frame, h.key, h.owner, h.records
	req := h.req
	isolated := newTestTreeCache(t, t.TempDir())
	foreign := frame.header
	foreign.Key = "foreign"
	foreignReceipt, err := testEncodeFrame(t, &foreign, frame.output)
	assert.NoError(t, err)
	foreignFrame, err := testDecodeFrame(t, foreignReceipt)
	assert.NoError(t, err)
	accepted, err := testAcceptedFrame(t, owner, &foreignFrame, maps.All(records))
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
		_, complete := previousHistory(t.Context(), isolated, &req, owner)
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
	assert.NoError(t, isolated.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: current,
		Records: maps.All(records),
		Content: acceptedInput,
	}))
	{
		_, complete := previousHistory(t.Context(), isolated, &mutable, owner)
		assert.FatalIf(t, !complete, "%s", "unchanged mutable history missed")
	}
	before, err := isolated.prepareRender(t.Context(), &mutable)
	assert.NoError(t, err)
	testutil.WriteFile(t, directory, "cm.yaml", cmYAML("after"))
	after, err := isolated.prepareRender(t.Context(), &mutable)
	assert.NoError(t, err)

	assert.FatalIf(t, before.key != after.key, "%s", "mutable content changed the fixed request key")

	{
		_, complete := previousHistory(t.Context(), isolated, &mutable, owner)
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
	assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: pending,
		Records: maps.All(records),
		Content: acceptedInput,
	}))
	before := input
	before.SkipKinds = []string{"Skipped"}
	{
		_, complete := previousHistory(t.Context(), renderer, &before, owner)
		assert.FatalIf(t, !complete, "%s", "caller mutation changed the prepared receipt")
	}
	{
		_, complete := previousHistory(t.Context(), renderer, &input, owner)
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
			assert.NoError(t, isolated.RecordRender(t.Context(), RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Records: maps.All(records),
				Content: acceptedInput,
			}))
			changed := req
			tc.mutate(&changed)
			{
				_, complete := previousHistory(t.Context(), isolated, &changed, owner)
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
		_, complete := previousHistory(t.Context(), fresh, &req, foreign)
		assert.FatalIf(t, complete, "%s", "foreign effective owner borrowed history")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		err := cache.RecordRender(canceled, RenderAcceptance{
			Owner:   owner,
			Receipt: receipt,
			Records: emptyRenderInventory,
			Content: acceptedInput,
		})
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled acceptance: %v", err)
	}
	previous, complete := previousHistory(t.Context(), fresh, &req, owner)
	assert.Equal(t, complete, true)
	for id := range previous {
		assert.Equal(t, id, child)
	}
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner:   owner,
		Receipt: receipt,
		Records: emptyRenderInventory,
		Content: acceptedInput,
	}))
	previous, complete = previousHistory(t.Context(), fresh, &req, owner)
	assert.Equal(t, complete && previous != nil, true)
	for range previous {
		t.Fatal("empty acceptance retained previous records")
	}
}

func (h *acceptedHistoryCase) remoteObservations(t *testing.T, fresh *TreeCache) {
	t.Helper()
	frame, cache, owner, records, req, key := h.frame, h.cache, h.owner, h.records, h.req, h.key
	remote := frame.header
	remote.Reads.Remotes = []remoteEntry{{Input: remoteInput{URL: "https://example.invalid/remote"}}}
	remoteFrame, err := sourcecache.NewFrame(frameDomain, remote, frame.output)
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner: owner,
		Receipt: manifest.NewRenderReceipt(renderFrame{
			header: remote, binding: frame.binding, output: frame.output, stored: remoteFrame,
		}, equalRenderFrame),
		Records: maps.All(records),
		Content: acceptedInput,
	}))
	fresh.SetGitBaseFetcher(
		func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			t.Fatal("history acquired a remote")
			//nolint:nilnil // The fixture explicitly models an absent successful result.
			return nil, nil
		},
	)
	{
		_, complete := previousHistory(t.Context(), fresh, &req, owner)
		assert.FatalIf(t, complete, "%s", "remote observations supplied history")
	}
	reader, _, _, err := cache.owner.Blob(t.Context(), key, &sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	assert.NoError(t, err)
	_, err = fs.ReadFile(reader, "content")
	assert.NoError(t, err)
}
