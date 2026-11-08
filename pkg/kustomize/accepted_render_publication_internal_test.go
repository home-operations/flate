package kustomize

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type acceptedHistoryPublication struct {
	*acceptedHistoryCase

	directory  string
	renderer   *TreeCache
	pending    *manifest.RenderReceipt
	indexPath  string
	initialKey string
}

func (h *acceptedHistoryCase) publication(t *testing.T) {
	t.Helper()

	directory := t.TempDir()
	p := &acceptedHistoryPublication{
		acceptedHistoryCase: h,
		directory:           directory,
		renderer:            newTestTreeCache(t, directory),
		pending:             h.receipt,
		indexPath:           filepath.Join(directory, "oci", "index.json"),
	}
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	p.initialKey = p.storedKey(t)
	p.warm(t)
	p.change(t)
	p.failures(t)
}

func (p *acceptedHistoryPublication) storedKey(t *testing.T) string {
	t.Helper()
	reader, descriptor, _, err := p.renderer.owner.Blob(
		t.Context(),
		p.key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(reader, "content")
	assert.NoError(t, err)
	stored, err := decodeFrame(data)
	assert.NoError(t, err)
	content := descriptor.Annotations["flate.accepted-render"]
	assert.FatalIf(t,
		content == "" || content != manifest.Fingerprint(stored.accepted),
		"%s", "stored accepted-frame key does not identify the actual accepted frame",
	)
	return content
}

func (p *acceptedHistoryPublication) unchanged(t *testing.T) func() {
	t.Helper()
	content, err := os.ReadFile(p.indexPath)
	assert.NoError(t, err)
	stamp := time.Unix(1, 0)
	assert.NoError(t, os.Chtimes(p.indexPath, stamp, stamp))
	before, err := os.Stat(p.indexPath)
	assert.NoError(t, err)
	return func() {
		t.Helper()
		after, err := os.Stat(p.indexPath)
		assert.NoError(t, err)
		current, err := os.ReadFile(p.indexPath)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!bytes.Equal(current, content) || !after.ModTime().Equal(before.ModTime()),
			"%s",
			"unchanged accepted frame republished the OCI index",
		)
	}
}

func (p *acceptedHistoryPublication) warm(t *testing.T) {
	t.Helper()
	_, _, err := testutil.RenderMaps(Render(t.Context(), p.renderer, p.req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	_, p.pending, _ = p.renderer.PreviousRender(t.Context(), p.req)
	resident := p.pending.Value().(renderFrame)
	assert.FatalIf(
		t,
		resident.accepted == nil || resident.acceptedKey == "" || resident.lookup == "",
		"%s",
		"warm receipt did not carry decoded accepted evidence",
	)
	check := p.unchanged(t)
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	check()
	reader, _, _, err := p.renderer.owner.Blob(
		t.Context(),
		p.key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(reader, "content")
	assert.NoError(t, err)
	_, _, _, err = p.renderer.owner.Blob(
		t.Context(),
		p.key,
		&sourcecache.Policy{MaxAge: 0},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, writeErr := w.Write(data)
			return sourcecache.ProducerResult{}, writeErr
		},
	)
	assert.NoError(t, err)

	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	assert.Equal(t, p.storedKey(t), p.initialKey)
	closeTestTreeCache(t, p.renderer)
	p.renderer = newTestTreeCache(t, p.directory)
	assert.Equal(t, p.storedKey(t), p.initialKey)
	check = p.unchanged(t)
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	check()
}

func (p *acceptedHistoryPublication) change(t *testing.T) {
	t.Helper()
	initialIndex, err := os.ReadFile(p.indexPath)
	assert.NoError(t, err)
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: emptyRenderInventory,
		Content: acceptedInput,
	}))
	changedIndex, err := os.ReadFile(p.indexPath)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		p.storedKey(t) == p.initialKey,
		"%s",
		"changed accepted frame retained the previous content key",
	)
	assert.FatalIf(t, bytes.Equal(initialIndex, changedIndex), "%s", "changed accepted frame was not published")
	previous, complete := previousHistory(t.Context(), p.renderer, &p.req, p.owner)
	assert.Equal(t, complete && previous != nil, true)
	for range previous {
		t.Fatal("changed complete-empty frame retained children")
	}
	_, borrowedReceipt, err := testutil.RenderMaps(Render(t.Context(), p.renderer, p.req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	foreign := p.owner
	foreign.Namespace = "foreign"
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   foreign,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	{
		_, previousHistoryComplete := previousHistory(t.Context(), p.renderer, &p.req, p.owner)
		assert.FatalIf(t, previousHistoryComplete, "%s", "foreign overwrite retained the previous owner's history")
	}
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: borrowedReceipt,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	assert.Equal(t, p.storedKey(t), p.initialKey)
	previous, complete = previousHistory(t.Context(), p.renderer, &p.req, p.owner)
	assert.Equal(t, complete, true)
	got := maps.Collect(previous)
	assert.Diff(t, got, p.records)
	check := p.unchanged(t)
	assert.NoError(t, p.renderer.RecordRender(t.Context(), RenderAcceptance{
		Owner:   p.owner,
		Receipt: p.pending,
		Records: maps.All(p.records),
		Content: acceptedInput,
	}))
	check()
}

func (p *acceptedHistoryPublication) failures(t *testing.T) {
	t.Helper()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	check := p.unchanged(t)
	{
		err := p.renderer.RecordRender(canceled, RenderAcceptance{
			Owner:   p.owner,
			Receipt: p.pending,
			Records: maps.All(p.records),
			Content: acceptedInput,
		})
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled same-frame publication: %v", err)
	}
	check()
	check = p.unchanged(t)
	{
		err := p.renderer.RecordRender(t.Context(), RenderAcceptance{
			Owner:   p.owner,
			Receipt: manifest.NewRenderReceipt("invalid", nil),
			Records: maps.All(p.records),
			Content: acceptedInput,
		})
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "failed receipt publication: %v", err)
	}
	check()
	check = p.unchanged(t)
	closeTestTreeCache(t, p.renderer)
	{
		err := p.renderer.RecordRender(t.Context(), RenderAcceptance{
			Owner:   p.owner,
			Receipt: p.pending,
			Records: emptyRenderInventory,
			Content: acceptedInput,
		})
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "closed owner lookup: %v", err)
	}
	check()
	p.renderer = newTestTreeCache(t, p.directory)
	previous, complete := previousHistory(t.Context(), p.renderer, &p.req, p.owner)
	assert.Equal(t, complete, true)
	got := maps.Collect(previous)
	assert.Diff(t, got, p.records)
}
