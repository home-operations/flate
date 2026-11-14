package kustomize

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestPreviousRender_RecordsHistory(t *testing.T) {
	t.Parallel()
	h := newAcceptedHistoryCase(t)
	persistent := h.persistent
	assert.NoError(t, h.cache.RecordRender(t.Context(), RenderAcceptance{
		Owner: h.req.Spec.Named(), Receipt: h.receipt, Records: maps.All(h.records), Content: acceptedInput,
	}))
	recorder := &manifest.WorkRecorder{
		Key: manifest.WorkKey{Owner: h.req.Spec.Named(), Source: manifest.BootstrapSourceID},
	}
	scope := recorder.Scope()
	h.req.Work = scope.RenderWork()
	var cell Preparation
	defer cell.Retire()
	h.req.Preparation = &cell
	_, receipt, available := h.cache.PreviousRender(t.Context(), h.req)
	_, complete := h.cache.RenderHistory(h.req.Spec.Named(), receipt, acceptedInput)
	assert.Equal(t, available && complete, persistent)
	var preparation, service manifest.WorkCounts
	for _, record := range scope.Snapshot(0) {
		assert.Equal(t, record.Key.Owner, h.req.Spec.Named())
		assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		if record.Key.Phase == manifest.WorkPreparation {
			preparation.Add(&record.Counts)
		}
		if record.Key.Phase == manifest.WorkCache {
			service.Add(&record.Counts)
		}
	}
	wantPreparation := uint64(0)
	wantService := manifest.WorkCounts{}
	if persistent {
		wantPreparation = 1
		wantService = manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1}
	}
	assert.Equal(t, preparation.OwnerPrepares, wantPreparation)
	assert.Equal(t, service, wantService)
	later := &manifest.WorkRecorder{Key: recorder.Key}
	h.req.Work = later.Scope().RenderWork()
	h.cache.PreviousRender(t.Context(), h.req)
	_, _, err := Render(t.Context(), h.cache, h.req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	preparation = manifest.WorkCounts{}
	for _, record := range later.Scope().Snapshot(0) {
		if record.Key.Phase == manifest.WorkPreparation {
			preparation.Add(&record.Counts)
		}
	}
	wantPreparation = 1
	if persistent {
		wantPreparation = 0
	}
	assert.Equal(t, preparation.OwnerPrepares, wantPreparation)
	cell.Retire()
	assert.Equal(t, cell.complete || cell.source != nil || cell.spec != nil || cell.lower != nil, false)
}
