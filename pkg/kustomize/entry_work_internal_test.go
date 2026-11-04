package kustomize

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreviousEntry_RecordsHistory(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	h := newAcceptedHistoryCase(t)
	assert.NoError(t, h.cache.RecordRender(t.Context(), RenderAcceptance{
		Owner: h.req.Spec.Named(), Receipt: h.receipt, Records: maps.All(h.records), Content: acceptedInput,
	}))
	recorder := &manifest.WorkRecorder{
		Key: manifest.WorkKey{Owner: h.req.Spec.Named(), Source: manifest.BootstrapSourceID},
	}
	scope := recorder.Scope()
	h.req.Work = scope.RenderWork()
	evidence := h.cache.PreviousEntry(t.Context(), h.req)
	assert.Equal(t, evidence.Owner, h.req.Spec.Named())
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
	assert.Equal(t, preparation.OwnerPrepares, uint64(1))
	assert.Equal(t, service, manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1})
}
