package kustomize

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestPreviousEntry_EffectiveLiteral(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("original"),
		"generator.yaml":     "application input",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	if !cache.owner.CanPersistRenders() {
		t.Fatal("receipt witness requires persistence")
	}
	spec := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	spec.TargetNamespace = "effective"
	request := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	application := map[string]any{"kind": "FluxInstance", "metadata": map[string]any{"name": "application"}}
	entry, err := cache.ApplicationReceipt(t.Context(), request.Source, EntryEvidence{
		Application: application, Owner: spec.Named(), Records: emptyRenderInventory,
	}, []string{"generator.yaml"})
	assert.NoError(t, err)
	docs, receipt, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{
		Owner:   spec.Named(),
		Receipt: receipt,
		Entry:   entry,
		Records: emptyRenderInventory,
		Content: request.Content(docs),
	}))
	assert.NoError(t, cache.owner.Close())
	cache = newTestTreeCache(t, cacheDir)
	query := request
	query.Spec = manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	evidence := cache.PreviousEntry(t.Context(), query)
	assert.Diff(t, evidence.Application, application)
	assert.Equal(t, evidence.Receipt != nil, true)
	_, _, stale := cache.PreviousRender(t.Context(), query)
	assert.Equal(t, stale, false)
	_, _, accepted := cache.PreviousRender(t.Context(), request)
	assert.Equal(t, accepted, true)
	changed := request
	changed.Spec = request.Spec.Clone()
	changed.Spec.TargetNamespace = "other"
	_, _, reusedContext := cache.PreviousRender(t.Context(), changed)
	assert.Equal(t, reusedContext, false)
	for _, kind := range []string{"policy", "source"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			changed := query
			if kind == "policy" {
				changed.StrictSubstitutions = true
			} else {
				source := *query.Source
				source.Revision = "other"
				changed.Source = &source
			}
			assert.Equal(t, cache.PreviousEntry(t.Context(), changed).Receipt == nil, true)
		})
	}
	recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Owner: spec.Named(), Source: manifest.BootstrapSourceID}}
	scope := recorder.Scope()
	request.Work = scope.RenderWork()
	warm, _, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Diff(t, warm, docs)
	var builds uint64
	for _, record := range scope.Snapshot(0) {
		if record.Key.Phase == manifest.WorkKustomize {
			builds += record.Counts.Attempts
		}
	}
	assert.Equal(t, builds, uint64(0))
}
