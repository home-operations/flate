package kustomize

import (
	"maps"
	"testing"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_RenderAddresses(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	h := newAcceptedHistoryCase(t)
	renderer := newTestTreeCache(t, t.TempDir())
	assert.FatalIf(t, !renderer.owner.CanPersistRenders(), "%s", "disk cache must persist renders")
	recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Owner: h.owner}}
	scope := recorder.Scope()
	req := h.req
	req.Preparation = new(Preparation)
	req.Work = scope.RenderWork()
	docs, receipt, err := Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	frame := receipt.Value().(renderFrame)
	publication := (&manifest.WorkRecorder{Key: manifest.WorkKey{Owner: h.owner}}).Scope()
	acceptance := sourcecache.RenderAcceptance{
		Owner: h.owner, Receipt: receipt, Records: maps.All(h.records), Content: h.content,
		Sources: []manifest.SourceEvidence{}, Work: publication.RenderWork(),
	}
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
	addresses := kustomizeRenderAddresses(t, renderer.owner, frame.Key)
	for _, address := range addresses {
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
		assert.Equal(t, publication.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(0))
	}

	replacement := map[manifest.NamedResource]manifest.DiscoveryRecord{h.child: {File: "replacement.yaml"}}
	acceptance.Records = maps.All(replacement)
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
	repeated, previous, available := renderer.PreviousRender(t.Context(), req)
	assert.FatalIf(t, !available, "%s", "replacement raw output missing")
	assert.Diff(t, repeated, docs)
	history, complete := renderer.RenderHistory(h.owner, previous, h.content)
	assert.FatalIf(t, !complete, "%s", "replacement inventory missing")
	assert.Diff(t, maps.Collect(history), replacement)
	for _, address := range addresses {
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
	}

	changed := req
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("changed"),
	})
	changed.Source = localSource(t, root)
	changedContent := PrepareShape(&changed, nil).Content()
	output, nextReceipt, err := Render(t.Context(), renderer, changed, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Equal(t, manifest.ValueEqual(output, docs), false)
	next := nextReceipt.Value().(renderFrame)
	assert.Equal(t, next.Key != frame.Key, true)
	acceptance.Receipt, acceptance.Content = nextReceipt, changedContent
	assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
	for i, address := range kustomizeRenderAddresses(t, renderer.owner, next.Key) {
		assert.Equal(t, address != addresses[i], true)
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
		assert.Equal(t, publication.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(0))
	}

	for _, label := range []string{"standalone", "retained A", "retained B"} {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Owner: h.owner}}
			scope := recorder.Scope()
			current := h.req
			current.Work = scope.RenderWork()
			if label != "standalone" {
				current.Preparation = new(Preparation)
			}
			documents, _, err := Render(t.Context(), renderer, current, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, documents, docs)
			for _, address := range addresses {
				assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
			}

			_, previous, available := renderer.PreviousRender(t.Context(), current)
			assert.FatalIf(t, !available, "%s", "original address lost its output")
			history, complete := renderer.RenderHistory(h.owner, previous, h.content)
			assert.FatalIf(t, !complete, "%s", "original address lost its accepted inventory")
			assert.Diff(t, maps.Collect(history), replacement)
			want := uint64(1)
			if label == "standalone" {
				want = 2
			}
			assert.Equal(t, scope.Record(manifest.WorkPreparation, addresses[0]).Counts.RawKeyPrepares, uint64(1))
			assert.Equal(t, scope.Record(manifest.WorkPreparation, addresses[1]).Counts.RawKeyPrepares, want)
		})
	}
}

func kustomizeRenderAddresses(t *testing.T, owner *sourcecache.Cache, key string) [2]string {
	t.Helper()
	var addresses [2]string
	for i, mode := range [...]sourcecache.RenderMode{sourcecache.RenderPending, sourcecache.RenderAccepted} {
		_, _, id, err := owner.Blob(
			t.Context(),
			sourcecache.BlobKey{Content: key, Render: mode},
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		assert.NoError(t, err)
		addresses[i] = id
	}
	assert.Equal(t, addresses[0] != addresses[1], true)
	return addresses
}
