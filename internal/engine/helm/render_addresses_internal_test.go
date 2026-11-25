package helm

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestTemplate_RenderAddresses(t *testing.T) {
	t.Parallel()
	if reason := cache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {value: {{ .Values.value | quote }}}\n",
	})
	owner := cachetest.New(t, t.TempDir(), cache.New)
	assert.FatalIf(t, !owner.CanPersistRenders(), "%s", "disk cache must persist renders")
	client := NewClient(owner, nil)
	hr := newHR()
	sources := localChartResolver(t, "chart-repo", "flux-system", root)
	values := map[string]any{"value": "original"}
	var preparation Preparation
	recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Owner: hr.Named()}}
	scope := recorder.Scope()
	input := RenderOptions{Sources: sources, Preparation: &preparation, Work: scope.RenderWork()}
	documents, receipt, err := client.Template(t.Context(), hr, values, input)
	assert.NoError(t, err)
	assert.Equal(t, len(documents), 1)
	frame := receipt.Value().(renderFrame)
	publication := (&manifest.WorkRecorder{Key: manifest.WorkKey{Owner: hr.Named()}}).Scope()
	acceptance := cache.RenderAcceptance{
		Owner: hr.Named(), Receipt: receipt, Records: emptyRenderInventory,
		Sources: []manifest.SourceEvidence{}, Work: publication.RenderWork(),
	}
	assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
	addresses := helmRenderAddresses(t, owner, frame.Key)
	for _, address := range addresses {
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
		assert.Equal(t, publication.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(0))
	}

	child := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "replacement"}
	replacement := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {File: "replacement.yaml"}}
	acceptance.Records = maps.All(replacement)
	assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
	history, complete := client.PreviousRender(t.Context(), hr, values, input)
	assert.FatalIf(t, !complete, "%s", "replacement inventory missing")
	assert.Diff(t, maps.Collect(history), replacement)
	repeated, _, err := client.Template(t.Context(), hr, values, input)
	assert.NoError(t, err)
	assert.Diff(t, repeated, documents)
	for _, address := range addresses {
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
	}

	changed, nextReceipt, err := client.Template(t.Context(), hr, map[string]any{"value": "changed"}, input)
	assert.NoError(t, err)
	assert.Equal(t, manifest.ValueEqual(changed, documents), false)
	next := nextReceipt.Value().(renderFrame)
	assert.Equal(t, next.Key != frame.Key, true)
	acceptance.Receipt = nextReceipt
	assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
	for i, address := range helmRenderAddresses(t, owner, next.Key) {
		assert.Equal(t, address != addresses[i], true)
		assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
		assert.Equal(t, publication.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(0))
	}

	t.Run("refresh receipt lookup", func(t *testing.T) {
		t.Parallel()
		receiptCache := cachetest.New(t, t.TempDir(), cache.New)
		receiptClient := NewClient(receiptCache, nil)
		current := RenderOptions{Sources: sources, Preparation: new(Preparation)}
		_, receipt, err := receiptClient.Template(t.Context(), hr, values, current)
		assert.NoError(t, err)
		request, err := receiptClient.prepareTemplate(t.Context(), hr, values, &current)
		assert.NoError(t, err)
		lookup := request.addresses.Accepted(request.key)
		stale := receipt.Value().(renderFrame)
		assert.Equal(t, stale.Lookup, lookup)
		assert.FatalIf(t, lookup == "", "request has no accepted address")
		stale.Lookup = ""
		record := &renderRecord{frame: stale, receipt: renderDomain.Receipt(stale)}
		refreshed, rendered, err := request.validateRecord(t.Context(), record, &request.binding, false)
		assert.NoError(t, err)
		assert.FatalIf(t, refreshed == nil, "cached render did not validate")
		assert.Diff(t, normalizeLists(rendered), documents)

		var recorder manifest.WorkRecorder
		assert.NoError(t, receiptCache.RecordRender(t.Context(), cache.RenderAcceptance{
			Owner: hr.Named(), Receipt: refreshed.receipt, Records: maps.All(replacement),
			Sources: []manifest.SourceEvidence{}, Work: recorder.Scope().RenderWork(),
		}))
		assert.Equal(t, refreshed.receipt.Value().(renderFrame).Lookup, lookup)
		assert.Equal(t, recorder.Scope().Record(manifest.WorkPreparation, lookup).Counts.RawKeyPrepares, uint64(0))
	})

	for _, label := range []string{"standalone", "retained A", "retained B"} {
		t.Run(label, func(t *testing.T) {
			t.Parallel()
			recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Owner: hr.Named()}}
			scope := recorder.Scope()
			current := RenderOptions{Sources: sources, Work: scope.RenderWork()}
			if label != "standalone" {
				current.Preparation = new(Preparation)
			}
			rendered, _, err := client.Template(t.Context(), hr, values, current)
			assert.NoError(t, err)
			assert.Diff(t, rendered, documents)
			for _, address := range addresses {
				assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, uint64(1))
			}
			previous, complete := client.PreviousRender(t.Context(), hr, values, current)
			assert.FatalIf(t, !complete, "%s", "original address lost its accepted inventory")
			assert.Diff(t, maps.Collect(previous), replacement)
			want := uint64(1)
			if label == "standalone" {
				want = 2
			}
			for _, address := range addresses {
				assert.Equal(t, scope.Record(manifest.WorkPreparation, address).Counts.RawKeyPrepares, want)
			}
		})
	}
}

func helmRenderAddresses(t *testing.T, owner *cache.Cache, key string) [2]string {
	t.Helper()
	var addresses [2]string
	for i, mode := range [...]cache.RenderMode{cache.RenderPending, cache.RenderAccepted} {
		_, _, id, err := owner.Blob(
			t.Context(), cache.BlobKey{Content: key, Render: mode}, &cache.Policy{MaxAge: cache.Forever}, nil,
		)
		assert.NoError(t, err)
		addresses[i] = id
	}
	assert.Equal(t, addresses[0] != addresses[1], true)
	return addresses
}
