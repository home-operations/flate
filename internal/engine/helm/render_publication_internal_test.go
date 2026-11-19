package helm

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordRender_ChartAnalysis(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
				"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
			})
			directory := ""
			if backing == "disk" {
				directory = t.TempDir()
			}
			owner := cachetest.New(t, directory, cache.New)
			client := NewClient(owner, nil)
			hr := newHR()
			input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
			request, err := client.prepareTemplate(t.Context(), hr, nil, &input)
			assert.NoError(t, err)
			documents, receipt, err := request.render(t.Context(), client, nil)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			assert.Equal(t, frame.header.Analysis == nil, true)
			validation, err := frame.RenderPublication(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, validation.Metadata == nil && validation.Content == nil && validation.Sources == nil, true)
			acceptance := cache.RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Records: emptyRenderInventory,
				Sources: []manifest.SourceEvidence{},
			}
			projection, err := frame.RenderPublication(t.Context(), &acceptance)
			assert.NoError(t, err)
			assert.Equal(t, projection.Metadata != nil, owner.CanPersistRenders())
			assert.Equal(t, frame.header.Analysis == nil, true)
			assert.Equal(t, frame.Stored == nil, true)
			if owner.CanPersistRenders() {
				metadata := projection.Metadata.(renderHeader)
				assert.Equal(t, metadata.Analysis != nil, true)
				assert.Equal(t, metadata.Analysis.Version, "0.1.0")
				assert.Equal(t, projection.Frame.Binding, frame.binding.Fingerprint())
				assert.Equal(t, manifest.ValueEqual(projection.Content, request.content), true)
			}
			assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
			_, complete := client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, owner.CanPersistRenders())
			_, _, err = client.Template(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			cached, cachedReceipt, err := client.Template(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			assert.Diff(t, cached, documents)
			cachedFrame := cachedReceipt.Value().(renderFrame)
			assert.Equal(t, bytes.Equal(cachedFrame.Output, frame.Output), true)
			assert.Equal(t, cachedFrame.header.Analysis != nil, true)
			acceptance.Receipt = cachedReceipt
			assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
			_, complete = client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, owner.CanPersistRenders())
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			assert.Equal(t, errors.Is(owner.RecordRender(canceled, acceptance), context.Canceled), true)
		})
	}
}

func TestRecordRender_InvalidHelmReceipt(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", cache.New)
	for _, tc := range []struct {
		name  string
		frame renderFrame
	}{
		{name: "missing key", frame: renderFrame{header: renderHeader{Analysis: &chartAnalysis{Version: "0.1.0"}}}},
		{name: "missing chart and analysis", frame: renderFrame{RenderFrame: cache.RenderFrame{Key: "invalid"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := owner.RecordRender(t.Context(), cache.RenderAcceptance{
				Receipt: manifest.NewRenderReceipt(tc.frame, equalRenderFrame),
				Records: emptyRenderInventory, Sources: []manifest.SourceEvidence{},
			})
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}

func TestRecordRender_HelmCaptures(t *testing.T) {
	t.Parallel()
	for _, capture := range []string{"reads", "output reads"} {
		t.Run(capture, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
				"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
			})
			owner := cachetest.New(t, t.TempDir(), cache.New)
			client := NewClient(owner, nil)
			hr := newHR()
			input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
			_, receipt, err := client.Template(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			child := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "previous"}
			records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {File: "previous.yaml"}}
			source := manifest.SourceIdentity{Root: root}
			previous := []manifest.SourceEvidence{{Source: source, Path: "previous.yaml"}}
			acceptance := cache.RenderAcceptance{
				Owner: hr.Named(), Receipt: receipt, Records: maps.All(records), Sources: previous,
			}
			assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
			_, receipt, err = client.Template(t.Context(), hr, nil, input)
			assert.NoError(t, err)
			st := store.New()
			objects := [2]*manifest.ConfigMap{{Name: "input"}, {Name: "output"}}
			var reads, outputReads store.ReadSet
			reads.Init(st, hr.Named(), nil)
			outputReads.Init(st, hr.Named(), nil)
			defer reads.Retire()
			defer outputReads.Retire()
			for _, object := range objects {
				st.AddObject(object)
			}
			reads.GetObject(objects[0].Named())
			outputReads.GetObject(objects[1].Named())
			changed := *objects[0]
			if capture == "output reads" {
				changed = *objects[1]
			}
			changed.Data = map[string]any{"value": "changed"}
			st.AddObject(&changed)
			assert.Equal(t, reads.Valid(), capture != "reads")
			assert.Equal(t, outputReads.Valid(), capture != "output reads")
			acceptance.Receipt, acceptance.Reads, acceptance.OutputReads = receipt, &reads, &outputReads
			acceptance.Records = emptyRenderInventory
			acceptance.Sources = []manifest.SourceEvidence{{Source: source, Path: "rejected.yaml"}}
			assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
			history, complete := client.PreviousRender(t.Context(), hr, nil, input)
			assert.Equal(t, complete, owner.CanPersistRenders())
			if complete {
				assert.Diff(t, maps.Collect(history), records)
			}
			producers, err := owner.ProducersForPaths(t.Context(), previous)
			assert.NoError(t, err)
			wantProducers := 0
			if owner.CanPersistRenders() {
				wantProducers = 1
			}
			assert.Equal(t, len(producers), wantProducers)
			if owner.CanPersistRenders() {
				assert.Diff(t, producers[hr.Named()], previous)
			}
			producers, err = owner.ProducersForPaths(t.Context(), acceptance.Sources)
			assert.NoError(t, err)
			assert.Equal(t, len(producers), 0)
		})
	}
}
