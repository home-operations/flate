package helm

import (
	"bytes"
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPreviousRender_SourceBinding(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
	)
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	for _, tc := range []struct {
		name   string
		mutate func(*manifest.HelmRelease, *store.SourceArtifact)
	}{
		{"source name", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Name = "foreign" }},
		{"source namespace", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Namespace = "foreign" }},
		{"source group", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Group = "foreign" }},
		{"source kind", func(h *manifest.HelmRelease, _ *store.SourceArtifact) { h.Chart.Source.Kind = manifest.KindBucket }},
		{"artifact kind", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Kind = "foreign" }},
		{"url", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.URL = "foreign" }},
		{"path", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.LocalPath = "foreign" }},
		{"origin", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.LocalRoot = "foreign" }},
		{"key", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Key = "foreign" }},
		{"revision", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Revision = "foreign" }},
		{"digest", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.Digest = "foreign" }},
		{"alias", func(_ *manifest.HelmRelease, a *store.SourceArtifact) { a.InputAlias = !a.InputAlias }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			_, receipt, err := testutil.RenderMaps(renderer.Template(t.Context(), hr, nil, input))
			assert.NoError(t, err)
			assert.NoError(
				t,
				renderer.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt,
						Sources: []manifest.SourceEvidence{},
						Records: emptyRenderInventory,
					},
				),
			)
			{
				var work manifest.WorkRecorder
				work.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
				observed := input
				observed.Work = work.Scope().RenderWork()
				_, complete := renderer.PreviousRender(t.Context(), hr, nil, observed)
				assert.Equal(t, complete, persistent)
				var lookup manifest.WorkCounts
				for _, row := range work.Scope().Snapshot(0) {
					if row.Key.Phase == manifest.WorkCache {
						lookup.Add(&row.Counts)
					}
				}
				wantLookup := manifest.WorkCounts{}
				if persistent {
					wantLookup = manifest.WorkCounts{Attempts: 1, Completed: 1}
				}
				assert.Equal(t, lookup, wantLookup)
			}
			changed := hr.Clone()
			source := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
			tc.mutate(changed, &source)
			objects := store.New()
			objects.SetArtifact(changed.ChartSource(), &source)
			current := input
			current.Sources = NewSources(objects)
			prepared, err := renderer.prepareTemplate(t.Context(), changed, nil, &current)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.binding
			binding.Policy = prepared.binding.Policy
			header := frame.header
			header.Key, header.Binding = prepared.key, manifest.Fingerprint(binding)
			header.Analysis = (&chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
			stored, err := cache.NewFrame(frameDomain, header, frame.output)
			assert.NoError(t, err)
			accepted, err := stored.PreparePublication(t.Context(), cache.FramePublicationInput{
				Owner: changed.Named(), Content: prepared.content, Records: emptyRenderInventory, Complete: true,
			})
			assert.NoError(t, err)
			_, _, _, err = renderer.cache.Blob(
				t.Context(),
				cache.BlobKey{Content: prepared.key, Render: cache.RenderAccepted},
				&cache.Policy{MaxAge: 0},
				func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					_, copyErr := accepted.WriteTo(w)
					return cache.ProducerResult{}, copyErr
				},
			)
			assert.NoError(t, err)

			{
				_, complete := renderer.PreviousRender(t.Context(), changed, nil, current)
				assert.FatalIf(t, complete, "%s", "foreign source coordinate borrowed history")
			}
		})
	}
	for _, field := range []string{"revision", "url"} {
		t.Run("reuse_"+field, func(t *testing.T) {
			t.Parallel()
			testPreviousRenderSourceBindingReuse(t, hr, &input, field)
		})
	}
}

func testPreviousRenderSourceBindingReuse(t *testing.T, hr *manifest.HelmRelease, input *RenderOptions, field string) {
	t.Helper()
	for _, lifetime := range []string{"retained", "transferred"} {
		t.Run(lifetime, func(t *testing.T) {
			t.Parallel()
			renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			var preparation, successor Preparation
			t.Cleanup(preparation.Retire)
			t.Cleanup(successor.Retire)
			var work manifest.WorkRecorder
			work.Init(&manifest.WorkKey{Owner: hr.Named()}, manifest.WorkScope{})
			source := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
			source.Content, source.Revision = "equal-chart-content", "before"
			objects := store.New()
			objects.SetArtifact(hr.ChartSource(), &source)
			current := RenderOptions{
				Sources: NewSources(objects), Preparation: &preparation, Work: work.Scope().RenderWork(),
			}
			docs, receipt, err := renderer.Template(t.Context(), hr, nil, current)
			assert.NoError(t, err)
			output, err := manifest.EncodeRender(docs)
			assert.NoError(t, err)
			prior := receipt.Value().(renderFrame)
			priorOutput := bytes.Clone(prior.output)
			binding, key := preparation.binding, preparation.key
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			}))
			_, complete := renderer.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, true)
			if lifetime == "transferred" {
				assert.Equal(t, successor.Take(&preparation, renderer, hr, &current), true)
				assert.Equal(t, preparation.complete || preparation.source != nil, false)
				current.Preparation = &successor
			}
			replacement := source
			if field == "revision" {
				replacement.Revision = "after"
			} else {
				replacement.URL = "https://example.test/current-chart"
			}
			objects.SetArtifact(hr.ChartSource(), &replacement)
			_, complete = renderer.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, false)
			changed, rebound, err := renderer.Template(t.Context(), hr, nil, current)
			assert.NoError(t, err)
			encoded, err := manifest.EncodeRender(changed)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Equal(encoded, output), true)
			frame := rebound.Value().(renderFrame)
			assert.Equal(t, bytes.Equal(frame.output, priorOutput), true)
			assert.Equal(t, frame.binding.Revision, replacement.Revision)
			assert.Equal(t, frame.binding.URL, replacement.URL)
			assert.Equal(t, current.Preparation.key, key)
			assert.Equal(t, current.Preparation.source, &replacement)
			assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: rebound,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			}))
			_, complete = renderer.PreviousRender(t.Context(), hr, nil, current)
			assert.Equal(t, complete, true)
			unchanged := receipt.Value().(renderFrame)
			assert.Equal(t, unchanged.binding.same(&binding), true)
			assert.Equal(t, bytes.Equal(unchanged.output, priorOutput), true)
			assert.Equal(t, source.Revision, "before")
			assert.Equal(t, source.URL, binding.URL)
			var engine manifest.WorkCounts
			rows := work.Scope().Snapshot(0)
			for i := range rows {
				row := &rows[i]
				if row.Key.Phase == manifest.WorkHelm {
					engine.Add(&row.Counts)
				}
			}
			assert.Equal(t, engine, manifest.WorkCounts{Attempts: 1, Completed: 1})
		})
	}
}
