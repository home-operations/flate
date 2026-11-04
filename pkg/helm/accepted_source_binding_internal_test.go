package helm

import (
	"bytes"
	"context"
	"io"
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
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
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
				_, complete := renderer.PreviousRender(t.Context(), hr, nil, input)
				assert.FatalIf(t, !complete, "%s", "matching source binding missed")
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
			wire, err := encodeFrame(header, frame.output)
			assert.NoError(t, err)
			stored, err := decodeFrame(wire)
			assert.NoError(t, err)
			accepted, err := encodeAccepted(changed.Named(), &stored, emptyRenderInventory)
			assert.NoError(t, err)
			_, _, _, err = renderer.cache.Blob(
				t.Context(),
				cache.BlobKey{Content: prepared.key, Render: cache.RenderAccepted},
				cache.Policy{MaxAge: 0},
				func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
					_, copyErr := io.Copy(w, io.MultiReader(bytes.NewReader(wire), bytes.NewReader(accepted)))
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
}
