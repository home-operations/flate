package helm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_OCIResidentGenerationValidation(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	st, hr, directory := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	artifact := &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        chartFixtureFS(hr, os.DirFS(directory)),
		LocalPath: directory,
		Digest:    digest,
		Revision:  "6.15.0@" + digest,
	}
	st.SetArtifact(hr.ChartSource(), artifact)
	owner := cachetest.New(t, t.TempDir(), cache.New)
	client := NewClient(owner)
	input := RenderOptions{Sources: NewSources(st)}
	for range 2 {
		_, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
		assert.NoError(t, err)
		assert.NoError(
			t,
			client.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt,
					Sources: []manifest.SourceEvidence{},
					Records: emptyRenderInventory,
				},
			),
		)
	}
	prepared, err := client.prepareTemplate(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	resident := prepared.loaded.record
	{
		assert.FatalIf(
			t,
			resident == nil || resident.identity == "",
			"%s",
			"fixture did not retain the validated resident generation",
		)
	}
	header := resident.frame.header
	analysis := *header.Analysis
	analysis.Version = "invalid"
	header.Analysis = &analysis
	payload, err := encodeFrame(header, resident.Output)
	assert.NoError(t, err)
	_, _, _, err = owner.Blob(
		t.Context(),
		cache.BlobKey{Content: resident.key, Render: cache.RenderAccepted},
		cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, writeErr := w.Write(payload)
			return cache.ProducerResult{}, writeErr
		},
	)
	assert.NoError(t, err)

	{
		_, _, err := testutil.RenderMaps(
			client.Template(t.Context(), hr, nil, input),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "replaced OCI generation escaped version validation: %v", err)
	}
	{
		_, complete := client.PreviousRender(t.Context(), hr, nil, input)
		assert.FatalIf(t, complete, "%s", "invalid OCI generation supplied accepted history")
	}
	changed := *artifact
	changed.Revision = "invalid"
	st.SetArtifact(hr.ChartSource(), &changed)
	{
		_, _, err := testutil.RenderMaps(
			client.Template(t.Context(), hr, nil, input),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "changed OCI revision borrowed resident validation: %v", err)
	}
}

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
			prepared, err := renderer.prepareTemplate(t.Context(), changed, nil, current)
			assert.NoError(t, err)
			frame := receipt.Value().(renderFrame)
			binding := frame.binding
			binding.Policy = prepared.binding.Policy
			header := frame.header
			header.Key, header.Binding = prepared.key, manifest.Fingerprint(binding)
			header.Analysis = (chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
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

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{
		header: renderHeader{Key: "oversized-accepted", Analysis: &chartAnalysis{Version: "0.1.0"}},
		output: payload,
	}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))},
	}
	{
		err := renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrFlux,
		), "oversized accepted inventory was published: %v", err)
	}
}
