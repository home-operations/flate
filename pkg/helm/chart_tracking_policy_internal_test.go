package helm

import (
	"os"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testTemplateCacheNonOCITrackingModeCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st, hr, dir := ociRenderFixture(t)
	hr = hr.Clone()
	hr.ChartRef = nil
	hr.Chart.Source.Kind = (*testCaseInput)
	if (*testCaseInput) == manifest.KindHelmChart {
		hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: (*testCaseInput), Name: "podinfo"}
	}
	st.SetArtifact(
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      (*testCaseInput),
			Namespace: "apps",
			Name:      "podinfo",
		},
		&store.SourceArtifact{Kind: (*testCaseInput), FS: chartFixtureFS(hr, os.DirFS(dir)), LocalPath: dir},
	)
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	var first []map[string]any
	var key string
	for _, disabled := range []bool{false, true} {
		opts := Options{DisableChartDigestTracking: disabled}
		out, receipt11, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt11,
						Records: emptyRenderInventory,
						Sources: []manifest.SourceEvidence{},
					},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.NoError(t, err)
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		merged, err := cli.mergeChartValuesFiles(
			t.Context(), &loaded, hr.ChartValuesFiles,
			hr.IgnoreMissingValuesFiles,
		)
		assert.NoError(t, err)
		nextKey := computeTemplateKey(&loaded, merged, opts, hr)
		if first != nil {
			assert.Diff(t, out, first)
			assert.Equal(t, nextKey, key)
		}
		assert.FatalIf(t, !hasRenderField(out, "version", "6.15.0"), "%v", out)
		first, key = out, nextKey
	}
}
