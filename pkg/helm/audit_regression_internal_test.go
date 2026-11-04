package helm

import (
	"math"
	"testing"

	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	chartutil "helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestComputeTemplateKey_UnmarshalableInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		values map[string]any
		chart  *chart.Chart
	}{
		{"channel value", map[string]any{"bad": make(chan int)}, minimalChart()},
		{"function value", map[string]any{"bad": func() {}}, minimalChart()},
		{"NaN value", map[string]any{"bad": math.NaN()}, minimalChart()},
		{"infinite value", map[string]any{"bad": math.Inf(1)}, minimalChart()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(
				t,
				computeTemplateKey(
					&chartDescription{fingerprint: chartFixtureDescription(t, tc.chart).fingerprint, chart: tc.chart},
					tc.values,
					Options{},
					minimalHR(),
				),
				"",
			)
		})
	}
}

func TestMergeChartValuesFiles_EmptyArchiveFile(t *testing.T) {
	t.Parallel()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	ch := minimalChart()
	ch.Files = []*common.File{{Name: "empty.yaml", Data: []byte{}}}
	archive, err := chartutil.Save(ch, t.TempDir())
	assert.NoError(t, err)
	loaded, err := loader.Load(archive)
	assert.NoError(t, err)
	values, err := cli.mergeChartValuesFiles(
		t.Context(), &chartDescription{chart: loaded}, []string{"empty.yaml"},
		false,
	)
	assert.FatalIf(t, err != nil, "empty file treated as missing: %v", err)
	assert.FatalIf(t, len(values) != 0, "empty file produced values: %v", values)
}

func TestTemplate_UnmarshalableValuesSkipCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n")
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	cliSources := localChartResolver(t, "chart-repo", "flux-system", dir)
	hr := newHR()
	hr.DisableSchemaValidation = true
	loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	values := map[string]any{"unused": func() {}}
	opts := Options{}
	assert.Equal(t, computeTemplateKey(&loaded, values, opts, hr), "")
	out, _, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, values, RenderOptions{Options: opts, Sources: cliSources}),
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(out, "kind", "ConfigMap"),
		"unmarshalable values prevented the render: %s",
		out,
	)
}
