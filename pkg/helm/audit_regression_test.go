package helm

import (
	"math"
	"strings"
	"testing"

	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	chartutil "helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
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
		{"chart defaults", nil, &chart.Chart{Values: map[string]any{"bad": make(chan int)}}},
		{"subchart defaults", nil, func() *chart.Chart {
			ch := minimalChart()
			ch.AddDependency(&chart.Chart{Values: map[string]any{"bad": make(chan int)}})
			return ch
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, computeTemplateKey(chartLoadResult{fingerprint: manifest.Fingerprint(projectChart(tc.chart)), chart: tc.chart}, tc.values, Options{}, minimalHR()), "")
		})
	}
}

func TestMergeChartValuesFiles_EmptyArchiveFile(t *testing.T) {
	t.Parallel()
	ch := minimalChart()
	ch.Files = []*common.File{{Name: "empty.yaml", Data: []byte{}}}
	archive, err := chartutil.Save(ch, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loader.Load(archive)
	if err != nil {
		t.Fatal(err)
	}
	values, err := mergeChartValuesFilesUncached(loaded, []string{"empty.yaml"}, false)
	if err != nil {
		t.Fatalf("empty file treated as missing: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("empty file produced values: %v", values)
	}
}

func TestTemplate_UnmarshalableValuesSkipCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n")
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	hr := newHR()
	hr.DisableSchemaValidation = true
	loaded, err := cli.loadChart(t.Context(), hr)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{"unused": func() {}}
	opts := Options{}
	assert.Equal(t, computeTemplateKey(loaded, values, opts, hr), "")
	out, err := cli.Template(t.Context(), hr, values, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "kind: ConfigMap") {
		t.Fatalf("unmarshalable values prevented the render: %s", out)
	}
}
