package helm

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoadFSChart_AutomaticArchiveSemantics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		files map[string]string
	}{
		{"split roots", map[string]string{
			"a/Chart.yaml":            "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
			"a/templates/first.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-first-root}\n",
			"b/templates/second.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-other-root}\n",
		}},
		{"multiple metadata roots", map[string]string{"a/Chart.yaml": "apiVersion: v2\nname: first\nversion: 1.0.0\n", "b/Chart.yaml": "apiVersion: v2\nname: last\nversion: 2.0.0\n", "b/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-other-root}\n"}},
		{"single root", map[string]string{"demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n"}},
		{"collisions", map[string]string{
			"a/Chart.yaml":  "apiVersion: v2\nname: first\nversion: 1.0.0\n",
			"b/Chart.yaml":  "apiVersion: v2\nname: last\nversion: 2.0.0\n",
			"a/values.yaml": "answer: first\n", "b/values.yaml": "answer: last\n",
			"a/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n",
			"b/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: last}\n",
		}},
		{"inert ignore", map[string]string{
			"demo/Chart.yaml":              "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
			"demo/.helmignore":             "templates/excluded.yaml\n",
			"demo/templates/excluded.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\n",
			"demo/templates/.hidden":       "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hidden}\n",
		}},
		{"invalid ignore", map[string]string{
			"demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n", "demo/.helmignore": "**\n",
		}},
		{"backslashes", map[string]string{`demo\Chart.yaml`: "apiVersion: v2\nname: demo\nversion: 1.0.0\n", `demo\templates\cm.yaml`: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: windows}\n"}},
		{"absolute", map[string]string{`demo\/outside`: "outside"}},
		{"drive", map[string]string{"demo/C:/file": "outside"}},
		{"parent prefix", map[string]string{"demo/..notes": "outside"}},
		{"base", map[string]string{"README": "outside"}},
		{"initial metadata component", map[string]string{"Chart.yaml/other": "outside"}},
		{"root metadata", map[string]string{"Chart.yaml": "apiVersion: v2\nname: root\nversion: 1.0.0\n", "templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root}\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartAutomaticArchiveSemanticsCase(t, &tc)
		},
		)
	}
}

func testLoadFSChartAutomaticArchiveSemanticsCase(
	t *testing.T,
	testCaseInput *struct {
		name  string
		files map[string]string
	},
) {
	t.Helper()

	want, wantErr := loader.LoadArchive(
		bytes.NewReader(testutil.TarGz(t, testCaseInput.files, 0o644, gzip.DefaultCompression)),
	)
	input := fstest.MapFS{}
	for name, content := range testCaseInput.files {
		input[name] = &fstest.MapFile{Data: []byte(content), ModTime: time.Unix(0, 0)}
	}
	st := store.New()
	hr := newHR()
	hr.Chart = manifest.HelmChart{
		Source: manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: hr.Namespace,
			Name:      "input",
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: input})
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	sources := NewSources(st)
	got, gotErr := loadDecodedChart(t.Context(), cli, hr, sources)
	if wantErr != nil {
		if gotErr == nil {
			t.Fatalf("Helm rejected archive with %v; flate loaded %s", wantErr, got.chart.Name())
		}
		{
			assert.FatalIf(
				t,
				!strings.Contains(gotErr.Error(), wantErr.Error()),
				"Helm error %v; flate error %v",
				wantErr,
				gotErr,
			)
		}
		return
	}
	t.Logf("Helm accepted chart %q with %d templates", want.Name(), len(want.Templates))
	{
		assert.FatalIf(t, gotErr != nil, "Helm accepted archive; flate failed: %v", gotErr)
	}
	{
		diff := cmp.Diff(
			want,
			got.chart,
			cmp.AllowUnexported(chart.Chart{}),
			cmpopts.IgnoreFields(chart.Chart{}, "parent"),
		)
		assert.ErrorIf(t, diff != "", "chart (-want +got):\n%s", diff)
	}
	if testCaseInput.name == "split roots" || testCaseInput.name == "multiple metadata roots" {
		docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, nil, RenderOptions{Sources: sources}))
		assert.NoError(t, err)
		assert.Equal(t, len(docs), len(want.Templates))
	}
}
