package helm

import (
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoadFSChart_CaptureIdentity(t *testing.T) {
	t.Parallel()
	base := func() fstest.MapFS {
		return fstest.MapFS{
			"Chart.yaml":                     {Data: []byte("apiVersion: v2\nname: parent\nversion: 1.0.0\n")},
			"charts/child/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
			"charts/child/templates/cm.yaml": {Data: []byte("old")},
		}
	}
	t.Run("one policy read and exact handoff", func(t *testing.T) {
		t.Parallel()
		testLoadFSChartCaptureIdentityOnePolicyReadAndExactHandoff(
			t,
			&base,
		)
	},
	)
	t.Run("archive exact handoff", func(t *testing.T) {
		t.Parallel()
		testLoadFSChartCaptureIdentityArchiveExactHandoff(
			t,
			&base,
		)
	},
	)

	for _, tc := range []struct {
		name   string
		change func(fstest.MapFS)
		same   bool
	}{
		{"policy self exclusion", func(input fstest.MapFS) { input[".helmignore"].Data = []byte(".helmignore\n# changed\n") }, true},
		{"raw BOM policy", func(input fstest.MapFS) {
			input[".helmignore"].Data = append([]byte{0xef, 0xbb, 0xbf}, input[".helmignore"].Data...)
		}, true},
		{"policy presence", func(input fstest.MapFS) { delete(input, ".helmignore") }, true},
		{"policy admission", func(input fstest.MapFS) { input[".helmignore"].Data = []byte(".helmignore\na\n") }, false},
		{"name and data", func(input fstest.MapFS) { delete(input, "a"); input["ab"] = &fstest.MapFile{Data: []byte("c")} }, false},
		{"mtime ignored", func(input fstest.MapFS) { input["a"].ModTime = time.Date(2300, 1, 2, 3, 4, 5, 6, time.UTC) }, true},
		{"actual data", func(input fstest.MapFS) { input["a"].Data = []byte("bd") }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartCaptureIdentityCase(t, &testLoadFSChartCaptureIdentityCaseInputs{base: &base, tc: &tc})
		},
		)
	}
}

func testLoadFSChartCaptureIdentityOnePolicyReadAndExactHandoff(
	t *testing.T,
	testCaseInput *func() fstest.MapFS,
) {
	t.Helper()

	input := (*testCaseInput)()
	input[".helmignore"] = &fstest.MapFile{Data: []byte("# first\n")}
	reads := map[string]int{}
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		reads[name]++
		return input.Open(name)
	}}
	files, err := chartFilesInFS(t.Context(), observed, false)
	assert.NoError(t, err)
	assert.Equal(t, reads[".helmignore"], 1)
	assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
	same, err := chartFilesInFS(t.Context(), input, false)
	assert.NoError(t, err)
	{
		diff := cmp.Diff(files, same)
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	input["charts/child/templates/cm.yaml"].Data = []byte("new")
	input[".helmignore"].Data = []byte("# second\n")
	ch, err := loader.LoadFiles(files)
	assert.NoError(t, err)
	assert.Equal(t, string(ch.Dependencies()[0].Templates[0].Data), "old")
	assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
	next, err := chartFilesInFS(t.Context(), input, false)
	assert.NoError(t, err)
	assert.Equal(t, cmp.Equal(next, files), false)
	later, _, err := captureChart(t.Context(), input, ".", false)
	assert.NoError(t, err)
	laterChart, err := later.load()
	assert.NoError(t, err)
	assert.Equal(t, string(laterChart.Dependencies()[0].Templates[0].Data), "new")
	clear(reads)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	st := store.New()
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Name: ".",
			Source: manifest.NamedResource{
				Group: manifest.SourceDomain,
				Kind:  manifest.KindGitRepository,
				Name:  "repo",
			},
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed})
	cliSources := NewSources(st)
	loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, string(loaded.chart.Dependencies()[0].Templates[0].Data), "new")
	assert.Equal(t, reads[".helmignore"], 1)
	assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
}

func testLoadFSChartCaptureIdentityArchiveExactHandoff(
	t *testing.T,
	testCaseInput *func() fstest.MapFS,
) {
	t.Helper()

	data := chartArchive(t, (*testCaseInput)())
	input := fstest.MapFS{"chart.tgz": {Data: data}}
	var bytesRead int64
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		file, err := input.Open(name)
		if err != nil {
			return nil, err
		}
		return countingFile{File: file, bytesRead: &bytesRead}, nil
	}}
	captured, _, err := captureChart(t.Context(), observed, "chart.tgz", false)
	assert.NoError(t, err)
	key := captured.fingerprint()
	changed := (*testCaseInput)()
	changed["charts/child/templates/cm.yaml"].Data = []byte("new")
	input["chart.tgz"].Data = chartArchive(t, changed)
	ch, err := captured.load()
	assert.NoError(t, err)
	assert.Equal(t, string(ch.Dependencies()[0].Templates[0].Data), "old")
	assert.Equal(t, captured.fingerprint(), key)
	assert.Equal(t, bytesRead, int64(len(data)))
	bytesRead = 0
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	st := store.New()
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Name: "chart.tgz",
			Source: manifest.NamedResource{
				Group: manifest.SourceDomain,
				Kind:  manifest.KindGitRepository,
				Name:  "repo",
			},
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed})
	cliSources := NewSources(st)
	loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, string(loaded.chart.Dependencies()[0].Templates[0].Data), "new")
	assert.Equal(t, bytesRead, int64(len(input["chart.tgz"].Data)))
	bytesRead = 0
	warm, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == loaded.chart, true)
	assert.Equal(t, warm.fingerprint, loaded.fingerprint)
	assert.Equal(t, bytesRead, int64(len(input["chart.tgz"].Data)))
}

type testLoadFSChartCaptureIdentityCaseInputs struct {
	base *func() fstest.MapFS
	tc   *struct {
		name   string
		change func(fstest.MapFS)
		same   bool
	}
}

func testLoadFSChartCaptureIdentityCase(t *testing.T, testCaseInput *testLoadFSChartCaptureIdentityCaseInputs) {
	t.Helper()

	input := (*testCaseInput.base)()
	input[".helmignore"] = &fstest.MapFile{Data: []byte(".helmignore\n")}
	input["a"] = &fstest.MapFile{Data: []byte("bc")}
	files, err := chartFilesInFS(t.Context(), input, false)
	assert.NoError(t, err)
	for _, file := range files {
		assert.FatalIf(t, file.Name == ".helmignore", "%s", "self-excluded policy admitted")
	}
	testCaseInput.tc.change(input)
	after, err := chartFilesInFS(t.Context(), input, false)
	assert.NoError(t, err)
	assert.Equal(
		t,
		cmp.Equal(files, after, cmpopts.IgnoreFields(archive.BufferedFile{}, "ModTime")),
		testCaseInput.tc.same,
	)
}
