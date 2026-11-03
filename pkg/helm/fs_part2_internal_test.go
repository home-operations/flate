package helm

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
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

//nolint:paralleltest // test replaces process-wide dependency configuration.
func TestLoadFSChart_FailureRecovery(t *testing.T) {
	for _, failure := range []string{"read", "close", "disappeared", "parse", "policy-limit"} {
		t.Run(failure, func(t *testing.T) {
			testLoadFSChartFailureRecoveryCase(t, &failure)
		},
		)
	}
}

type chartErrorFile struct {
	fs.File

	err       error
	closeOnly bool
}

func (f *chartErrorFile) Read(p []byte) (int, error) {
	if f.closeOnly {
		return f.File.Read(p)
	}
	return 0, f.err
}

func (f *chartErrorFile) Close() error {
	if f.File != nil {
		_ = f.File.Close()
	}
	if f.closeOnly {
		return f.err
	}
	return nil
}

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

type chartStatFS struct {
	fs.FS

	broken *bool
}

func (f chartStatFS) Stat(name string) (fs.FileInfo, error) {
	if *f.broken {
		return nil, fs.ErrPermission
	}
	return fs.Stat(f.FS, name)
}

type chartReadDirFS struct {
	fs.FS

	broken *bool
}

func (f chartReadDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if *f.broken {
		return nil, fs.ErrPermission
	}
	return fs.ReadDir(f.FS, name)
}

func TestLoadFSChart_AutomaticInputErrors(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"stat", "open", "enumeration", "read", "close", "read and close"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartAutomaticInputErrorsCase(t, &failure)
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
		{
			assert.FatalIf(t, file.Name == ".helmignore", "%s", "self-excluded policy admitted")
		}
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

//nolint:reassign // Serial fixtures must set and restore Helm's process-wide decompression limit.
func testLoadFSChartFailureRecoveryCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	input := fstest.MapFS{
		"Chart.yaml":        {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")},
		"templates/cm.yaml": {Data: []byte("old")},
	}
	broken := true
	want := errors.New("capture failed")
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		if broken && name == "templates/cm.yaml" {
			switch *testCaseInput {
			case "read":
				return &chartErrorFile{err: want}, nil
			case "close":
				f, err := input.Open(name)
				return &chartErrorFile{File: f, err: want, closeOnly: true}, err
			case "disappeared":
				return nil, fs.ErrNotExist
			}
		}
		return input.Open(name)
	}}
	if (*testCaseInput) == "parse" {
		input["Chart.yaml"].Data = []byte("name: chart\nversion: invalid\n")
	}
	if (*testCaseInput) == "policy-limit" {
		old := archive.MaxDecompressedChartSize
		archive.MaxDecompressedChartSize = 64
		t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
		input[".helmignore"] = &fstest.MapFile{Data: bytes.Repeat([]byte("#"), 65)}
	}
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
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed, Key: "untrusted"})
	cliSources := NewSources(st)
	_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	{
		assert.FatalIf(t, err == nil, "%s", "failed input accepted")
	}
	{
		assert.FatalIf(
			t,
			((*testCaseInput) == "read" || (*testCaseInput) == "close") && !errors.Is(err, want),
			"lost read/close error: %v",
			err,
		)
	}
	if (*testCaseInput) == "disappeared" {
		_, missing, captureChartErr := captureChart(t.Context(), observed, ".", false)
		assert.Equal(t, missing, false)
		assert.Equal(t, errors.Is(captureChartErr, fs.ErrNotExist), true)
	}
	captured := 0
	if (*testCaseInput) == "parse" {
		captured = 1
		for dIndex := range cli.chartCache {
			d := cli.chartCache[dIndex]
			assert.Equal(t, d.chart == nil, true)
		}
	}
	assert.Equal(t, len(cli.chartCache), captured)
	broken = false
	input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")
	delete(input, ".helmignore")
	_, err = loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	assert.Equal(t, len(cli.chartCache), captured+1)
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

func testLoadFSChartAutomaticInputErrorsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	files := fstest.MapFS{
		"a/Chart.yaml": {Data: []byte("apiVersion: v2\nname: first\nversion: 1.0.0\n")},
		"z/Chart.yaml": {Data: []byte("apiVersion: v2\nname: alternate\nversion: 1.0.0\n")},
	}
	broken := true
	closeErr := errors.New("close failed")
	var input fs.FS = chartOpenFS{FS: files, open: func(name string) (fs.File, error) {
		if broken && name == "a/Chart.yaml" && (*testCaseInput) == "open" {
			return nil, fs.ErrPermission
		}
		file, err := files.Open(name)
		if !broken || err != nil || name != "a/Chart.yaml" {
			return file, err
		}
		switch *testCaseInput {
		case "read":
			return valuesFaultFile{File: file, readErr: fs.ErrPermission}, nil
		case "close":
			return valuesFaultFile{File: file, closeErr: fs.ErrPermission}, nil
		case "read and close":
			return valuesFaultFile{File: file, readErr: fs.ErrPermission, closeErr: closeErr}, nil
		}
		return file, nil
	}}
	if (*testCaseInput) == "stat" {
		input = chartStatFS{FS: input, broken: &broken}
	}
	if (*testCaseInput) == "enumeration" {
		input = chartReadDirFS{FS: input, broken: &broken}
	}
	hr := newHR()
	hr.Chart = manifest.HelmChart{
		Source: manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindHelmChart,
			Name:      "source",
			Namespace: hr.Namespace,
		},
	}
	st := store.New()
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	docs, _, err := testutil.RenderMaps(
		cli.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(st)}),
	)
	{
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrPermission) || !errors.Is(err, manifest.ErrInput) || docs != nil,
			"%s failure selected another chart: docs=%v, error=%v", (*testCaseInput), docs,
			err,
		)
	}
	{
		assert.FatalIf(
			t, (*testCaseInput) ==
				"read and close" && !errors.Is(err, closeErr), "lost joined close error: %v",
			err,
		)
	}
	assert.Equal(t, len(cli.chartCache), 0)
	broken = false
	loaded, err := loadDecodedChart(t.Context(), cli, hr, NewSources(st))
	assert.NoError(t, err)
	assert.Equal(t, loaded.chart.Name(), "alternate")
	assert.Equal(t, len(cli.chartCache), 1)
}
