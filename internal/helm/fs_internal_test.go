package helm

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
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
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoadFSChart_MatchesHelmDirLoader(t *testing.T) {
	t.Parallel()
	for _, irregular := range []bool{false, true} {
		name := "files"
		if irregular {
			name = "irregular"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartMatchesHelmDirLoaderCase(
				t,
				&irregular,
			)
		},
		)
	}
}

func TestLoadChart_ArtifactKeyIsolation(t *testing.T) {
	t.Parallel()
	st := store.New()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	cliSources := NewSources(st)
	for _, name := range []string{"first", "second", "first", "missing-key", "invalid-metadata", "invalid-archive"} {
		key := name
		if name == "missing-key" {
			key = ""
		}
		version := "1.0.0"
		if name == "invalid-metadata" || name == "invalid-archive" {
			version = "invalid"
		}
		data := []byte("apiVersion: v2\nname: " + name + "\nversion: " + version + "\n")
		input := fstest.MapFS{"Chart.yaml": {Data: data}}
		if name == "invalid-archive" {
			input = fstest.MapFS{
				"chart.tgz": {
					Data: testutil.TarGz(
						t,
						map[string]string{name + "/Chart.yaml": string(data)},
						0o644,
						gzip.DefaultCompression,
					),
				},
			}
		}
		art := &store.SourceArtifact{
			Kind: manifest.KindGitRepository,
			Key:  key,
			FS:   acquireChartFS(t, cachetest.New(t, "", sourcecache.New), input),
		}
		st.SetArtifact(
			manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Name:      name,
				Namespace: "test",
			},
			art,
		)
		hr := &manifest.HelmRelease{
			Namespace: "test",
			Chart: manifest.HelmChart{
				Name: ".",
				Source: manifest.NamedResource{
					Group: manifest.SourceDomain,
					Kind:  manifest.KindGitRepository,
					Name:  name,
				},
			},
		}
		if name == "invalid-archive" {
			hr.Chart.Name = "chart.tgz"
		}
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		if name == "invalid-metadata" || name == "invalid-archive" {
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || strings.Contains(err.Error(), "input error"),
				"invalid metadata must be classified once: %v",
				err,
			)
			continue
		}
		assert.NoError(t, err)
		if loaded.chart.Name() != name {
			t.Fatalf("loaded %s, want %s", loaded.chart.Name(), name)
		}
	}
	t.Run("shared chart source values", func(t *testing.T) {
		t.Parallel()
		testLoadChartArtifactKeyIsolationSharedChartSourceValues(
			t,
			&testLoadChartArtifactKeyIsolationSharedChartSourceValuesInputs{
				st:         &st,
				cli:        &cli,
				cliSources: &cliSources,
			},
		)
	},
	)
}

func BenchmarkLoadChart_FSWarm(b *testing.B) {
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
	st := store.New()
	cliSources := NewSources(st)
	st.SetArtifact(manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "test", Name: "chart",
	},
		&store.SourceArtifact{
			Kind: manifest.KindHelmChart, Key: "generation",
			FS: fstest.MapFS{"chart/Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}},
		})
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Name:      "chart",
				Namespace: "test",
			},
		},
	}
	if _, err := loadDecodedChart(b.Context(), cli, hr, cliSources); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := loadDecodedChart(b.Context(), cli, hr, cliSources); err != nil {
			b.Fatal(err)
		}
	}
}

type countingFile struct {
	fs.File

	bytesRead *int64
}

func (f countingFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	*f.bytesRead += int64(n)
	return n, err
}

//nolint:paralleltest,reassign // Serial fixtures must set and restore Helm's process-wide decompression limit.
func TestLoadFSChart_ArchiveSizeLimit(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	archive.MaxDecompressedChartSize = 256
	t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
	t.Run("oversized", func(t *testing.T) {
		payload := testutil.TarGz(t, map[string]string{
			"chart/Chart.yaml":  "apiVersion: v2\nname: chart\nversion: 1.0.0\n",
			"chart/values.yaml": strings.Repeat(" ", int(archive.MaxDecompressedChartSize)+1),
		}, 0o600, gzip.DefaultCompression)
		assert.Equal(t, int64(len(payload)) < archive.MaxDecompressedChartSize, true)
		_, wantErr := loader.LoadArchive(bytes.NewReader(payload))
		assert.FatalIf(t, wantErr == nil, "%s", "Helm accepted oversized fixture")
		captured, missing, err := captureChart(
			t.Context(),
			fstest.MapFS{"chart.tgz": {Data: payload}},
			"chart.tgz",
			false,
		)
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		_, err = captured.load()
		assert.FatalIf(t, err == nil, "%s", "flate accepted oversized fixture")
		assert.Equal(t, err.Error(), wantErr.Error())
	})
	t.Run("valid reload", func(t *testing.T) {
		data := "apiVersion: v2\nname: chart\nversion: 1.0.0\n"
		payload := testutil.TarGz(t, map[string]string{"chart/Chart.yaml": data}, 0o600, gzip.NoCompression)
		assert.Equal(t, int64(len(data)) < archive.MaxDecompressedChartSize, true)
		assert.Equal(t, int64(len(payload)) > archive.MaxDecompressedChartSize, true)
		_, err := loader.LoadArchive(bytes.NewReader(payload))
		assert.NoError(t, err)
		input := fstest.MapFS{"chart.tgz": {Data: payload}}
		captured, missing, err := captureChart(t.Context(), input, "chart.tgz", false)
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		loaded, err := captured.load()
		assert.NoError(t, err)
		assert.Equal(t, loaded.Metadata.Name, "chart")
		assert.Equal(t, loaded.Metadata.Version, "1.0.0")
		assert.Equal(t, bytes.Equal(captured.archive, payload), true)
		warm, missing, err := captureChart(t.Context(), input, "chart.tgz", false)
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		assert.Equal(t, warm.fingerprint(), captured.fingerprint())
		assert.Equal(t, bytes.Equal(warm.archive, captured.archive), true)
	})
}

//nolint:paralleltest,reassign // Serial fixtures must set and restore Helm's process-wide decompression limit.
func TestLoadFSChart_DecompressedBudgetMatchesHelm(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	archive.MaxDecompressedChartSize = 128
	defer func() { archive.MaxDecompressedChartSize = old }()
	for _, automatic := range []bool{false, true} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("automatic=%t/delta=%d", automatic, delta), func(t *testing.T) {
				testLoadFSChartDecompressedBudgetMatchesHelmCase(
					t,
					&testLoadFSChartDecompressedBudgetMatchesHelmCaseInputs{automatic: &automatic, delta: &delta},
				)
			},
			)
		}
	}
}

func TestLoadFSChart_InvalidInputs(t *testing.T) {
	t.Parallel()
	for name, input := range map[string]fstest.MapFS{
		"missing":       {},
		"empty":         {"chart": {Mode: fs.ModeDir}},
		"empty archive": {"chart": {Data: []byte{}}},
		"archive":       {"chart": {Data: []byte("invalid archive")}},
		"metadata":      {"chart/Chart.yaml": {Data: []byte("name: chart\nversion: invalid\n")}},
		"ignore":        {"chart/.helmignore": {Data: []byte("**\n")}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testLoadFSChartInvalidInputsCase(t, &testLoadFSChartInvalidInputsCaseInputs{name: &name, input: &input})
		},
		)
	}
}

func TestLoadChart_ReleasedPinRejectsHostChart(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := cachetest.New(t, "", sourcecache.New)
	cli := NewClient(owner)
	cliSources := NewSources(st)
	label := t.TempDir()
	testutil.WriteFile(t, label, "Chart.yaml", "apiVersion: v2\nname: host\nversion: 1.0.0\n")
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Name:      "chart",
				Namespace: "n",
			},
		},
	}
	art := &store.SourceArtifact{
		FS:        acquireChartFS(t, owner, chartArchiveFS{os.DirFS(label)}),
		LocalPath: label,
		Key:       "pin",
	}
	st.SetArtifact(hr.ChartSource(), art)
	_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	released := *art
	released.FS = nil
	st.SetArtifact(hr.ChartSource(), &released)
	{
		_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.FatalIf(t, !errors.Is(err, manifest.ErrObjectNotFound), "released content used host chart: %v", err)
	}
}

func TestLoadChart_MutableArchiveInvalidates(t *testing.T) {
	t.Parallel()
	roots := []string{t.TempDir(), t.TempDir()}
	st := store.New()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	cliSources := NewSources(st)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "n",
		Name:      "chart",
	}
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Name:   "chart.tgz",
			Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Namespace: id.Namespace, Name: id.Name},
		},
	}
	var previous *chart.Chart
	var previousInfo fs.FileInfo
	for _, tc := range []struct {
		root, version string
		stamp         int64
	}{
		{roots[0], "1.0.0", 1},
		{roots[0], "2.0.0", 1},
		{roots[1], "3.0.0", 1},
	} {
		payload := testutil.TarGz(
			t,
			map[string]string{"chart/Chart.yaml": "apiVersion: v2\nname: chart\nversion: " + tc.version + "\n"},
			0o600,
			gzip.NoCompression,
		)
		testutil.WriteFile(t, tc.root, "chart.tgz", string(payload))
		stamp := time.Unix(tc.stamp, 0)
		filename := filepath.Join(tc.root, "chart.tgz")
		assert.NoError(t, os.Chtimes(filename, stamp, stamp))
		info, err := os.Stat(filename)
		assert.NoError(t, err)
		if previousInfo != nil {
			assert.Equal(t, info.Size(), previousInfo.Size())
			assert.Equal(t, info.ModTime().Equal(previousInfo.ModTime()), true)
		}
		location, err := gitrepo.Locate(t.Context(), tc.root, tc.root)
		assert.NoError(t, err)
		admitted, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		st.SetArtifact(id, &store.SourceArtifact{LocalRoot: tc.root, LocalPath: ".", FS: admitted.FS})
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart.Metadata.Version, tc.version)
		assert.Equal(t, loaded.chart != previous, true)
		warm, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == loaded.chart, true)
		assert.Equal(t, warm.fingerprint, loaded.fingerprint)
		previous, previousInfo = loaded.chart, info
	}
}

func TestLoadChart_AnonymousMutableInputsDoNotShareMemo(t *testing.T) {
	t.Parallel()
	st := store.New()
	client := NewClient(cachetest.New(t, "", sourcecache.New))
	clientSources := NewSources(st)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "test",
		Name:      "anonymous",
	}
	hr := &manifest.HelmRelease{
		Chart: manifest.HelmChart{
			Name:   ".",
			Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Name: id.Name, Namespace: id.Namespace},
		},
	}
	for _, archive := range []bool{false, true} {
		for _, label := range []string{"", "mutable"} {
			t.Run(fmt.Sprintf("archive_%t/label_%s", archive, label), func(t *testing.T) {
				t.Parallel()
				testLoadChartAnonymousMutableInputsDoNotShareMemoCase(
					t,
					&testLoadChartAnonymousMutableInputsDoNotShareMemoCaseInputs{
						st:            &st,
						client:        &client,
						clientSources: &clientSources,
						id:            &id,
						hr:            &hr,
						archive:       &archive,
						label:         &label,
					},
				)
			},
			)
		}
	}
}

// Only Open is intercepted so the wrapper itself carries no acquisition authority.
type chartOpenFS struct {
	fs.FS

	open func(string) (fs.File, error)
}

func (f chartOpenFS) Open(name string) (fs.File, error) { return f.open(name) }

func testLoadFSChartMatchesHelmDirLoaderCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	for name, data := range map[string]string{
		"Chart.yaml":            "\xef\xbb\xbfapiVersion: v2\nname: parent\nversion: 1.0.0\n",
		"values.yaml":           "\xef\xbb\xbfanswer: 42\n",
		"templates/config.yaml": "\xef\xbb\xbfkind: ConfigMap\n",
		"crds/example.yaml":     "kind: CustomResourceDefinition\n",
		".helmignore":           "drop.txt\nignored/\n*.tmp\n",
		"drop.txt":              "ignored", "ignored/Chart.yaml": "invalid",
		"other.tmp": "ignored", "templates/.hidden": "default ignore",
		"charts/child/Chart.yaml":            "apiVersion: v2\nname: child\nversion: 2.0.0\n",
		"charts/child/values.yaml":           "enabled: true\n",
		"charts/child/templates/config.yaml": "kind: Secret\n",
	} {
		testutil.WriteFile(t, root, name, data)
	}
	if *testCaseInput {
		listener, err := net.Listen("unix", filepath.Join(root, "socket"))
		assert.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
	}
	want, wantErr := loader.LoadDir(root)
	captured, _, gotErr := captureChart(t.Context(), os.DirFS(root), ".", false)
	var got *chart.Chart
	if gotErr == nil {
		got, gotErr = captured.load()
	}
	if *testCaseInput {
		assert.FatalIf(t, wantErr == nil || gotErr == nil, "irregular: upstream=%v FS=%v", wantErr, gotErr)
		assert.FatalIf(
			t,
			!errors.Is(gotErr, manifest.ErrInput),
			"irregular error lacks input classification: %v",
			gotErr,
		)
		{
			want := strings.ReplaceAll(
				wantErr.Error(),
				filepath.Join(root, "socket"),
				"socket",
			)
			assert.FatalIf(t, gotErr.Error() != want, "irregular error: got %q, want %q", gotErr, want)
		}
		return
	}
	assert.FatalIf(t, wantErr != nil || gotErr != nil, "upstream=%v FS=%v", wantErr, gotErr)
	{
		diff := cmp.Diff(
			want,
			got,
			cmp.AllowUnexported(chart.Chart{}),
			cmpopts.IgnoreFields(chart.Chart{}, "parent"),
		)
		assert.FatalIf(t, diff != "", "chart (-want +got):\n%s", diff)
	}
}

type testLoadChartArtifactKeyIsolationSharedChartSourceValuesInputs struct {
	st         **store.Store
	cli        **Client
	cliSources *Sources
}

func testLoadChartArtifactKeyIsolationSharedChartSourceValues(
	t *testing.T,
	testCaseInput *testLoadChartArtifactKeyIsolationSharedChartSourceValuesInputs,
) {
	t.Helper()

	var canonical *chart.Chart
	for _, marker := range []string{"first", "second", "first"} {
		root := t.TempDir()
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n",
		)
		testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
		location, err := gitrepo.Locate(t.Context(), root, root)
		assert.NoError(t, err)
		admitted, err := gittree.AdmitDirectory(t.Context(), location)
		assert.NoError(t, err)
		hr := &manifest.HelmRelease{
			Name:      "demo",
			Namespace: "test",
			Chart: manifest.HelmChart{
				Name: "charts/demo",
				Source: manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindGitRepository,
					Name:      "repo",
					Namespace: "test",
				},
			},
			ChartValuesFiles: []string{"config/prod.yaml"},
		}
		(*testCaseInput.st).SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: admitted.FS})
		loaded, err := loadDecodedChart(t.Context(), (*testCaseInput.cli), hr, (*testCaseInput.cliSources))
		assert.NoError(t, err)
		if canonical == nil {
			canonical = loaded.chart
		}
		assert.Equal(t, loaded.chart == canonical, true)
		out, _, err := testutil.RenderMaps(
			(*testCaseInput.cli).Template(
				t.Context(),
				hr,
				nil,
				RenderOptions{Options: Options{}, Sources: (*testCaseInput.cliSources)},
			),
		)
		assert.NoError(t, err)
		assert.FatalIf(t, !hasRenderField(out, "marker", marker), "%v", out)
	}
}

type testLoadFSChartDecompressedBudgetMatchesHelmCaseInputs struct {
	automatic *bool
	delta     *int
}

func testLoadFSChartDecompressedBudgetMatchesHelmCase(
	t *testing.T,
	testCaseInput *testLoadFSChartDecompressedBudgetMatchesHelmCaseInputs,
) {
	t.Helper()
	metadata := "\xef\xbb\xbfapiVersion: v2\nname: chart\nversion: 1.0.0\n"
	policy := "templates/.hidden\n"
	files := map[string]string{
		"Chart.yaml": metadata, ".helmignore": policy,
		"values.yaml": strings.Repeat(" ", 128+(*testCaseInput.delta)-len(metadata)-len(policy)),
	}
	if *testCaseInput.automatic {
		files["templates/.hidden"] = files["values.yaml"]
		delete(files, "values.yaml")
		rooted := make(map[string]string)
		for name, data := range files {
			rooted["demo/"+name] = data
		}
		files = rooted
	}
	input := fstest.MapFS{}
	root := t.TempDir()
	for name, data := range files {
		input[name] = &fstest.MapFile{Data: []byte(data)}
		testutil.WriteFile(t, root, name, data)
	}
	var wantErr error
	if *testCaseInput.automatic {
		_, wantErr = loader.LoadArchive(
			bytes.NewReader(testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)),
		)
	} else {
		_, wantErr = loader.LoadDir(root)
	}
	captured, _, gotErr := captureChart(t.Context(), input, ".", (*testCaseInput.automatic))
	if gotErr == nil {
		_, gotErr = captured.load()
	}
	assert.Equal(
		t,
		wantErr != nil,
		(*testCaseInput.delta) > 0 || (*testCaseInput.automatic) && (*testCaseInput.delta) == 0,
	)
	assert.Equal(t, gotErr != nil, wantErr != nil)
	assert.FatalIf(
		t,
		gotErr != nil && !errors.Is(gotErr, manifest.ErrInput),
		"budget error lacks input classification: %v",
		gotErr,
	)
}

type testLoadFSChartInvalidInputsCaseInputs struct {
	name  *string
	input *fstest.MapFS
}

func testLoadFSChartInvalidInputsCase(t *testing.T, testCaseInput *testLoadFSChartInvalidInputsCaseInputs) {
	t.Helper()

	captured, missing, err := captureChart(t.Context(), (*testCaseInput.input), "chart", false)
	if err == nil {
		_, err = captured.load()
	}
	assert.FatalIf(
		t, (*testCaseInput.name) ==
			"missing" && !errors.Is(err, fs.ErrNotExist), "missing original cause: %v",
		err,
	)
	assert.FatalIf(
		t,
		missing != ((*testCaseInput.name) == "missing"),
		"missing location = %v for %s: %v",
		missing, (*testCaseInput.name), err,
	)
	if (*testCaseInput.name) == "archive" || (*testCaseInput.name) == "empty" ||
		(*testCaseInput.name) == "empty archive" {
		assert.FatalIf(t, err == nil, "%s", "invalid chart must return an error")
		return
	}
	if (*testCaseInput.name) == "metadata" {
		{
			_, ok := errors.AsType[chart.ValidationError](err)
			assert.FatalIf(t, !ok, "invalid metadata lacks validation error: %v", err)
		}
		return
	}
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput),
		"invalid chart lacks input classification: %v",
		err,
	)
}

type testLoadChartAnonymousMutableInputsDoNotShareMemoCaseInputs struct {
	st            **store.Store
	client        **Client
	clientSources *Sources
	id            *manifest.NamedResource
	hr            **manifest.HelmRelease
	archive       *bool
	label         *string
}

func testLoadChartAnonymousMutableInputsDoNotShareMemoCase(
	t *testing.T,
	testCaseInput *testLoadChartAnonymousMutableInputsDoNotShareMemoCaseInputs,
) {
	t.Helper()
	st := store.New()
	hr := (*testCaseInput.hr).Clone()
	sources := NewSources(st)
	testCaseInput.st, testCaseInput.hr, testCaseInput.clientSources = &st, &hr, &sources

	canonical := map[string]chartDescription{}
	for _, value := range []string{"one", "two", "one"} {
		input := fstest.MapFS{
			"Chart.yaml":  {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
			"values.yaml": {Data: []byte("answer: " + value + "\n")},
		}
		for _, version := range []string{"1.0.0", "2.0.0"} {
			input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: demo\nversion: " + version + "\n")
			admitted := input
			(*testCaseInput.hr).Chart.Name = "."
			if *testCaseInput.archive {
				(*testCaseInput.hr).Chart.Name = "chart.tgz"
				admitted = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, input)}}
			}
			(*testCaseInput.st).SetArtifact(
				(*testCaseInput.id),
				&store.SourceArtifact{FS: admitted, LocalPath: (*testCaseInput.label)},
			)
			loaded, err := loadDecodedChart(
				t.Context(),
				(*testCaseInput.client),
				(*testCaseInput.hr),
				(*testCaseInput.clientSources),
			)
			assert.NoError(t, err)
			assert.Equal(t, loaded.chart.Values["answer"], any(value))
			assert.Equal(t, loaded.chart.Metadata.Version, version)
			warm, err := loadDecodedChart(
				t.Context(),
				(*testCaseInput.client),
				(*testCaseInput.hr),
				(*testCaseInput.clientSources),
			)
			assert.NoError(t, err)
			assert.Equal(t, warm.chart == loaded.chart, true)
			assert.Equal(t, warm.fingerprint, loaded.fingerprint)
			if previous, ok := canonical[value+version]; ok {
				assert.Equal(t, loaded.chart == previous.chart, true)
				assert.Equal(t, loaded.fingerprint, previous.fingerprint)
			}
			canonical[value+version] = loaded
		}
	}
}
