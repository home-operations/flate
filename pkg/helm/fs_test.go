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
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
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
			if irregular {
				listener, err := net.Listen("unix", filepath.Join(root, "socket"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
			}
			want, wantErr := loader.LoadDir(root)
			captured, _, gotErr := captureChart(t.Context(), os.DirFS(root), ".")
			var got *chart.Chart
			if gotErr == nil {
				got, gotErr = captured.load()
			}
			if irregular {
				if wantErr == nil || gotErr == nil {
					t.Fatalf("irregular: upstream=%v FS=%v", wantErr, gotErr)
				}
				if !errors.Is(gotErr, manifest.ErrInput) {
					t.Fatalf("irregular error lacks input classification: %v", gotErr)
				}
				if want := strings.ReplaceAll(wantErr.Error(), filepath.Join(root, "socket"), "socket"); manifest.TrimSentinelPrefix(gotErr.Error()) != want {
					t.Fatalf("irregular error: got %q, want %q", gotErr, want)
				}
				return
			}
			if wantErr != nil || gotErr != nil {
				t.Fatalf("upstream=%v FS=%v", wantErr, gotErr)
			}
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(chart.Chart{}), cmpopts.IgnoreFields(chart.Chart{}, "parent")); diff != "" {
				t.Fatalf("chart (-want +got):\n%s", diff)
			}
		})
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
			input = fstest.MapFS{"chart.tgz": {Data: testutil.TarGz(t, map[string]string{name + "/Chart.yaml": string(data)}, 0o644, gzip.DefaultCompression)}}
		}
		art := &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: key, FS: acquireChartFS(t, cachetest.New(t, "", sourcecache.New), input)}
		st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: name, Namespace: "test"}, art)
		hr := &manifest.HelmRelease{Namespace: "test", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: name}}}
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		if name == "invalid-metadata" || name == "invalid-archive" {
			if !errors.Is(err, manifest.ErrInput) || strings.Count(err.Error(), "input error") != 1 {
				t.Fatalf("invalid metadata must be classified once: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if loaded.chart.Name() != name {
			t.Fatalf("loaded %s, want %s", loaded.chart.Name(), name)
		}
	}
	t.Run("shared chart source values", func(t *testing.T) {
		t.Parallel()
		var canonical *chart.Chart
		for _, marker := range []string{"first", "second", "first"} {
			root := t.TempDir()
			testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  marker: {{ .Values.marker | quote }}\n")
			testutil.WriteFile(t, root, "config/prod.yaml", "marker: "+marker+"\n")
			location, err := gitrepo.Locate(t.Context(), root, root)
			assert.NoError(t, err)
			admitted, err := gittree.AdmitDirectory(t.Context(), location)
			assert.NoError(t, err)
			hr := &manifest.HelmRelease{Name: "demo", Namespace: "test", Chart: manifest.HelmChart{Name: "charts/demo", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo", Namespace: "test"}}, ChartValuesFiles: []string{"config/prod.yaml"}}
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: admitted.FS})
			loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
			assert.NoError(t, err)
			if canonical == nil {
				canonical = loaded.chart
			}
			assert.Equal(t, loaded.chart == canonical, true)
			out, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
			assert.NoError(t, err)
			if !hasRenderField(out, "marker", marker) {
				t.Fatal(out)
			}
		}
	})

}

func BenchmarkLoadChart_FSWarm(b *testing.B) {
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
	st := store.New()
	cliSources := NewSources(st)
	st.SetArtifact(manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "test", Name: "chart"},
		&store.SourceArtifact{Kind: manifest.KindHelmChart, Key: "generation",
			FS: fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}}})
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "chart", Namespace: "test"}}}
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
		if wantErr == nil {
			t.Fatal("Helm accepted oversized fixture")
		}
		captured, missing, err := captureChart(t.Context(), fstest.MapFS{"chart.tgz": {Data: payload}}, "chart.tgz")
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		_, err = captured.load()
		if err == nil {
			t.Fatal("flate accepted oversized fixture")
		}
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
		captured, missing, err := captureChart(t.Context(), input, "chart.tgz")
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		loaded, err := captured.load()
		assert.NoError(t, err)
		assert.Equal(t, loaded.Metadata.Name, "chart")
		assert.Equal(t, loaded.Metadata.Version, "1.0.0")
		assert.Equal(t, bytes.Equal(captured.archive, payload), true)
		warm, missing, err := captureChart(t.Context(), input, "chart.tgz")
		assert.NoError(t, err)
		assert.Equal(t, missing, false)
		assert.Equal(t, warm.fingerprint(), captured.fingerprint())
		assert.Equal(t, bytes.Equal(warm.archive, captured.archive), true)
	})
}

func TestLoadFSChart_DecompressedBudgetMatchesHelm(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	archive.MaxDecompressedChartSize = 64
	defer func() { archive.MaxDecompressedChartSize = old }()
	input := fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}, "values.yaml": {Data: bytes.Repeat([]byte(" "), 65)}}
	root := t.TempDir()
	for name, file := range input {
		testutil.WriteFile(t, root, name, string(file.Data))
	}
	if _, err := loader.LoadDir(root); err == nil {
		t.Fatal("Helm accepted oversized fixture")
	}
	if _, _, err := captureChart(t.Context(), input, "."); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("oversized FS chart lacks input classification: %v", err)
	}
}

func TestChartPathInFS_InvalidArtifacts(t *testing.T) {
	t.Parallel()
	for name, input := range map[string]fstest.MapFS{
		"missing-layer":   {},
		"multiple-charts": {"a/Chart.yaml": {}, "b/Chart.yaml": {}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := chartPathInFS(input); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid artifact lacks input classification: %v", err)
			}
		})
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
			captured, missing, err := captureChart(t.Context(), input, "chart")
			if err == nil {
				_, err = captured.load()
			}
			if missing != (name == "missing") {
				t.Fatalf("missing location = %v for %s: %v", missing, name, err)
			}
			if name == "archive" || name == "empty" || name == "empty archive" {
				if err == nil {
					t.Fatal("invalid chart must return an error")
				}
				return
			}
			if name == "metadata" {
				if _, ok := errors.AsType[chart.ValidationError](err); !ok {
					t.Fatalf("invalid metadata lacks validation error: %v", err)
				}
				return
			}
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid chart lacks input classification: %v", err)
			}
		})
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
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "chart", Namespace: "n"}}}
	art := &store.SourceArtifact{FS: acquireChartFS(t, owner, os.DirFS(label)), LocalPath: label, Key: "pin"}
	st.SetArtifact(hr.ChartSource(), art)
	_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
	assert.NoError(t, err)
	released := *art
	released.FS = nil
	st.SetArtifact(hr.ChartSource(), &released)
	if _, err := loadDecodedChart(t.Context(), cli, hr, cliSources); !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("released content used host chart: %v", err)
	}
}

func TestLoadChart_MutableArchiveInvalidates(t *testing.T) {
	t.Parallel()
	roots := []string{t.TempDir(), t.TempDir()}
	st := store.New()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	cliSources := NewSources(st)
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "n", Name: "chart"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}}}
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
		payload := testutil.TarGz(t, map[string]string{"chart/Chart.yaml": "apiVersion: v2\nname: chart\nversion: " + tc.version + "\n"}, 0o600, gzip.NoCompression)
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
	st := store.New()
	client := NewClient(cachetest.New(t, "", sourcecache.New))
	clientSources := NewSources(st)
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "test", Name: "anonymous"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Name: id.Name, Namespace: id.Namespace}}}
	for _, archive := range []bool{false, true} {
		for _, label := range []string{"", "mutable"} {
			t.Run(fmt.Sprintf("archive_%t/label_%s", archive, label), func(t *testing.T) {
				canonical := map[string]chartDescription{}
				for _, value := range []string{"one", "two", "one"} {
					input := fstest.MapFS{
						"Chart.yaml":  {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
						"values.yaml": {Data: []byte("answer: " + value + "\n")},
					}
					for _, version := range []string{"1.0.0", "2.0.0"} {
						input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: demo\nversion: " + version + "\n")
						admitted := input
						if archive {
							admitted = fstest.MapFS{"chart.tgz": {Data: chartArchive(t, input)}}
						}
						st.SetArtifact(id, &store.SourceArtifact{FS: admitted, LocalPath: label})
						loaded, err := loadDecodedChart(t.Context(), client, hr, clientSources)
						assert.NoError(t, err)
						assert.Equal(t, loaded.chart.Values["answer"], any(value))
						assert.Equal(t, loaded.chart.Metadata.Version, version)
						warm, err := loadDecodedChart(t.Context(), client, hr, clientSources)
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
			})
		}
	}
}

// Only Open is intercepted so the wrapper itself carries no acquisition authority.
type chartOpenFS struct {
	fs.FS
	open func(string) (fs.File, error)
}

func (f chartOpenFS) Open(name string) (fs.File, error) { return f.open(name) }

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
		input := base()
		input[".helmignore"] = &fstest.MapFile{Data: []byte("# first\n")}
		reads := map[string]int{}
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
			reads[name]++
			return input.Open(name)
		}}
		files, err := chartFilesInFS(t.Context(), observed)
		assert.NoError(t, err)
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
		same, err := chartFilesInFS(t.Context(), input)
		assert.NoError(t, err)
		if diff := cmp.Diff(files, same); diff != "" {
			t.Fatal(diff)
		}
		input["charts/child/templates/cm.yaml"].Data = []byte("new")
		input[".helmignore"].Data = []byte("# second\n")
		ch, err := loader.LoadFiles(files)
		assert.NoError(t, err)
		assert.Equal(t, string(ch.Dependencies()[0].Templates[0].Data), "old")
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
		next, err := chartFilesInFS(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, cmp.Equal(next, files), false)
		later, _, err := captureChart(t.Context(), input, ".")
		assert.NoError(t, err)
		laterChart, err := later.load()
		assert.NoError(t, err)
		assert.Equal(t, string(laterChart.Dependencies()[0].Templates[0].Data), "new")
		clear(reads)
		cli := NewClient(cachetest.New(t, "", sourcecache.New))
		st := store.New()
		hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo"}}}
		st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed})
		cliSources := NewSources(st)
		loaded, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
		assert.NoError(t, err)
		assert.Equal(t, string(loaded.chart.Dependencies()[0].Templates[0].Data), "new")
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
	})
	t.Run("archive exact handoff", func(t *testing.T) {
		t.Parallel()
		data := chartArchive(t, base())
		input := fstest.MapFS{"chart.tgz": {Data: data}}
		var bytesRead int64
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
			file, err := input.Open(name)
			if err != nil {
				return nil, err
			}
			return countingFile{File: file, bytesRead: &bytesRead}, nil
		}}
		captured, _, err := captureChart(t.Context(), observed, "chart.tgz")
		assert.NoError(t, err)
		key := captured.fingerprint()
		changed := base()
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
		hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: "chart.tgz", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo"}}}
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
	})

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
			input := base()
			input[".helmignore"] = &fstest.MapFile{Data: []byte(".helmignore\n")}
			input["a"] = &fstest.MapFile{Data: []byte("bc")}
			files, err := chartFilesInFS(t.Context(), input)
			assert.NoError(t, err)
			for _, file := range files {
				if file.Name == ".helmignore" {
					t.Fatal("self-excluded policy admitted")
				}
			}
			tc.change(input)
			after, err := chartFilesInFS(t.Context(), input)
			assert.NoError(t, err)
			assert.Equal(t, cmp.Equal(files, after, cmpopts.IgnoreFields(archive.BufferedFile{}, "ModTime")), tc.same)
		})
	}
}

func TestLoadFSChart_FailureRecovery(t *testing.T) {
	for _, failure := range []string{"read", "close", "disappeared", "parse", "policy-limit"} {
		t.Run(failure, func(t *testing.T) {
			input := fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}, "templates/cm.yaml": {Data: []byte("old")}}
			broken := true
			want := errors.New("capture failed")
			observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
				if broken && name == "templates/cm.yaml" {
					switch failure {
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
			if failure == "parse" {
				input["Chart.yaml"].Data = []byte("name: chart\nversion: invalid\n")
			}
			if failure == "policy-limit" {
				old := archive.MaxDecompressedChartSize
				archive.MaxDecompressedChartSize = 64
				t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
				input[".helmignore"] = &fstest.MapFile{Data: bytes.Repeat([]byte("#"), 65)}
			}
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			st := store.New()
			hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo"}}}
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed, Key: "untrusted"})
			cliSources := NewSources(st)
			_, err := loadDecodedChart(t.Context(), cli, hr, cliSources)
			if err == nil {
				t.Fatal("failed input accepted")
			}
			if (failure == "read" || failure == "close") && !errors.Is(err, want) {
				t.Fatalf("lost read/close error: %v", err)
			}
			if failure == "disappeared" {
				_, missing, err := captureChart(t.Context(), observed, ".")
				assert.Equal(t, missing, false)
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			captured := 0
			if failure == "parse" {
				captured = 1
				for _, d := range cli.chartCache {
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
		})
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
