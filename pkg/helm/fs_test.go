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
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
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
			captured, _, gotErr := captureChart(t.Context(), os.DirFS(root), ".", false)
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
		art := &store.SourceArtifact{Kind: manifest.KindGitRepository, Key: key, FS: acquireChartFS(t, cachetest.New(t, "", sourcecache.New), input)}
		st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: name, Namespace: "test"}, art)
		hr := &manifest.HelmRelease{Namespace: "test", Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: name}}}
		if name == "invalid-archive" {
			hr.Chart.Name = "chart.tgz"
		}
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
			out, _, err := cli.Template(t.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
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
			FS: fstest.MapFS{"chart/Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}}})
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
		captured, missing, err := captureChart(t.Context(), fstest.MapFS{"chart.tgz": {Data: payload}}, "chart.tgz", false)
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

func TestLoadFSChart_DecompressedBudgetMatchesHelm(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	archive.MaxDecompressedChartSize = 128
	defer func() { archive.MaxDecompressedChartSize = old }()
	for _, automatic := range []bool{false, true} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("automatic=%t/delta=%d", automatic, delta), func(t *testing.T) {
				metadata := "\xef\xbb\xbfapiVersion: v2\nname: chart\nversion: 1.0.0\n"
				policy := "templates/.hidden\n"
				files := map[string]string{
					"Chart.yaml": metadata, ".helmignore": policy,
					"values.yaml": strings.Repeat(" ", 128+delta-len(metadata)-len(policy)),
				}
				if automatic {
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
				if automatic {
					_, wantErr = loader.LoadArchive(bytes.NewReader(testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)))
				} else {
					_, wantErr = loader.LoadDir(root)
				}
				captured, _, gotErr := captureChart(t.Context(), input, ".", automatic)
				if gotErr == nil {
					_, gotErr = captured.load()
				}
				assert.Equal(t, wantErr != nil, delta > 0 || automatic && delta == 0)
				assert.Equal(t, gotErr != nil, wantErr != nil)
				if gotErr != nil && !errors.Is(gotErr, manifest.ErrInput) {
					t.Fatalf("budget error lacks input classification: %v", gotErr)
				}
			})
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
			captured, missing, err := captureChart(t.Context(), input, "chart", false)
			if err == nil {
				_, err = captured.load()
			}
			if name == "missing" && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("missing original cause: %v", err)
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
	art := &store.SourceArtifact{FS: acquireChartFS(t, owner, chartArchiveFS{os.DirFS(label)}), LocalPath: label, Key: "pin"}
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
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "n", Name: "chart"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: "chart.tgz", Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}}}
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
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "test", Name: "anonymous"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Group: id.Group, Kind: id.Kind, Name: id.Name, Namespace: id.Namespace}}}
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
						hr.Chart.Name = "."
						if archive {
							hr.Chart.Name = "chart.tgz"
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
		files, err := chartFilesInFS(t.Context(), observed, false)
		assert.NoError(t, err)
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
		same, err := chartFilesInFS(t.Context(), input, false)
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
		captured, _, err := captureChart(t.Context(), observed, "chart.tgz", false)
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
			files, err := chartFilesInFS(t.Context(), input, false)
			assert.NoError(t, err)
			for _, file := range files {
				if file.Name == ".helmignore" {
					t.Fatal("self-excluded policy admitted")
				}
			}
			tc.change(input)
			after, err := chartFilesInFS(t.Context(), input, false)
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
				_, missing, err := captureChart(t.Context(), observed, ".", false)
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
			want, wantErr := loader.LoadArchive(bytes.NewReader(testutil.TarGz(t, tc.files, 0o644, gzip.DefaultCompression)))
			input := fstest.MapFS{}
			for name, content := range tc.files {
				input[name] = &fstest.MapFile{Data: []byte(content), ModTime: time.Unix(0, 0)}
			}
			st := store.New()
			hr := newHR()
			hr.Chart = manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: hr.Namespace, Name: "input"}}
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, FS: input})
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			sources := NewSources(st)
			got, gotErr := loadDecodedChart(t.Context(), cli, hr, sources)
			if wantErr != nil {
				if gotErr == nil {
					t.Fatalf("Helm rejected archive with %v; flate loaded %s", wantErr, got.chart.Name())
				}
				if !strings.Contains(gotErr.Error(), wantErr.Error()) {
					t.Fatalf("Helm error %v; flate error %v", wantErr, gotErr)
				}
				return
			}
			t.Logf("Helm accepted chart %q with %d templates", want.Name(), len(want.Templates))
			if gotErr != nil {
				t.Fatalf("Helm accepted archive; flate failed: %v", gotErr)
			}
			if diff := cmp.Diff(want, got.chart, cmp.AllowUnexported(chart.Chart{}), cmpopts.IgnoreFields(chart.Chart{}, "parent")); diff != "" {
				t.Errorf("chart (-want +got):\n%s", diff)
			}
			if tc.name == "split roots" || tc.name == "multiple metadata roots" {
				docs, _, err := cli.Template(t.Context(), hr, nil, RenderOptions{Sources: sources})
				assert.NoError(t, err)
				assert.Equal(t, len(docs), len(want.Templates))
			}
		})
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
			files := fstest.MapFS{
				"a/Chart.yaml": {Data: []byte("apiVersion: v2\nname: first\nversion: 1.0.0\n")},
				"z/Chart.yaml": {Data: []byte("apiVersion: v2\nname: alternate\nversion: 1.0.0\n")},
			}
			broken := true
			closeErr := errors.New("close failed")
			var input fs.FS = chartOpenFS{FS: files, open: func(name string) (fs.File, error) {
				if broken && name == "a/Chart.yaml" && failure == "open" {
					return nil, fs.ErrPermission
				}
				file, err := files.Open(name)
				if !broken || err != nil || name != "a/Chart.yaml" {
					return file, err
				}
				switch failure {
				case "read":
					return valuesFaultFile{File: file, readErr: fs.ErrPermission}, nil
				case "close":
					return valuesFaultFile{File: file, closeErr: fs.ErrPermission}, nil
				case "read and close":
					return valuesFaultFile{File: file, readErr: fs.ErrPermission, closeErr: closeErr}, nil
				}
				return file, nil
			}}
			if failure == "stat" {
				input = chartStatFS{FS: input, broken: &broken}
			}
			if failure == "enumeration" {
				input = chartReadDirFS{FS: input, broken: &broken}
			}
			hr := newHR()
			hr.Chart = manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "source", Namespace: hr.Namespace}}
			st := store.New()
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			docs, _, err := cli.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(st)})
			if !errors.Is(err, fs.ErrPermission) || !errors.Is(err, manifest.ErrInput) || docs != nil {
				t.Fatalf("%s failure selected another chart: docs=%v, error=%v", failure, docs, err)
			}
			if failure == "read and close" && !errors.Is(err, closeErr) {
				t.Fatalf("lost joined close error: %v", err)
			}
			assert.Equal(t, len(cli.chartCache), 0)
			broken = false
			loaded, err := loadDecodedChart(t.Context(), cli, hr, NewSources(st))
			assert.NoError(t, err)
			assert.Equal(t, loaded.chart.Name(), "alternate")
			assert.Equal(t, len(cli.chartCache), 1)
		})
	}
}

func TestLoadChart_AliasArchiveLayout(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := map[string]string{
		"cluster/ks.yaml":         testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: OCIRepository, name: automatic}\n"),
		"apps/kustomization.yaml": "resources: []\n",
		"cluster/hr.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: automatic, namespace: flux-system}
`,
		"demo/Chart.yaml":         "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
		"demo/.helmignore":        "templates/.hidden\n",
		"demo/templates/.hidden":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hidden}\n",
		"other/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other}\n",
	}
	for name, data := range files {
		testutil.WriteFile(t, root, name, data)
		assert.NoError(t, os.Chtimes(filepath.Join(root, filepath.FromSlash(name)), time.Unix(0, 0), time.Unix(0, 0)))
	}
	testutil.GitInit(t, root)
	want, err := loader.LoadArchive(bytes.NewReader(testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)))
	assert.NoError(t, err)
	owner := cachetest.New(t, "", sourcecache.New)
	trees := kustomize.NewTreeCache(owner, nil, task.NewBounded(2))
	t.Cleanup(trees.Close)
	st := store.New()
	_, err = discovery.Run(t.Context(), discovery.Config{Trees: trees, Path: filepath.Join(root, "cluster"), RepoRoot: root, Store: st})
	assert.NoError(t, err)
	hr := st.GetObject(testutil.HelmReleaseID("apps", "demo")).(*manifest.HelmRelease)
	assert.Equal(t, hr.TracksDigest(), true)
	artifact := st.GetArtifact(hr.ChartSource()).(*store.SourceArtifact)
	assert.Equal(t, artifact.InputAlias, true)
	assert.Equal(t, artifact.Revision, "")
	cli := NewClient(owner)
	loaded, err := loadDecodedChart(t.Context(), cli, hr, NewSources(st))
	assert.NoError(t, err)
	if diff := cmp.Diff(want, loaded.chart, cmp.AllowUnexported(chart.Chart{}), cmpopts.IgnoreFields(chart.Chart{}, "parent")); diff != "" {
		t.Fatalf("alias chart (-want +got):\n%s", diff)
	}
	warm, err := loadDecodedChart(t.Context(), cli, hr, NewSources(st))
	assert.NoError(t, err)
	assert.Equal(t, warm.chart == loaded.chart, true)
	assert.Equal(t, warm.fingerprint, loaded.fingerprint)
	assert.Equal(t, st.GetArtifact(hr.ChartSource()), store.Artifact(artifact))
}
