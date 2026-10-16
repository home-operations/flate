package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
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
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
)

func TestLoadFSChart_MatchesHelmDirLoader(t *testing.T) {
	for _, irregular := range []bool{false, true} {
		name := "files"
		if irregular {
			name = "irregular"
		}
		t.Run(name, func(t *testing.T) {
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
			got, _, _, gotErr := loadFSChart(os.DirFS(root), ".")
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
	st := store.New()
	cli, err := NewClient(newTestCache(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
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
			var buf bytes.Buffer
			gw := gzip.NewWriter(&buf)
			tw := tar.NewWriter(gw)
			if err := tw.WriteHeader(&tar.Header{Name: name + "/Chart.yaml", Size: int64(len(data)), Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(tw.Close(), gw.Close()); err != nil {
				t.Fatal(err)
			}
			input = fstest.MapFS{"chart.tgz": {Data: buf.Bytes()}}
		}
		art := &store.SourceArtifact{Kind: manifest.KindHelmChart, Key: key, FS: acquireChartFS(t, newTestCache(t, ""), input)}
		st.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: name, Namespace: "test"}, art)
		hr := &manifest.HelmRelease{Namespace: "test", Chart: manifest.HelmChart{RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmChart, RepoName: name}}
		loaded, err := cli.loadChart(t.Context(), hr)
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
}

func BenchmarkLoadChart_FSWarm(b *testing.B) {
	cli, err := NewClient(newTestCache(b, ""))
	if err != nil {
		b.Fatal(err)
	}
	st := store.New()
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	st.SetArtifact(manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "test", Name: "chart"},
		&store.SourceArtifact{Kind: manifest.KindHelmChart, Key: "generation",
			FS: fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}}})
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmChart, RepoName: "chart", RepoNamespace: "test"}}
	if _, err := cli.loadChart(b.Context(), hr); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := cli.loadChart(b.Context(), hr); err != nil {
			b.Fatal(err)
		}
	}
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
	if _, _, _, err := loadFSChart(input, "."); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("oversized FS chart lacks input classification: %v", err)
	}
}

func TestChartPathInFS_InvalidArtifacts(t *testing.T) {
	for name, input := range map[string]fstest.MapFS{
		"missing-layer":   {},
		"multiple-charts": {"a/Chart.yaml": {}, "b/Chart.yaml": {}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := chartPathInFS(input); !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("invalid artifact lacks input classification: %v", err)
			}
		})
	}
}

func TestLoadFSChart_InvalidInputs(t *testing.T) {
	for name, input := range map[string]fstest.MapFS{
		"missing":  {},
		"empty":    {"chart": {Mode: fs.ModeDir}},
		"archive":  {"chart": {Data: []byte("invalid archive")}},
		"metadata": {"chart/Chart.yaml": {Data: []byte("name: chart\nversion: invalid\n")}},
		"ignore":   {"chart/.helmignore": {Data: []byte("**\n")}},
	} {
		t.Run(name, func(t *testing.T) {
			_, fingerprint, missing, err := loadFSChart(input, "chart")
			if missing != (name == "missing") {
				t.Fatalf("missing location = %v for %s: %v", missing, name, err)
			}
			if name == "empty" && fingerprint == "" {
				t.Fatal("empty directory was treated as an archive")
			}
			if name == "archive" || name == "empty" {
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
	st := store.New()
	cli, err := NewClient(newTestCache(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	label := t.TempDir()
	testutil.WriteFile(t, label, "Chart.yaml", "apiVersion: v2\nname: host\nversion: 1.0.0\n")
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindHelmChart, RepoName: "chart", RepoNamespace: "n"}}
	st.SetArtifact(hr.ChartSource(),
		&store.SourceArtifact{LocalPath: label, Key: "pin"})
	if _, err := cli.loadChart(t.Context(), hr); !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("released content used host chart: %v", err)
	}
}

func TestLoadChart_MutableArchiveInvalidates(t *testing.T) {
	roots := []string{t.TempDir(), t.TempDir()}
	st := store.New()
	cli, err := NewClient(newTestCache(t, ""))
	assert.NoError(t, err)
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "n", Name: "chart"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoGroup: id.Group, RepoKind: id.Kind, RepoNamespace: id.Namespace, RepoName: id.Name}}
	var previous *chart.Chart
	var previousInfo fs.FileInfo
	for _, tc := range []struct {
		root, version string
		stamp         int64
	}{
		{roots[0], "1.0.0", 1},
		{roots[0], "2.0.0", 2},
		{roots[1], "3.0.0", 2},
	} {
		var buf bytes.Buffer
		gw, err := gzip.NewWriterLevel(&buf, gzip.NoCompression)
		assert.NoError(t, err)
		tw := tar.NewWriter(gw)
		data := []byte("apiVersion: v2\nname: chart\nversion: " + tc.version + "\n")
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "chart/Chart.yaml", Mode: 0o600, Size: int64(len(data))}))
		_, err = tw.Write(data)
		assert.NoError(t, err)
		assert.NoError(t, tw.Close())
		assert.NoError(t, gw.Close())
		testutil.WriteFile(t, tc.root, "chart.tgz", buf.String())
		stamp := time.Unix(tc.stamp, 0)
		filename := filepath.Join(tc.root, "chart.tgz")
		assert.NoError(t, os.Chtimes(filename, stamp, stamp))
		info, err := os.Stat(filename)
		assert.NoError(t, err)
		if tc.root == roots[1] {
			assert.Equal(t, info.Size(), previousInfo.Size())
			assert.Equal(t, info.ModTime().Equal(previousInfo.ModTime()), true)
		}
		input, err := source.NewLocalFS(t.Context(), tc.root)
		assert.NoError(t, err)
		st.SetArtifact(id, &store.SourceArtifact{LocalRoot: tc.root, LocalPath: ".", FS: input})
		loaded, err := cli.loadChart(t.Context(), hr)
		assert.NoError(t, err)
		assert.Equal(t, loaded.chart.Metadata.Version, tc.version)
		assert.Equal(t, loaded.chart != previous, true)
		warm, err := cli.loadChart(t.Context(), hr)
		assert.NoError(t, err)
		assert.Equal(t, warm.chart == loaded.chart, true)
		previous, previousInfo = loaded.chart, info
	}
}

func TestLoadChart_AnonymousMutableInputsDoNotShareMemo(t *testing.T) {
	st := store.New()
	client, err := NewClient(newTestCache(t, ""))
	assert.NoError(t, err)
	client.SetSourceResolver(NewStoreSourceResolver(st))
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "test", Name: "anonymous"}
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoGroup: id.Group, RepoKind: id.Kind, RepoName: id.Name, RepoNamespace: id.Namespace}}
	for _, label := range []string{"", "mutable"} {
		for _, value := range []string{"one", "two", "one"} {
			input := fstest.MapFS{
				"Chart.yaml":  {Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")},
				"values.yaml": {Data: []byte("answer: " + value + "\n")},
			}
			st.SetArtifact(id, &store.SourceArtifact{FS: input, LocalPath: label})
			loaded, err := client.loadChart(t.Context(), hr)
			if err != nil || loaded.chart.Values["answer"] != value {
				t.Fatalf("anonymous input %q: %+v, %v", value, loaded, err)
			}
			input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: demo\nversion: 20.0.0\n")
			loaded, err = client.loadChart(t.Context(), hr)
			if err != nil || loaded.chart.Metadata.Version != "20.0.0" {
				t.Fatalf("modified anonymous chart: %+v, %v", loaded, err)
			}
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
	base := func() fstest.MapFS {
		return fstest.MapFS{
			"Chart.yaml":                     {Data: []byte("apiVersion: v2\nname: parent\nversion: 1.0.0\n")},
			"charts/child/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
			"charts/child/templates/cm.yaml": {Data: []byte("old")},
		}
	}
	t.Run("one policy read and exact handoff", func(t *testing.T) {
		input := base()
		input[".helmignore"] = &fstest.MapFile{Data: []byte("# first\n")}
		reads := map[string]int{}
		observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
			reads[name]++
			return input.Open(name)
		}}
		files, fp, err := chartFilesInFS(observed)
		assert.NoError(t, err)
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
		assert.Equal(t, fp, manifest.Fingerprint([][2]any{
			{".helmignore", []byte("# first\n")},
			{"Chart.yaml", input["Chart.yaml"].Data},
			{"charts/child/Chart.yaml", input["charts/child/Chart.yaml"].Data},
			{"charts/child/templates/cm.yaml", []byte("old")},
		}))
		_, same, err := chartFilesInFS(input)
		assert.NoError(t, err)
		assert.Equal(t, same, fp)
		input["charts/child/templates/cm.yaml"].Data = []byte("new")
		input[".helmignore"].Data = []byte("# second\n")
		ch, err := loader.LoadFiles(files)
		assert.NoError(t, err)
		assert.Equal(t, string(ch.Dependencies()[0].Templates[0].Data), "old")
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
		_, next, err := chartFilesInFS(input)
		assert.NoError(t, err)
		assert.Equal(t, next == fp, false)
		later, laterFP, _, err := loadFSChart(input, ".")
		assert.NoError(t, err)
		assert.Equal(t, laterFP, next)
		assert.Equal(t, string(later.Dependencies()[0].Templates[0].Data), "new")
		clear(reads)
		cli, err := NewClient(newTestCache(t, ""))
		assert.NoError(t, err)
		st := store.New()
		hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository, RepoName: "repo"}}
		st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed})
		cli.SetSourceResolver(NewStoreSourceResolver(st))
		loaded, err := cli.loadChart(t.Context(), hr)
		assert.NoError(t, err)
		assert.Equal(t, string(loaded.chart.Dependencies()[0].Templates[0].Data), "new")
		assert.Equal(t, reads[".helmignore"], 1)
		assert.Equal(t, reads["charts/child/templates/cm.yaml"], 1)
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
			input := base()
			input[".helmignore"] = &fstest.MapFile{Data: []byte(".helmignore\n")}
			input["a"] = &fstest.MapFile{Data: []byte("bc")}
			files, before, err := chartFilesInFS(input)
			assert.NoError(t, err)
			for _, file := range files {
				if file.Name == ".helmignore" {
					t.Fatal("self-excluded policy admitted")
				}
			}
			tc.change(input)
			_, after, err := chartFilesInFS(input)
			assert.NoError(t, err)
			assert.Equal(t, before == after, tc.same)
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
			cli, err := NewClient(newTestCache(t, ""))
			assert.NoError(t, err)
			st := store.New()
			hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", RepoGroup: manifest.SourceDomain, RepoKind: manifest.KindGitRepository, RepoName: "repo"}}
			st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: observed, Key: "untrusted"})
			cli.SetSourceResolver(NewStoreSourceResolver(st))
			_, err = cli.loadChart(t.Context(), hr)
			if err == nil {
				t.Fatal("failed input accepted")
			}
			if (failure == "read" || failure == "close") && !errors.Is(err, want) {
				t.Fatalf("lost read/close error: %v", err)
			}
			if failure == "disappeared" {
				_, _, missing, err := loadFSChart(observed, ".")
				assert.Equal(t, missing, false)
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			assert.Equal(t, len(cli.chartCache), 0)
			broken = false
			input["Chart.yaml"].Data = []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")
			delete(input, ".helmignore")
			_, err = cli.loadChart(t.Context(), hr)
			assert.NoError(t, err)
			assert.Equal(t, len(cli.chartCache), 1)
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
