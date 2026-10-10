package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/engine"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

func chartTree(t *testing.T, files map[string][]byte) *tree.Tree {
	t.Helper()
	b := tree.NewBuilder(nil)
	for n, data := range files {
		if err := b.AddFile(n, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tr, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func chartArchive(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	tw := tar.NewWriter(gz)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		content := files[name]
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func retainedChartBytes(ch *chart.Chart) int {
	total := len(ch.Schema)
	for _, f := range ch.Raw {
		total += cap(f.Data)
	}
	for _, f := range ch.Templates {
		total += cap(f.Data)
	}
	for _, f := range ch.Files {
		total += cap(f.Data)
	}
	for _, dep := range ch.Dependencies() {
		total += retainedChartBytes(dep)
	}
	return total
}

func TestLoadChart_ExactSizeAndNoRaw(t *testing.T) {
	files := map[string][]byte{
		"app/Chart.yaml":        []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n"),
		"app/values.yaml":       []byte("value: same\n"),
		"app/templates/cm.yaml": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Values.value }}\n"),
		"app/charts/child.tgz":  buildChartTarGz(t, "child", "1.0.0"),
	}
	for _, origin := range []string{"directory", "archive"} {
		t.Run(origin, func(t *testing.T) {
			var input fs.FS
			var name string
			var oracle *chart.Chart
			var err error
			if origin == "archive" {
				data := chartArchive(t, files)
				oracle, err = loader.LoadArchive(bytes.NewReader(data))
				input = chartTree(t, map[string][]byte{"chart.tgz": data})
				name = "chart.tgz"
			} else {
				root := t.TempDir()
				for n, data := range files {
					testutil.WriteFile(t, root, n, string(data))
				}
				oracle, err = loader.LoadDir(path.Join(root, "app"))
				input = chartTree(t, files)
				name = "app"
			}
			if err != nil {
				t.Fatal(err)
			}
			ch, err := loadChartFS(t.Context(), input, name)
			if err != nil {
				t.Fatal(err)
			}
			trimChart(ch)
			var check func(*chart.Chart)
			check = func(c *chart.Chart) {
				if c.Raw != nil {
					t.Fatal("cached raw chart input retained")
				}
				for _, f := range c.Templates {
					if cap(f.Data) != len(f.Data) {
						t.Fatalf("template buffer %s: len=%d cap=%d", f.Name, len(f.Data), cap(f.Data))
					}
				}
				for _, f := range c.Files {
					if cap(f.Data) != len(f.Data) {
						t.Fatalf("file buffer %s: len=%d cap=%d", f.Name, len(f.Data), cap(f.Data))
					}
				}
				for _, dep := range c.Dependencies() {
					check(dep)
				}
			}
			check(ch)
			values := map[string]any{"Values": ch.Values, "Release": map[string]any{"Name": "demo", "Namespace": "default"}}
			want, err := engine.Render(oracle, values)
			if err != nil {
				t.Fatal(err)
			}
			got, err := engine.Render(ch, values)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(want) != fmt.Sprint(got) {
				t.Fatalf("render differs: want=%v got=%v", want, got)
			}
			before, after := retainedChartBytes(oracle), retainedChartBytes(ch)
			if after >= before {
				t.Fatalf("retained bytes: before=%d after=%d", before, after)
			}
			t.Logf("retained file buffers: before=%d after=%d", before, after)
		})
	}
}

func TestLoadChart_Memory(t *testing.T) {
	files := map[string][]byte{"app/Chart.yaml": []byte("\xef\xbb\xbfapiVersion: v2\nname: app\nversion: 1.0.0\n"), "app/values.yaml": []byte("k: v\n"), "app/.helmignore": []byte("ignored/\n*.skip\n"), "app/ignored/bad": []byte("bad"), "app/bad.skip": []byte("bad"), "app/templates/cm.yaml": []byte("same")}
	tr := chartTree(t, files)
	ch, err := loadChartFS(t.Context(), tr, "app")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range ch.Files {
		if strings.Contains(f.Name, "bad") {
			t.Fatalf("ignored file loaded: %s", f.Name)
		}
	}
	root := t.TempDir()
	for n, data := range files {
		testutil.WriteFile(t, root, n, string(data))
	}
	want, err := loader.LoadDir(path.Join(root, "app"))
	if err != nil {
		t.Fatal(err)
	}
	if chartFingerprint(ch) != chartFingerprint(want) {
		t.Fatal("memory loader differs from upstream directory loader")
	}
	old := archive.MaxDecompressedChartSize
	t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
	for _, limit := range []int64{1, 10000} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			archive.MaxDecompressedChartSize = limit
			_, err := loadChartFS(t.Context(), tr, "app")
			if (err != nil) != (limit == 1) {
				t.Fatalf("budget %d: %v", limit, err)
			}
		})
	}
}

type countedChartFS struct {
	fs.FS
	reads atomic.Int32
}

func (c *countedChartFS) Open(n string) (fs.File, error) {
	if n == "app/templates/cm.yaml" {
		c.reads.Add(1)
	}
	return c.FS.Open(n)
}
func (c *countedChartFS) Stat(n string) (fs.FileInfo, error)      { return fs.Stat(c.FS, n) }
func (c *countedChartFS) Lstat(n string) (fs.FileInfo, error)     { return fs.Lstat(c.FS, n) }
func (c *countedChartFS) ReadDir(n string) ([]fs.DirEntry, error) { return fs.ReadDir(c.FS, n) }

func TestLoadChart_MemoryConcurrent(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			input := &countedChartFS{FS: chartTree(t, map[string][]byte{"app/Chart.yaml": []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n"), "app/values.yaml": []byte("k: v\n"), "app/templates/cm.yaml": []byte("same")})}
			s := store.New()
			id := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "source", Namespace: "default"}
			s.SetArtifact(id, &store.SourceArtifact{FS: input, Key: "pin+options+auth", LocalPath: "/absent-label"})
			c, err := NewClient(cacheroot.Layout{})
			if err != nil {
				t.Fatal(err)
			}
			c.SetSourceResolver(NewStoreSourceResolver(s))
			hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: "app", RepoKind: id.Kind, RepoName: id.Name, RepoNamespace: id.Namespace}}
			results := make([]ChartLoadResult, concurrency)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range concurrency {
				wg.Go(func() {
					<-start
					var err error
					results[i], err = c.LoadChart(t.Context(), hr)
					if err != nil {
						t.Error(err)
					}
				})
			}
			close(start)
			wg.Wait()
			if input.reads.Load() != 1 {
				t.Fatalf("chart parsed %d times", input.reads.Load())
			}
			for _, r := range results {
				if r.Chart == nil {
					t.Fatal("missing chart")
				}
				if r.Chart.Templates[0] != results[0].Chart.Templates[0] {
					t.Fatal("parsed bytes not shared")
				}
			}
			results[0].Chart.Values["k"] = "mutated"
			if results[1].Chart.Values["k"] != "v" {
				t.Fatal("render clone shared mutable values")
			}
		})
	}
}

func TestLoadChart_BaseFSLinks(t *testing.T) {
	b := tree.NewBuilder(chartTree(t, map[string][]byte{"app/Chart.yaml": []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n"), "shared/cm.yaml": []byte("same")}))
	if err := b.AddSymlink("app/templates", "../shared"); err != nil {
		t.Fatal(err)
	}
	tr, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	ch, err := loadChartFS(t.Context(), tr, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.Templates) != 1 || string(ch.Templates[0].Data) != "same" {
		t.Fatal("directory link not followed")
	}
	for _, target := range []string{"../../host", "/host", "loop"} {
		t.Run(target, func(t *testing.T) {
			b := tree.NewBuilder(tr)
			if err := b.AddSymlink("app/loop", target); err != nil {
				t.Fatal(err)
			}
			links, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			_, err = loadChartFS(t.Context(), links, "app")
			if !errors.Is(err, tree.ErrEscape) && !errors.Is(err, tree.ErrLoop) {
				t.Fatalf("link confinement: %v", err)
			}
		})
	}
}

func TestLoadChart_MemoryArchivePolicy(t *testing.T) {
	old := archive.MaxDecompressedChartSize
	t.Cleanup(func() { archive.MaxDecompressedChartSize = old })
	for _, tc := range []struct {
		name  string
		files map[string][]byte
		limit int64
		bad   bool
	}{
		{name: "valid", files: map[string][]byte{"app/Chart.yaml": []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n")}, limit: 100},
		{name: "parent", files: map[string][]byte{"app/../escape": []byte("ok")}, limit: 100, bad: true},
		{name: "absolute", files: map[string][]byte{"app//escape": []byte("ok")}, limit: 100, bad: true},
		{name: "windows-drive", files: map[string][]byte{"app/c:/escape": []byte("ok")}, limit: 100, bad: true},
		{name: "root-chart", files: map[string][]byte{"Chart.yaml": []byte("ok")}, limit: 100, bad: true},
		{name: "exact-limit", files: map[string][]byte{"app/Chart.yaml": []byte("ok")}, limit: 2, bad: true},
		{name: "empty", files: map[string][]byte{}, limit: 100, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			archive.MaxDecompressedChartSize = tc.limit
			data := chartArchive(t, tc.files)
			_, want := archive.LoadArchiveFiles(bytes.NewReader(data))
			_, got := loadChartFS(t.Context(), chartTree(t, map[string][]byte{"chart.tgz": data}), "chart.tgz")
			if (got != nil) != tc.bad || (want != nil) != (got != nil) {
				t.Fatalf("archive policy got=%v upstream=%v", got, want)
			}
		})
	}
}

func TestLoadChart_MemoryCorruptionLeavesLabel(t *testing.T) {
	root := t.TempDir()
	data := buildChartTarGz(t, "app", "1.0.0")
	testutil.WriteFile(t, root, "chart.tgz", string(data))
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindHelmChart, Name: "source", Namespace: "default"}
	c, err := NewClient(cacheroot.Layout{})
	if err != nil {
		t.Fatal(err)
	}
	c.SetSourceResolver(NewStoreSourceResolver(s))
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{RepoKind: id.Kind, RepoName: id.Name, RepoNamespace: id.Namespace}}
	for _, input := range []fs.FS{chartTree(t, map[string][]byte{"chart.tgz": []byte("corrupt")}), nil} {
		s.SetArtifact(id, &store.SourceArtifact{FS: input, Key: "pin", LocalPath: root})
		if _, err := c.LoadChart(t.Context(), hr); err == nil {
			t.Fatal("corrupt or released artifact read its label")
		}
		got, err := os.ReadFile(filepath.Join(root, "chart.tgz"))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("memory recovery removed host content: %v", err)
		}
	}
}
