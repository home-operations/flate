package helm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/engine"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

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

func TestLoadChart_CachedChartIsTrimmed(t *testing.T) {
	files := map[string][]byte{
		"app/Chart.yaml":         []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n"),
		"app/values.yaml":        []byte("value: same\n"),
		"app/templates/cm.yaml":  []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Values.value }}\n"),
		"app/charts/child.tgz":   buildChartTarGz(t, "child", "1.0.0"),
		"app/values.schema.json": []byte(`{"type":"object"}`),
		"app/README.md":          []byte("retained file"),
	}
	for _, origin := range []string{"directory", "archive"} {
		t.Run(origin, func(t *testing.T) {
			root := t.TempDir()
			var chartName string
			if origin == "archive" {
				chartName = "chart.tgz"
				testutil.WriteFile(t, root, chartName, string(chartArchive(t, files)))
			} else {
				chartName = "app"
				for n, data := range files {
					testutil.WriteFile(t, root, n, string(data))
				}
			}
			oracle, err := loader.Load(filepath.Join(root, chartName))
			if err != nil {
				t.Fatal(err)
			}
			s := store.New()
			id := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "source", Namespace: "default"}
			if origin == "archive" {
				id.Kind = manifest.KindHelmChart
			}
			s.SetArtifact(id, &store.SourceArtifact{LocalPath: root})
			c, err := NewClient(cacheroot.Layout{})
			if err != nil {
				t.Fatal(err)
			}
			c.SetSourceResolver(NewStoreSourceResolver(s))
			hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: chartName, RepoKind: id.Kind, RepoName: id.Name, RepoNamespace: id.Namespace}}
			res, err := c.LoadChart(t.Context(), hr)
			if err != nil {
				t.Fatal(err)
			}
			ch := res.Chart
			var check func(*chart.Chart)
			check = func(c *chart.Chart) {
				if c.Raw != nil {
					t.Fatal("cached raw chart input retained")
				}
				for _, f := range slices.Concat(c.Templates, c.Files) {
					if cap(f.Data) != len(f.Data) {
						t.Fatalf("template buffer %s: len=%d cap=%d", f.Name, len(f.Data), cap(f.Data))
					}
				}
				if cap(c.Schema) != len(c.Schema) {
					t.Fatalf("schema buffer: len=%d cap=%d", len(c.Schema), cap(c.Schema))
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
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("render differs (-want +got):\n%s", diff)
			}
			before, after := retainedChartBytes(oracle), retainedChartBytes(ch)
			if after >= before {
				t.Fatalf("retained bytes: before=%d after=%d", before, after)
			}
			t.Logf("retained file buffers: before=%d after=%d", before, after)
		})
	}
}
