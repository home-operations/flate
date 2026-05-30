package helm

import (
	"context"
	"fmt"
	"strings"
	"testing"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkTemplate_AppTemplateChart measures helm.Template against
// the testdata/simple/charts/mychart chart referenced in the plan.
// Each b.Loop iteration runs one Template — the same shape every HR
// reconcile drives.
//
// With the template-output cache wired (NewClient's default) this
// benchmark exercises the cache HIT path after the first iteration:
// the first Template call populates the cache, every subsequent one
// serves from memory. Compare BenchmarkTemplate_AppTemplateChartUncached
// for the miss-path baseline.
func BenchmarkTemplate_AppTemplateChart(b *testing.B) {
	chartDir := stageBenchChartDir(b)
	cli, err := NewClient(cacheroot.New(b.TempDir()))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(benchLocalChartResolver(b, "chart-repo", "flux-system", chartDir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}
	values := map[string]any{"greeting": "hello"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := cli.Template(ctx, hr, values, Options{})
		if err != nil {
			b.Fatalf("Template: %v", err)
		}
		if !strings.Contains(out, "demo-cm") {
			b.Fatalf("missing rendered name")
		}
	}
}

// BenchmarkTemplate_AppTemplateChartUncached measures helm.Template
// on the cache MISS path — TemplateCacheBytes=0 disables the cache
// so every iteration runs action.Install.RunWithContext end to end.
// This is the Phase 2.2 baseline; comparing against
// BenchmarkTemplate_AppTemplateChart (cache hit) quantifies the
// cache's wall-time + allocation savings on warm renders.
func BenchmarkTemplate_AppTemplateChartUncached(b *testing.B) {
	chartDir := stageBenchChartDir(b)
	cli, err := NewClientWithOptions(cacheroot.New(b.TempDir()), ClientOptions{TemplateCacheBytes: 0})
	if err != nil {
		b.Fatalf("NewClientWithOptions: %v", err)
	}
	cli.SetSourceResolver(benchLocalChartResolver(b, "chart-repo", "flux-system", chartDir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}
	values := map[string]any{"greeting": "hello"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := cli.Template(ctx, hr, values, Options{})
		if err != nil {
			b.Fatalf("Template: %v", err)
		}
		if !strings.Contains(out, "demo-cm") {
			b.Fatalf("missing rendered name")
		}
	}
}

// BenchmarkLoadChart_Cached measures LoadChart on a warm chart — the
// mtime+size fingerprint hits and the cached pointer (cloned for
// per-call mutation safety) returns. The clone walk is the dominant
// cost on the cached path.
func BenchmarkLoadChart_Cached(b *testing.B) {
	chartDir := stageBenchChartDir(b)
	cli, err := NewClient(cacheroot.New(b.TempDir()))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	cli.SetSourceResolver(benchLocalChartResolver(b, "chart-repo", "flux-system", chartDir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}
	ctx := context.Background()
	// Prime the cache so b.Loop measures the warm path.
	if _, err := cli.LoadChart(ctx, hr); err != nil {
		b.Fatalf("warm LoadChart: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := cli.LoadChart(ctx, hr); err != nil {
			b.Fatalf("LoadChart: %v", err)
		}
	}
}

// BenchmarkMergeChartValuesFiles measures mergeChartValuesFiles for a
// chart with 10 named values files, simulating the Flux HR pattern
// where spec.chart.spec.valuesFiles enumerates a stack of values
// layers.
func BenchmarkMergeChartValuesFiles(b *testing.B) {
	const n = 10
	files := make([]*chartcommon.File, 0, n)
	names := make([]string, 0, n)
	for i := range n {
		name := fmt.Sprintf("values-%d.yaml", i)
		body := fmt.Sprintf("layer-%d:\n  key: value-%d\nshared:\n  nested-%d: %d\n", i, i, i, i)
		files = append(files, &chartcommon.File{Name: name, Data: []byte(body)})
		names = append(names, name)
	}
	ch := &chart.Chart{
		Metadata: &chart.Metadata{Name: "mychart", Version: "0.1.0"},
		Files:    files,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := mergeChartValuesFilesUncached(ch, names, false); err != nil {
			b.Fatalf("mergeChartValuesFilesUncached: %v", err)
		}
	}
}

// stageBenchChartDir copies the testdata/simple/charts/mychart layout
// into a fresh temp dir under b.TempDir() so the bench is hermetic
// (the helm client's LoadChart caches by on-disk path; reusing the
// repo's checked-in chart would couple every run's cache to that
// path's mtime).
func stageBenchChartDir(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	testutil.WriteFile(b, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
description: minimal local chart used for flate E2E tests
version: 0.1.0
appVersion: "1.0"
`)
	testutil.WriteFile(b, dir, "mychart/values.yaml", `greeting: hi
replicas: 1
`)
	testutil.WriteFile(b, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
  namespace: {{ .Release.Namespace }}
data:
  greeting: {{ .Values.greeting | quote }}
  replicas: "{{ .Values.replicas }}"
`)
	return dir
}

// benchLocalChartResolver wires a store with a single GitRepository
// pointing at dir, then returns a StoreSourceResolver — mirrors the
// pattern in helm_test.go's localChartResolver but takes *testing.B.
func benchLocalChartResolver(b *testing.B, name, namespace, dir string) SourceResolver {
	b.Helper()
	st := store.New()
	gr := &manifest.GitRepository{Name: name, Namespace: namespace}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	return NewStoreSourceResolver(st)
}
