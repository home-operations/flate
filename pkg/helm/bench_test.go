package helm

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkOCIChartVersion_DirectReference(b *testing.B) {
	const revision = "6.15.0@sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ociChartVersion("6.15.0", revision); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTemplate_AppTemplateChart(b *testing.B) {
	benchmarkTemplateAppTemplateChart(b, false)
}

func BenchmarkTemplate_AppTemplateChart_Acquired(b *testing.B) {
	benchmarkTemplateAppTemplateChart(b, true)
}

func benchmarkTemplateAppTemplateChart(b *testing.B, acquired bool) {
	b.Helper()
	if acquired && !renderPersistenceEligible(b) {
		b.Skip("acquired render benchmark requires identified build metadata from a clean checkout")
	}
	chartDir := stageBenchChartDir(b)
	cli := NewClient(cachetest.New(b, b.TempDir(), sourcecache.New))
	input := os.DirFS(chartDir)
	if acquired {
		input = acquireChartFS(b, cli.cache, input)
	}
	cliSources := benchChartResolver(b, chartDir, input)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
	values := map[string]any{"greeting": "hello"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := cli.Template(ctx, hr, values, RenderOptions{Options: Options{}, Sources: cliSources})
		if err != nil {
			b.Fatalf("Template: %v", err)
		}
		if len(out) == 0 || out[0]["metadata"].(map[string]any)["name"] != "demo-cm" {
			b.Fatalf("missing rendered name")
		}
	}
}

// BenchmarkTemplate_AppTemplateChartUncached uses a memory owner, so each
// iteration runs action.Install.RunWithContext without retained completed output.
func BenchmarkTemplate_AppTemplateChartUncached(b *testing.B) {
	chartDir := stageBenchChartDir(b)
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
	cliSources := benchChartResolver(b, chartDir, os.DirFS(chartDir))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
	values := map[string]any{"greeting": "hello"}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := cli.Template(ctx, hr, values, RenderOptions{Options: Options{}, Sources: cliSources})
		if err != nil {
			b.Fatalf("Template: %v", err)
		}
		if len(out) == 0 || out[0]["metadata"].(map[string]any)["name"] != "demo-cm" {
			b.Fatalf("missing rendered name")
		}
	}
}

func BenchmarkTemplate_SourceValues(b *testing.B) {
	for _, tc := range []struct {
		name string
		disk bool
	}{
		{name: "WarmRender"},
		{name: "PersistedHit", disk: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if tc.disk && !renderPersistenceEligible(b) {
				b.Fatalf("persisted benchmark requires a clean VCS-stamped build: %s", sourcecache.RenderPersistenceDisabledReason())
			}
			dir := stageBenchChartDir(b)
			testutil.WriteFile(b, dir, "config/prod.yaml", "greeting: pinned\nreplicas: 3\n")
			root := ""
			if tc.disk {
				root = b.TempDir()
			}
			owner := cachetest.New(b, root, sourcecache.New)
			cli := NewClient(owner)
			input := acquireChartFS(b, owner, os.DirFS(dir))
			cliSources := benchChartResolver(b, dir, input)
			hr := newHR()
			hr.ChartValuesFiles = []string{"config/prod.yaml"}
			first, err := cli.Template(b.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
			if err != nil {
				b.Fatal(err)
			}
			if !hasRenderField(first, "greeting", "pinned") || !hasRenderField(first, "replicas", "3") {
				b.Fatal(first)
			}
			second, err := cli.Template(b.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
			if err != nil {
				b.Fatalf("warm render differs: %v", err)
			}
			assert.Diff(b, second, first)
			loaded, err := cli.describeChart(b.Context(), hr, cliSources)
			if err != nil || loaded.valuesPin == "" {
				b.Fatalf("source values are not pinned: %v", err)
			}
			if tc.disk {
				if loaded.record == nil {
					b.Fatal("warm render did not use persisted output")
				}
				witness := manifest.DeepCopyMap(first[0])
				witness["cacheWitness"] = "selected source values persisted hit"
				output := encodedRender(b, []map[string]any{witness})
				reader, _, _, err := owner.Blob(b.Context(), sourcecache.BlobKey{Content: loaded.record.key, PersistOnly: true}, sourcecache.Policy{}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					err := json.NewEncoder(w).Encode(renderRecord{Output: output, Analysis: loaded.record.Analysis})
					return sourcecache.ProducerResult{}, err
				})
				if err != nil {
					b.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					b.Fatal(err)
				}
				out, err := cli.Template(b.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources})
				if err != nil || len(out) != 1 || out[0]["cacheWitness"] != witness["cacheWitness"] {
					b.Fatalf("persisted output witness missing: %v", err)
				}
				b.Log("verified selected source values persisted hit")
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := cli.Template(b.Context(), hr, nil, RenderOptions{Options: Options{}, Sources: cliSources}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTemplate_CRDsValidation(b *testing.B) {
	const crd = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
`
	for _, count := range []int{1, 10} {
		for _, policy := range []string{"Create", "Skip"} {
			b.Run(fmt.Sprintf("%d/%s", count, policy), func(b *testing.B) {
				dir := stageBenchChartDir(b)
				for i := range count {
					testutil.WriteFile(b, dir, fmt.Sprintf("mychart/crds/%d.yaml", i), crd)
				}
				cli := NewClient(cachetest.New(b, "", sourcecache.New))
				cliSources := benchChartResolver(b, dir, os.DirFS(dir))
				hr := newHR()
				hr.CRDsPolicy = policy
				b.ReportAllocs()
				for b.Loop() {
					if _, err := cli.Template(b.Context(), hr, nil, RenderOptions{SkipCRDs: true, Sources: cliSources}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTemplate_CommonMetadataVariants(b *testing.B) {
	benchmarkTemplateCommonMetadataVariants(b, false)
}

func BenchmarkTemplate_CommonMetadataVariants_Acquired(b *testing.B) {
	benchmarkTemplateCommonMetadataVariants(b, true)
}

func benchmarkTemplateCommonMetadataVariants(b *testing.B, acquired bool) {
	b.Helper()
	if acquired && !renderPersistenceEligible(b) {
		b.Skip("acquired render benchmark requires identified build metadata from a clean checkout")
	}
	chartDir := stageBenchChartDir(b)
	cli := NewClient(cachetest.New(b, b.TempDir(), sourcecache.New))
	input := os.DirFS(chartDir)
	if acquired {
		input = acquireChartFS(b, cli.cache, input)
	}
	cliSources := benchChartResolver(b, chartDir, input)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
	values := map[string]any{"greeting": "hello"}
	ctx := context.Background()
	if _, err := cli.Template(ctx, hr, values, RenderOptions{Options: Options{}, Sources: cliSources}); err != nil {
		b.Fatalf("warm Template: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		i++
		// CommonMetadata MUST remain outside the completed-output key.
		hr.CommonMetadata = &helmv2.CommonMetadata{Labels: map[string]string{"rev": fmt.Sprintf("%d", i)}}
		out, err := cli.Template(ctx, hr, values, RenderOptions{Options: Options{}, Sources: cliSources})
		if err != nil {
			b.Fatalf("Template: %v", err)
		}
		if len(out) == 0 || out[0]["metadata"].(map[string]any)["name"] != "demo-cm" {
			b.Fatalf("missing rendered name")
		}
	}
}

// BenchmarkLoadChart_Cached validates mutable content and reuses its canonical chart.
func BenchmarkLoadChart_Cached(b *testing.B) {
	benchmarkLoadChartCached(b, false)
}

func BenchmarkLoadChart_Cached_Acquired(b *testing.B) {
	benchmarkLoadChartCached(b, true)
}

func benchmarkLoadChartCached(b *testing.B, acquired bool) {
	b.Helper()
	chartDir := stageBenchChartDir(b)
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
	input := os.DirFS(chartDir)
	if acquired {
		input = acquireChartFS(b, cli.cache, input)
	}
	cliSources := benchChartResolver(b, chartDir, input)

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}
	ctx := context.Background()
	if _, err := loadDecodedChart(ctx, cli, hr, cliSources); err != nil {
		b.Fatalf("warm loadChart: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := loadDecodedChart(ctx, cli, hr, cliSources); err != nil {
			b.Fatalf("loadChart: %v", err)
		}
	}
}

// BenchmarkMergeChartValuesFiles measures mergeChartValuesFiles for a
// chart with 10 named values files, simulating the Flux HR pattern
// where spec.chart.spec.valuesFiles enumerates a stack of values
// layers.
func BenchmarkMergeChartValuesFiles(b *testing.B) {
	cli := NewClient(cachetest.New(b, "", sourcecache.New))
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
		if _, err := cli.mergeChartValuesFiles(b.Context(), chartDescription{chart: ch}, names, false); err != nil {
			b.Fatalf("mergeChartValuesFiles: %v", err)
		}
	}
}

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

func benchChartResolver(b *testing.B, dir string, input fs.FS) Sources {
	b.Helper()
	st := store.New()
	gr := &manifest.GitRepository{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: input, LocalPath: dir,
	})
	return NewSources(st)
}

func BenchmarkTemplate_AppTemplateChartConstructed(b *testing.B) {
	benchmarkTemplateAppTemplateChart(b, false)
}

func BenchmarkTemplate_AppTemplateChartConstructed_Acquired(b *testing.B) {
	benchmarkTemplateAppTemplateChart(b, true)
}

func BenchmarkTemplate_AppTemplateChartUncachedConstructed(b *testing.B) {
	BenchmarkTemplate_AppTemplateChartUncached(b)
}

func BenchmarkTemplate_CommonMetadataVariantsConstructed(b *testing.B) {
	benchmarkTemplateCommonMetadataVariants(b, false)
}

func BenchmarkTemplate_CommonMetadataVariantsConstructed_Acquired(b *testing.B) {
	benchmarkTemplateCommonMetadataVariants(b, true)
}
