package orchestrator

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_EntryDeclarationQualifiesOpaqueApplier(t *testing.T) {
	for _, scenario := range []string{"declaration", "mutable_chart", "nested_chart", "nested_unchanged", "incomplete_inputs", "content", "owned_source"} {
		t.Run(scenario, func(t *testing.T) {
			declaration := scenario != "content" && scenario != "owned_source"
			nested := scenario == "nested_chart" || scenario == "nested_unchanged"
			workload := "workload"
			if scenario == "owned_source" {
				workload = "entry/workload"
			}
			root, original, chart := t.TempDir(), t.TempDir(), t.TempDir()
			const url = "https://example.test/entry/repo"
			for _, dir := range []string{root, original} {
				testutil.GitInit(t, dir)
				testutil.WriteFile(t, dir, "entry/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n")
				testutil.WriteFile(t, dir, "entry/chart.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {url: 'oci://fixture.invalid/chart', ref: {tag: 1.0.0}}\n")
				testutil.WriteFile(t, dir, "entry/generator.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n")
				testutil.WriteFile(t, dir, "entry/child.yaml", testutil.KustomizationYAML("child", "apps", "  interval: 1h\n  path: ./"+workload+"\n  sourceRef: {kind: GitRepository, name: self}\n"))
				testutil.WriteFile(t, dir, workload+"/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, dir, workload+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: old}\n")
				if scenario == "owned_source" {
					testutil.WriteFile(t, dir, workload+"/kustomization.yaml", "resources: [cm.yaml, input.yaml]\n")
					testutil.WriteFile(t, dir, workload+"/input.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: oci://fixture.invalid/input, ref: {tag: old}}\n")
				}
			}
			testutil.WriteFile(t, chart, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
			testutil.WriteFile(t, chart, "fixture/templates/application.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: applies-entry, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n")
			var charts map[manifest.NamedResource]string
			if nested {
				child := t.TempDir()
				charts = map[manifest.NamedResource]string{{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "nested-chart"}: child}
				for _, dir := range []string{root, original} {
					testutil.WriteFile(t, dir, "entry/nested-chart.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: nested-chart, namespace: apps}\nspec: {url: oci://fixture.invalid/nested, ref: {tag: 1.0.0}}\n")
				}
				testutil.WriteFile(t, child, "fixture/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
				testutil.WriteFile(t, child, "fixture/templates/application.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: '{{ .Values.applier }}', namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n")
				testutil.WriteFile(t, chart, "fixture/templates/application.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: nested, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: nested-chart}, values: {applier: applies-entry}}\n")
			}
			before := testDirectoryInput(t, original)
			file := workload + "/cm.yaml"
			if declaration {
				file = "entry/child.yaml"
				testutil.WriteFile(t, root, file, testutil.KustomizationYAML("child", "apps", "  interval: 1h\n  timeout: 2m\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n"))
			} else if scenario == "owned_source" {
				file = workload + "/input.yaml"
				testutil.WriteFile(t, root, file, "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: oci://fixture.invalid/input, ref: {tag: new}}\n")
			} else {
				testutil.WriteFile(t, root, file, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\ndata: {value: new}\n")
			}
			cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
			tree := Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}, BaseInput: before, ExternalChanges: change.NewSet([]string{file})}
			for run := range 2 {
				applier := "applies-entry"
				if scenario == "mutable_chart" && run == 1 {
					applier = "changed-applier"
					testutil.WriteFile(t, chart, "fixture/templates/application.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: changed-applier, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./entry}\n")
				}
				if scenario == "nested_chart" && run == 1 {
					applier = "changed-applier"
					testutil.WriteFile(t, chart, "fixture/templates/application.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: nested, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: nested-chart}, values: {applier: changed-applier}}\n")
				}
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				t.Cleanup(func() { slog.SetDefault(previous) })
				o, err := newTestOrchestrator(t, tree, cfg)
				assert.NoError(t, err)
				fetcher := &countedChartFetcher{chart: chart, charts: charts, incomplete: scenario == "incomplete_inputs"}
				o.src.Fetchers[manifest.KindOCIRepository] = fetcher
				assert.NoError(t, o.bootstrap(t.Context()))
				id := testutil.HelmReleaseID("apps", "generator")
				calls, admitted := map[manifest.NamedResource]*atomic.Int64{id: {}}, map[manifest.NamedResource]*atomic.Int64{id: {}}
				if nested {
					nested := testutil.HelmReleaseID("apps", "nested")
					calls[nested], admitted[nested] = new(atomic.Int64), new(atomic.Int64)
				}
				o.controllers.HelmRelease = countedNodeController{o.hrc, calls, admitted}
				assert.NoError(t, o.run(t.Context()))
				renders, reused := 0, 0
				for line := range bytes.Lines(logs.Bytes()) {
					var event struct {
						Message string `json:"msg"`
					}
					assert.NoError(t, json.Unmarshal(line, &event))
					if event.Message == "helm: render" {
						renders++
					}
					if event.Message == "discovery: entry application reused" {
						reused++
					}
				}
				slog.SetDefault(previous)
				if declaration {
					assert.Equal(t, o.diagnostic(o.entryBinding.Producer).Resource, manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: applier})
					_, _, output := o.store.GetEmission(o.entryBinding.Producer)
					found := false
					for _, occurrence := range output.Manifests {
						doc := occurrence.Document
						if doc["kind"] == manifest.KindKustomization && doc["metadata"].(map[string]any)["name"] == "child" {
							assert.Equal(t, doc["spec"].(map[string]any)["timeout"], any("2m"))
							found = true
						}
					}
					output.Finish()
					assert.Equal(t, found, true)
					root, ok := o.store.GetObject(o.entryBinding.Producer).(*manifest.Kustomization)
					if !ok {
						t.Fatalf("entry producer absent after qualification: %v\n%s", o.entryBinding, logs.String())
					}
					assert.Equal(t, root.EntryReceipt == nil, scenario == "incomplete_inputs")
					if root.EntryReceipt == nil && scenario != "incomplete_inputs" {
						t.Log(logs.String())
					}
					if run == 0 || scenario == "mutable_chart" || scenario == "nested_chart" || scenario == "incomplete_inputs" || !o.cache.CanPersistRenders() {
						expected := 1
						if nested {
							expected = 2
						}
						assert.Equal(t, fetcher.calls.Load(), int64(expected))
						if scenario != "incomplete_inputs" || run == 0 {
							assert.Equal(t, renders, expected)
						}
						assert.Equal(t, o.filter.ShouldReconcile(id), true)
					} else {
						assert.Equal(t, renders, 0)
						expected := int64(1)
						if nested {
							expected = 2
						}
						assert.Equal(t, fetcher.calls.Load(), expected)
						assert.Equal(t, admitted[id].Load(), int64(0))
					}
				} else {
					fetches := int64(0)
					if scenario == "owned_source" {
						fetches = 1
					}
					assert.Equal(t, renders, 0)
					assert.Equal(t, fetcher.calls.Load(), fetches)
					assert.Equal(t, admitted[id].Load(), int64(0))
				}
				wantReuse := 0
				if run == 1 && (scenario == "declaration" || scenario == "nested_unchanged") && o.cache.CanPersistRenders() {
					wantReuse = 1
				}
				assert.Equal(t, reused, wantReuse)
				t.Logf("run=%d declaration=%t renders=%d fetches=%d admissions=%d reused=%d persist=%t", run, declaration, renders, fetcher.calls.Load(), admitted[id].Load(), reused, o.cache.CanPersistRenders())
				o.stop()
				assert.NoError(t, o.cache.Close())
			}
		})
	}
}
