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
	for _, scenario := range []string{"declaration", "content", "owned_source"} {
		t.Run(scenario, func(t *testing.T) {
			declaration := scenario == "declaration"
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
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
				t.Cleanup(func() { slog.SetDefault(previous) })
				o, err := newTestOrchestrator(t, tree, cfg)
				assert.NoError(t, err)
				fetcher := &countedChartFetcher{chart: chart}
				o.src.Fetchers[manifest.KindOCIRepository] = fetcher
				assert.NoError(t, o.bootstrap(t.Context()))
				id := testutil.HelmReleaseID("apps", "generator")
				calls, admitted := map[manifest.NamedResource]*atomic.Int64{id: {}}, map[manifest.NamedResource]*atomic.Int64{id: {}}
				o.controllers.HelmRelease = countedNodeController{o.hrc, calls, admitted}
				assert.NoError(t, o.run(t.Context()))
				renders := 0
				for line := range bytes.Lines(logs.Bytes()) {
					var event struct {
						Message string `json:"msg"`
					}
					assert.NoError(t, json.Unmarshal(line, &event))
					if event.Message == "helm: render" {
						renders++
					}
				}
				slog.SetDefault(previous)
				if declaration {
					assert.Equal(t, o.diagnostic(o.entryBinding.Producer).Resource, manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "applies-entry"})
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
					if run == 0 || !o.cache.CanPersistRenders() {
						assert.Equal(t, fetcher.calls.Load(), int64(1))
						assert.Equal(t, renders, 1)
						assert.Equal(t, o.filter.ShouldReconcile(id), true)
					} else {
						assert.Equal(t, renders, 0)
						assert.Equal(t, fetcher.calls.Load(), int64(0))
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
				t.Logf("run=%d declaration=%t renders=%d fetches=%d admissions=%d persist=%t", run, declaration, renders, fetcher.calls.Load(), admitted[id].Load(), o.cache.CanPersistRenders())
				o.stop()
				assert.NoError(t, o.cache.Close())
			}
		})
	}
}
