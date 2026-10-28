package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_UnindexedSourceMaterializer(t *testing.T) {
	t.Parallel()
	root, artifact := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./payload\n  sourceRef: {kind: OCIRepository, name: app}\n"))
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: producer, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
	testutil.WriteFile(t, root, "flux/charts.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
	testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "foreign", "  path: ./unrelated\n  targetNamespace: foreign\n"))
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/demo/templates/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: app, namespace: apps}\nspec: {url: oci://fixture.invalid/app, interval: 1h, ref: {tag: latest}}\n")
	testutil.WriteFile(t, artifact, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, artifact, "payload/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output}\n")
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.HelmReleaseID("apps", "producer")
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "app"}
	var oracle []map[string]any
	for _, restricted := range []bool{false, true} {
		cfg := Config{Concurrency: 2}
		if restricted {
			cfg.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
		}
		o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}, cfg)
		assert.NoError(t, err)
		fetcher := &generatedOCIFetcher{root: artifact}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		assert.NoError(t, o.bootstrap(t.Context()))
		if restricted {
			_, indexed := o.index.Lookup(source)
			assert.Equal(t, indexed, false)
			assert.Equal(t, o.Store().GetObject(source) == nil, true)
			needed, selected, dirty, _ := o.filter.Membership(producer)
			assert.Equal(t, needed, true)
			assert.Equal(t, selected, false)
			assert.Equal(t, dirty, false)
		}
		result, err := o.render(t.Context())
		assert.NoError(t, err)
		assert.Equal(t, fetcher.fetched.Load(), true)
		info, _ := o.Store().GetStatus(consumer)
		assert.Equal(t, info.Outcome(), store.OutcomePassed)
		output := outputByParent(result.Manifests)[consumer]
		if !restricted {
			oracle = output
		} else {
			assert.Diff(t, output, oracle)
			_, dispatched := o.Store().GetStatus(testutil.KustomizationID("foreign", "unrelated"))
			assert.Equal(t, dispatched, false)
		}
	}
}

func TestRender_OpaqueSelectionDemand(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: factory, namespace: apps}
spec:
  inputs: [{name: child}]
  resourcesTemplate: |
    apiVersion: helm.toolkit.fluxcd.io/v2
    kind: HelmRelease
    metadata: {name: '<< inputs.name >>', namespace: apps}
    spec:
      interval: 1m
      chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}
`)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated}\ndata: {value: expected}\n")
	child := testutil.HelmReleaseID("apps", "child")
	var oracle []map[string]any
	for _, name := range []string{"full", "all_releases", "named_release"} {
		cfg := Config{Concurrency: 2}
		if name != "full" {
			selected := child
			if name == "all_releases" {
				selected.Name = ""
			}
			cfg.Selection = manifest.Selection{Roots: []manifest.NamedResource{selected}}
		}
		o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, cfg)
		assert.NoError(t, err)
		result, err := o.render(t.Context())
		assert.NoError(t, err)
		documents := outputByParent(result.Manifests)[child]
		if name == "full" {
			if len(documents) != 1 {
				t.Fatalf("full oracle output=%v", documents)
			}
			oracle = documents
		} else {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				assert.Diff(t, documents, oracle)
			})
		}
	}
}
