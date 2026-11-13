package orchestrator

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type namespaceFixture struct {
	tree     Tree
	artifact string
}

func namespaceChildrenFixture(tb testing.TB, peers int) namespaceFixture {
	tb.Helper()
	root, artifact := tb.TempDir(), tb.TempDir()
	testutil.WriteFile(tb, root, "flux/set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: factory, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: selected, namespace: apps}
    spec:
      interval: 1m
      path: ./payload
      sourceRef: {kind: OCIRepository, name: app, namespace: monitoring}
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: excluded, namespace: monitoring}
    spec: {interval: 1m, path: ./peer, targetNamespace: monitoring}
    ---
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: app, namespace: monitoring}
    spec: {interval: 1m, url: oci://fixture.invalid/app, ref: {tag: latest}}
    ---
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: excluded, namespace: monitoring}
    spec: {interval: 1m, url: oci://fixture.invalid/excluded, ref: {tag: latest}}
`)
	for i := range peers {
		testutil.WriteFile(
			tb,
			root,
			fmt.Sprintf("flux/peer-%d.yaml", i),
			testutil.KustomizationYAML(
				fmt.Sprintf("peer-%d", i),
				"monitoring",
				"  path: ./peer\n  targetNamespace: monitoring\n",
			),
		)
	}
	testutil.WriteFile(tb, root, "peer/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(tb, root, "peer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\n")
	testutil.WriteFile(tb, artifact, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		tb,
		artifact,
		"payload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected, namespace: apps}\ndata: {value: expected}\n",
	)
	return namespaceFixture{tree: Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, artifact: artifact}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_EmittedNamespaceChildren(t *testing.T) {
	namespaceChildrenFixtureResult1 := namespaceChildrenFixture(t, 2)
	tree, artifact := namespaceChildrenFixtureResult1.tree, namespaceChildrenFixtureResult1.artifact
	roots := []manifest.NamedResource{
		testutil.KustomizationID("apps", ""),
		testutil.HelmReleaseID("apps", ""),
		{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps"},
	}
	selected, excluded := testutil.KustomizationID(
		"apps",
		"selected",
	), testutil.KustomizationID(
		"monitoring",
		"excluded",
	)
	var oracle []map[string]any
	for _, mode := range []string{"full", "namespace", "namespace_again"} {
		cfg := Config{Concurrency: 2}
		if mode != "full" {
			cfg.Selection = selection.Request{Roots: roots}
		}
		o, err := newTestOrchestrator(t, tree, cfg)
		assert.NoError(t, err)
		fetcher := &generatedOCIFetcher{root: artifact}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		var renders atomic.Int64
		unsub := o.store.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, payload any) {
			if _, ok := payload.(*store.Emission); ok && id == excluded {
				renders.Add(1)
			}
		}, false)
		result, err := o.render(t.Context(), t)
		unsub()
		assert.NoError(t, err)
		output := outputByParent(result.Manifests)[selected]
		membershipResult1 := o.selection.Snapshot()
		needed, selectedRoot, dirty := membershipResult1.Reconcile(
			selected,
		), membershipResult1.Output(
			selected,
		), membershipResult1.Changed(
			selected,
		)
		t.Logf("%s: selected membership needed=%t selected=%t dirty=%t", mode, needed, selectedRoot, dirty)
		t.Logf(
			"%s: fetches=%d excluded renders=%d selected documents=%d",
			mode,
			fetcher.calls.Load(),
			renders.Load(),
			len(output),
		)
		if mode == "full" {
			oracle = output
			continue
		}
		assert.Diff(t, output, oracle)
		assert.Equal(t, len(output), 1)
		assert.Equal(t, fetcher.calls.Load(), int64(1))
		assert.Equal(t, renders.Load(), int64(0))
		for _, id := range []manifest.NamedResource{excluded, testutil.KustomizationID("monitoring", "peer-0"), testutil.KustomizationID("monitoring", "peer-1")} {
			membershipResult2 := o.selection.Snapshot()
			needed, selected, dirty := membershipResult2.Reconcile(
				id,
			), membershipResult2.Output(
				id,
			), membershipResult2.Changed(
				id,
			)
			assert.Equal(t, needed || selected || dirty, false)
			_, dispatched := o.store.GetStatus(id)
			assert.Equal(t, dispatched, false)
		}
		parent := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "factory",
		}
		_, _, batch := o.store.GetEmission(parent)
		assert.Equal(t, len(batch.Children), 4)
		assert.Equal(t, len(batch.Manifests), 4)
		batch.Finish()
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_UnindexedSourceMaterializer(t *testing.T) {
	root, artifact := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./payload\n  sourceRef: {kind: OCIRepository, name: app}\n",
		),
	)
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
	testutil.WriteFile(
		t,
		root,
		"flux/charts.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "foreign", "  path: ./unrelated\n  targetNamespace: foreign\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: app, namespace: apps}\nspec: {url: oci://fixture.invalid/app, interval: 1h, ref: {tag: latest}}\n",
	)
	testutil.WriteFile(t, artifact, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		artifact,
		"payload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output}\n",
	)
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.HelmReleaseID("apps", "producer")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "app",
	}
	var oracle []map[string]any
	for _, restricted := range []bool{false, true} {
		cfg := Config{Concurrency: 2}
		if restricted {
			cfg.Selection = selection.Request{Roots: []manifest.NamedResource{consumer}}
		}
		o, err := newTestOrchestrator(
			t,
			Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}},
			cfg,
		)
		assert.NoError(t, err)
		fetcher := &generatedOCIFetcher{root: artifact}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		assert.NoError(t, o.bootstrap(t.Context()))
		if restricted {
			_, indexed := o.index.Lookup(source)
			assert.Equal(t, indexed, false)
			assert.Equal(t, o.Store().GetObject(source) == nil, true)
			membershipResult3 := o.selection.Snapshot()
			needed, selected, dirty := membershipResult3.Reconcile(
				producer,
			), membershipResult3.Output(
				producer,
			), membershipResult3.Changed(
				producer,
			)
			assert.Equal(t, needed, true)
			assert.Equal(t, selected, false)
			assert.Equal(t, dirty, false)
		}
		result, err := o.render(t.Context(), t)
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

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_OpaqueSelectionDemand(t *testing.T) {
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
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated}\ndata: {value: expected}\n",
	)
	child := testutil.HelmReleaseID("apps", "child")
	var oracle []map[string]any
	testutil.WriteFile(
		t,
		root,
		"flux/indexed.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: indexed, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n",
	)
	for _, name := range []string{"full", "all_releases", "named_release"} {
		cfg := Config{Concurrency: 2}
		if name != "full" {
			selected := child
			if name == "all_releases" {
				selected.Name = ""
			}
			cfg.Selection = selection.Request{Roots: []manifest.NamedResource{selected}}
		}
		o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, cfg)
		assert.NoError(t, err)
		var dispatches atomic.Int64
		unsub := o.store.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, payload any) {
			if _, ok := payload.(*store.Emission); id == child && ok {
				dispatches.Add(1)
			}
		}, false)
		result, err := o.render(t.Context(), t)
		unsub()
		assert.NoError(t, err)
		documents := outputByParent(result.Manifests)[child]
		membershipResult4 := o.selection.Snapshot()
		needed, selected, dirty := membershipResult4.Reconcile(
			child,
		), membershipResult4.Output(
			child,
		), membershipResult4.Changed(
			child,
		)
		t.Logf("%s: child membership needed=%t selected=%t dirty=%t", name, needed, selected, dirty)
		t.Logf("%s: child dispatches=%d documents=%d", name, dispatches.Load(), len(documents))
		assert.Equal(t, dispatches.Load() > 0, true)
		if name == "full" {
			assert.FatalIf(t, len(documents) != 1, "full oracle output=%v", documents)
			oracle = documents
		} else {
			t.Run(name, func(t *testing.T) {
				testRenderOpaqueSelectionDemandCase2(
					t,
					testRenderOpaqueSelectionDemandCase2Inputs{oracle: &oracle, documents: &documents},
				)
			},
			)
		}
	}
}

type testRenderOpaqueSelectionDemandCaseInputs struct {
	oracle    *[]map[string]any
	documents *[]map[string]any
}

func testRenderOpaqueSelectionDemandCase(t *testing.T, testCaseInput testRenderOpaqueSelectionDemandCaseInputs) {
	t.Helper()

	assert.Diff(t, (*testCaseInput.documents), (*testCaseInput.oracle))
}

type testRenderOpaqueSelectionDemandCase2Inputs struct {
	oracle    *[]map[string]any
	documents *[]map[string]any
}

func testRenderOpaqueSelectionDemandCase2(t *testing.T, testCaseInput testRenderOpaqueSelectionDemandCase2Inputs) {
	t.Helper()
	testRenderOpaqueSelectionDemandCase(
		t,
		testRenderOpaqueSelectionDemandCaseInputs(testCaseInput),
	)
}
