package orchestrator

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_EntryApplicationIntervalReceipt(t *testing.T) {
	root, original, cache := t.TempDir(), t.TempDir(), t.TempDir()
	const url = "https://example.test/entry/repo"
	for _, dir := range []string{root, original} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(
			t,
			dir,
			"entry/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
		)
		testutil.WriteFile(
			t,
			dir,
			"entry/generator.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/bootstrap\n      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n",
		)
		testutil.WriteFile(t, dir, "charts/bootstrap/Chart.yaml", "apiVersion: v2\nname: bootstrap\nversion: 1.0.0\n")
		application := "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: " + url + ", ref: main, path: ./entry}\n"
		testutil.WriteFile(
			t,
			dir,
			"charts/bootstrap/templates/application.yaml",
			"{{ print "+fmt.Sprintf("%q", application)+" }}",
		)
		testutil.WriteFile(
			t,
			dir,
			"entry/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  interval: 1h\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
			),
		)
		testutil.WriteFile(t, dir, "workload/kustomization.yaml", "resources: []\n")
	}
	before := testDirectoryInput(t, original)
	tree := Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}}
	cfg := Config{CacheDir: cache, Concurrency: 2}
	prime, err := newTestOrchestrator(t, tree, cfg)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		{
			_, localErr := prime.render(t.Context())
			assert.FatalIf(t, localErr != nil, "%v", localErr)
		}
	}
	if !prime.cache.CanPersistRenders() {
		t.Skip("warm receipt witness requires a clean VCS-stamped build")
	}
	testutil.WriteFile(
		t,
		root,
		"entry/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  interval: 2h\n  path: ./workload\n  sourceRef: {kind: GitRepository, name: self}\n",
		),
	)
	current := tree
	current.BaseInput, current.ExternalChanges = before, change.NewSet([]string{"entry/child.yaml"})
	warm, err := newTestOrchestrator(t, current, cfg)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		{
			innerLocalErr := warm.bootstrap(t.Context())
			assert.FatalIf(t, innerLocalErr != nil, "%v", innerLocalErr)
		}
	}
	id := testutil.HelmReleaseID("apps", "generator")
	calls, admitted := map[manifest.NamedResource]*atomic.Int64{
		id: {},
	}, map[manifest.NamedResource]*atomic.Int64{
		id: {},
	}
	warm.controllers.HelmRelease = countedNodeController{warm.hrc, calls, admitted}
	{
		{
			innerInnerLocalErr := warm.run(t.Context())
			assert.FatalIf(t, innerInnerLocalErr != nil, "%v", innerInnerLocalErr)
		}
	}
	if admitted[id].Load() != 0 || warm.store.GetArtifact(id, store.ArtifactContent) != nil {
		t.Fatalf(
			"warm interval edit rendered the applier producer: calls=%d admitted=%d",
			calls[id].Load(),
			admitted[id].Load(),
		)
	}
	check := func(run *Orchestrator) {
		t.Helper()
		_, _, emission := run.store.GetEmission(run.entryBinding.Producer)
		defer emission.Finish()
		for _, occurrence := range emission.Manifests {
			doc := occurrence.Document
			metadata, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == "Kustomization" && metadata["name"] == "child" {
				spec, _ := doc["spec"].(map[string]any)
				{
					assert.FatalIf(t, spec["interval"] != "2h", "stale interval: %v", spec)
				}
				return
			}
		}
		t.Fatal("root-layer Kustomization interval edit was omitted")
	}
	check(warm)
	full, err := newTestOrchestrator(t, tree, cfg)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		{
			_, innerInnerInnerLocalErr := full.render(t.Context())
			assert.FatalIf(t, innerInnerInnerLocalErr != nil, "%v", innerInnerInnerLocalErr)
		}
	}
	check(full)
	t.Logf("warm applier producer: calls=%d admitted=%d", calls[id].Load(), admitted[id].Load())
	testutil.WriteFile(
		t,
		root,
		"charts/bootstrap/templates/application.yaml",
		"{{ print "+fmt.Sprintf(
			"%q",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: flux, namespace: apps}\nspec:\n  sync: {name: self, kind: GitRepository, url: "+url+", ref: main, path: ./elsewhere}\n",
		)+" }}",
	)
	invalidated, err := newTestOrchestrator(t, current, cfg)
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		{
			err := invalidated.bootstrap(t.Context())
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	{
		assert.FatalIf(
			t,
			invalidated.entryBinding.Producer != (manifest.NamedResource{}),
			"%v",
			"changed applier input retained stale application evidence",
		)
	}
}
