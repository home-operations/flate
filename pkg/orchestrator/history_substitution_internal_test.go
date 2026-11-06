package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func effectiveHistoryCases() []struct {
	name          string
	firstEmpty    bool
	first, second map[string]string
	same          bool
} {
	return []struct {
		name          string
		firstEmpty    bool
		first, second map[string]string
		same          bool
	}{
		{
			name:   "unused provider",
			second: map[string]string{"NAME": "unrelated", "USED": "kept", "OTHER": "second"},
			same:   true,
		},
		{name: "used provider", second: map[string]string{"NAME": "unrelated", "USED": "changed", "OTHER": "first"}},
		{name: "deactivation", second: map[string]string{}},
		{name: "optional appears", firstEmpty: true, second: map[string]string{"NAME": "unrelated", "USED": "kept"}},
		{name: "optional disappears"},
		{name: "emitted name added", second: map[string]string{"NAME": "wanted", "USED": "kept"}},
		{
			name:   "emitted name removed",
			first:  map[string]string{"NAME": "wanted", "USED": "kept"},
			second: map[string]string{"NAME": "unrelated", "USED": "kept"},
		},
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBuildChangeFilter_EffectiveSubstitutionHistory(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, tc := range effectiveHistoryCases() {
		t.Run(tc.name, func(t *testing.T) {
			testBuildChangeFilterEffectiveSubstitutionHistoryCase(
				t,
				&tc,
			)
		},
		)
	}
}

func writeSubstitutionHistoryCluster(t *testing.T, root string) {
	t.Helper()
	testutil.WriteFile(
		t,
		root,
		"flux/emitter.yaml",
		testutil.KustomizationYAML(
			"emitter",
			"apps",
			"  path: ./producers\n"+
				"  postBuild:\n"+
				"    substituteFrom: [{kind: ConfigMap, name: supplied, optional: true}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"apps",
			"  path: ./app\n"+
				"  postBuild:\n"+
				"    substituteFrom: [{kind: ConfigMap, name: wanted, optional: true}]\n",
		),
	)
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producers/cm.yaml",
		"apiVersion: v1\n"+
			"kind: ConfigMap\n"+
			"metadata: {name: '${NAME:=unrelated}', namespace: apps}\n"+
			"data: {value: '${USED:=kept}'}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\n"+
			"kind: ConfigMap\n"+
			"metadata: {name: result, namespace: apps}\n"+
			"data: {value: literal}\n",
	)
}

func substitutionHistoryConfig(cacheDir string, variables map[string]string) Config {
	cfg := Config{CacheDir: cacheDir, Concurrency: 2}
	if variables != nil {
		data := make(map[string]any, len(variables))
		for name, value := range variables {
			data[name] = value
		}
		obj := &manifest.ConfigMap{Name: "supplied", Namespace: "apps", Data: data}
		cfg.Supply = []store.Declaration{
			{Obj: obj, Doc: testutil.ObjectDocument(obj), Origin: manifest.SuppliedDeclaration},
		}
	}
	return cfg
}

func testBuildChangeFilterEffectiveSubstitutionHistoryCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		firstEmpty bool
		first      map[string]string
		second     map[string]string
		same       bool
	},
) {
	t.Helper()

	root, cacheDir := t.TempDir(), t.TempDir()
	writeSubstitutionHistoryCluster(t, root)
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}

	owner := testutil.KustomizationID("apps", "emitter")
	consumer := testutil.KustomizationID("apps", "consumer")
	first := testCaseInput.first
	if first == nil && !testCaseInput.firstEmpty {
		first = map[string]string{"NAME": "unrelated", "USED": "kept", "OTHER": "first"}
	}
	warm, err := newTestOrchestrator(t, tree, substitutionHistoryConfig(cacheDir, first))
	assert.NoError(t, err)
	seed, err := warm.render(t.Context())
	assert.NoError(t, err)
	declaration := warm.store.GetObject(owner)
	seedOutput := outputByParent(seed.Manifests)[owner]
	assert.Equal(t, len(seedOutput), 1)
	warm.inputs.Close()
	warm.cfg.Network.Close()
	assert.NoError(t, warm.cfg.Cache.Close())

	tree.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
	fresh, err := newTestOrchestrator(t, tree, substitutionHistoryConfig(cacheDir, testCaseInput.second))
	assert.NoError(t, err)
	t.Log("history probe")
	assert.NoError(t, fresh.bootstrap(t.Context()))
	fresh.store.AddObject(declaration)
	reads := &store.ReadSet{}
	reads.Init(fresh.store, owner, nil)
	preparedHistory166 := fresh.controllers.PreviousRender(t.Context(), declaration, schedule.Result{})
	if preparedHistory166.Assessment != nil {
		defer preparedHistory166.Assessment.Close()
	}
	history, complete := preparedHistory166.Records, preparedHistory166.Complete
	assert.Equal(t, complete, testCaseInput.same)
	assert.Equal(t, history != nil, testCaseInput.same)
	reads.Close()
	if testCaseInput.same {
		assert.Equal(t, fresh.filter.ShouldReconcile(owner), false)
	}
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	t.Log("changed-only render")
	head, err := fresh.collectResult(fresh.run(t.Context()))
	assert.NoError(t, err)
	if testCaseInput.same {
		assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	}
	fresh.stop()

	var selectedOutput []map[string]any
	if !testCaseInput.same {
		t.Log("forced selected render")
		selectedTree := tree
		selectedTree.ExternalChanges = nil
		selectedCfg := substitutionHistoryConfig(cacheDir, testCaseInput.second)
		selectedCfg.Selection = manifest.Selection{Roots: []manifest.NamedResource{owner}}
		selected, localErr := newTestOrchestrator(t, selectedTree, selectedCfg)
		assert.NoError(t, localErr)
		selectedResult, localErr := selected.render(t.Context())
		assert.NoError(t, localErr)
		selectedOutput = outputByParent(selectedResult.Manifests)[owner]
		assert.Equal(t, len(selectedOutput), 1)
	}
	t.Log("independent full render")
	tree.ExternalChanges = nil
	cfg := substitutionHistoryConfig(cacheDir, testCaseInput.second)
	cfg.CacheDir = t.TempDir()
	full, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	result, err := full.render(t.Context())
	assert.NoError(t, err)
	assert.Diff(t, outputByParent(head.Manifests)[consumer], outputByParent(result.Manifests)[consumer])
	assert.Diff(t, head.Failed, result.Failed)
	assert.Diff(t, head.Warnings, result.Warnings)
	if testCaseInput.same {
		assert.Diff(t, seedOutput, outputByParent(result.Manifests)[owner])
	} else {
		assert.Diff(t, selectedOutput, outputByParent(result.Manifests)[owner])
	}
	cfg.CacheDir = t.TempDir()
	again, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	deterministic, err := again.render(t.Context())
	assert.NoError(t, err)
	assert.Diff(t, outputByParent(result.Manifests), outputByParent(deterministic.Manifests))
	assert.Diff(t, result.Failed, deterministic.Failed)
	assert.Diff(t, result.Warnings, deterministic.Warnings)
}
