package orchestrator

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

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
	for _, regime := range []string{"present", "missing", "stale", "shallow"} {
		t.Run("dependency namespaces/"+regime, func(t *testing.T) {
			testDependencyNamespaceHistory(t, regime)
		})
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""

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
	seed, err := warm.render(t.Context(), t)
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
	assert.Equal(t, complete, persistent && testCaseInput.same)
	assert.Equal(t, history != nil, persistent && testCaseInput.same)
	reads.Close()
	if testCaseInput.same {
		assert.Equal(t, fresh.selection.Snapshot().Reconcile(owner), false)
	}
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	t.Log("changed-only render")
	head, err := fresh.collectResult(t.Context(), fresh.run(t.Context()))
	t.Cleanup(head.Close)
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
		selectedCfg.Selection = selection.Request{Roots: []manifest.NamedResource{owner}}
		selected, localErr := newTestOrchestrator(t, selectedTree, selectedCfg)
		assert.NoError(t, localErr)
		selectedResult, localErr := selected.render(t.Context(), t)
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
	result, err := full.render(t.Context(), t)
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
	deterministic, err := again.render(t.Context(), t)
	assert.NoError(t, err)
	assert.Diff(t, outputByParent(result.Manifests), outputByParent(deterministic.Manifests))
	assert.Diff(t, result.Failed, deterministic.Failed)
	assert.Diff(t, result.Warnings, deterministic.Warnings)
	if testCaseInput.name == "optional appears" {
		obj := &manifest.ConfigMap{
			Name:      "supplied",
			Namespace: "apps",
			Data:      map[string]any{"NAME": "wanted", "USED": "kept"},
		}
		document, encodeErr := json.Marshal(testutil.ObjectDocument(obj))
		assert.NoError(t, encodeErr)
		testHistoryInputSides(t, &historyInputSides{
			root: root, file: "flux/supplied.yaml", document: document, consumer: consumer,
			files: map[string]string{
				"flux/emitter.yaml": testutil.KustomizationYAML(
					"emitter",
					"apps",
					"  path: ./producers\n  postBuild:\n    substitute: {NAME: wanted, UNUSED: enabled}\n    substituteFrom: [{kind: ConfigMap, name: supplied, optional: true}]\n",
				),
				"flux/consumer.yaml": testutil.KustomizationYAML(
					"consumer",
					"apps",
					"  path: ./app\n  postBuild:\n    substitute: {UNUSED: enabled}\n    substituteFrom: [{kind: ConfigMap, name: wanted, optional: true}]\n",
				),
				"producers/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: wanted, namespace: apps}\ndata: {value: '${USED:=absent}'}\n",
				"app/cm.yaml":       "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result, namespace: apps}\ndata: {value: '${value:=absent}'}\n",
			},
		})
	}
}

type historyInputSides struct {
	root, file string
	files      map[string]string
	document   []byte
	consumer   manifest.NamedResource
}

func testHistoryInputSides(t *testing.T, fixture *historyInputSides) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		name := "insertion"
		if reverse {
			name = "removal"
		}
		t.Run(name, func(t *testing.T) {
			testHistoryInputDirection(t, fixture, reverse)
		})
	}
}

func testHistoryInputDirection(t *testing.T, fixture *historyInputSides, reverse bool) {
	t.Helper()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 0, len(roots))
	oracles := make([]Rendered, 0, len(roots))
	wantError := false
	orphan := manifest.NamedResource{
		Kind:      manifest.KindConfigMap,
		Namespace: fixture.consumer.Namespace,
		Name:      "independent",
	}
	for side, root := range roots {
		assert.NoError(t, os.CopyFS(root, os.DirFS(fixture.root)))
		for _, path := range slices.Sorted(maps.Keys(fixture.files)) {
			testutil.WriteFile(t, root, path, fixture.files[path])
		}
		present := (side == 1) != reverse
		if present {
			testutil.WriteFile(t, root, fixture.file, string(fixture.document))
		} else if err := os.Remove(filepath.Join(root, fixture.file)); err != nil {
			assert.Equal(t, os.IsNotExist(err), true)
		}
		testutil.WriteFile(
			t,
			root,
			"flux/independent.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent, namespace: "+orphan.Namespace+"}\ndata: {value: independent}\n",
		)
		kind := manifest.TreeBase
		if side == 1 {
			kind = manifest.TreeHead
		}
		tree := Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: kind},
			RepoRoot: root,
			Path:     filepath.Join(root, "flux"),
			SelfURLs: []string{"https://fixture.invalid/repo"},
		}
		full, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		wantError = wantError || err != nil
		assert.Equal(t, len(full), 1)
		oracles = append(oracles, full[0])
		tree.ExternalChanges = change.NewSet([]string{fixture.file})
		trees = append(trees, tree)
	}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
	first, err := renderTreesForTest(t.Context(), t, trees, cfg)
	assert.Equal(t, err != nil, wantError)
	second, err := renderTreesForTest(t.Context(), t, trees, cfg)
	assert.Equal(t, err != nil, wantError)
	assert.Equal(t, len(first), len(oracles))
	assert.Equal(t, len(second), len(oracles))
	for side, oracle := range oracles {
		want, encodeErr := json.Marshal(outputByParent(oracle.Result.Manifests)[fixture.consumer])
		assert.NoError(t, encodeErr)
		for _, runs := range [][]Rendered{first, second} {
			run := &runs[side]
			got, encodeErr := json.Marshal(outputByParent(run.Result.Manifests)[fixture.consumer])
			assert.NoError(t, encodeErr)
			assert.Equal(t, string(got), string(want))
			assert.Equal(t, len(run.Result.Failed), len(oracle.Result.Failed))
			for id, expected := range oracle.Result.Failed {
				actual, failed := run.Result.Failed[id]
				assert.Equal(t, failed, true)
				assert.Equal(t, actual.Status, expected.Status)
				assert.Equal(t, actual.Reason, expected.Reason)
				assert.Equal(t, actual.Outcome(), expected.Outcome())
				subject := manifest.DiagnosticFor(id, nil)
				assert.Diff(t, actual.DependencyReason(subject), expected.DependencyReason(subject))
			}
			assert.Diff(t, run.Result.Warnings, oracle.Result.Warnings)
			record, known := run.index.Lookup(orphan)
			assert.Equal(t, known, true)
			assert.Equal(t, record.File, "flux/independent.yaml")
			assert.Equal(t, record.Path, filepath.Join(roots[side], record.File))
			for i := range record.Productions {
				assert.Equal(t, record.Productions[i].Owner, manifest.NamedResource{})
			}
			input := run.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, input.LocalPath, roots[side])
		}
	}
}
