package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

var effectiveHistoryCases = []struct {
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

func TestBuildChangeFilter_EffectiveSubstitutionHistory(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, tc := range effectiveHistoryCases {
		t.Run(tc.name, func(t *testing.T) {
			root, cacheDir := t.TempDir(), t.TempDir()
			writeSubstitutionHistoryCluster(t, root)
			tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}

			owner := testutil.KustomizationID("apps", "emitter")
			consumer := testutil.KustomizationID("apps", "consumer")
			first := tc.first
			if first == nil && !tc.firstEmpty {
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
			warm = nil

			tree.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
			fresh, err := newTestOrchestrator(t, tree, substitutionHistoryConfig(cacheDir, tc.second))
			assert.NoError(t, err)
			t.Log("history probe")
			assert.NoError(t, fresh.bootstrap(t.Context()))
			fresh.store.AddObject(declaration)
			reads := &store.ReadSet{}
			reads.Init(fresh.store, owner, nil)
			history, complete := fresh.controllers.PreviousRender(t.Context(), declaration, reads)
			assert.Equal(t, complete, tc.same)
			assert.Equal(t, history != nil, tc.same)
			reads.Close()
			if tc.same {
				assert.Equal(t, fresh.filter.ShouldReconcile(owner), false)
			}
			assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
			t.Log("changed-only render")
			head, err := fresh.collectResult(fresh.run(t.Context()))
			assert.NoError(t, err)
			if tc.same {
				assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
			}
			fresh.stop()

			var selectedOutput []map[string]any
			if !tc.same {
				t.Log("forced selected render")
				selectedTree := tree
				selectedTree.ExternalChanges = nil
				selectedCfg := substitutionHistoryConfig(cacheDir, tc.second)
				selectedCfg.Selection = manifest.Selection{Roots: []manifest.NamedResource{owner}}
				selected, err := newTestOrchestrator(t, selectedTree, selectedCfg)
				assert.NoError(t, err)
				selectedResult, err := selected.render(t.Context())
				assert.NoError(t, err)
				selectedOutput = outputByParent(selectedResult.Manifests)[owner]
				assert.Equal(t, len(selectedOutput), 1)
			}
			t.Log("independent full render")
			tree.ExternalChanges = nil
			cfg := substitutionHistoryConfig(cacheDir, tc.second)
			cfg.CacheDir = t.TempDir()
			full, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			result, err := full.render(t.Context())
			assert.NoError(t, err)
			assert.Diff(t, outputByParent(head.Manifests)[consumer], outputByParent(result.Manifests)[consumer])
			assert.Diff(t, head.Failed, result.Failed)
			assert.Diff(t, head.Warnings, result.Warnings)
			if tc.same {
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
		})
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
		cfg.Supply = []store.SuppliedObject{
			{Object: &manifest.ConfigMap{Name: "supplied", Namespace: "apps", Data: data}},
		}
	}
	return cfg
}
