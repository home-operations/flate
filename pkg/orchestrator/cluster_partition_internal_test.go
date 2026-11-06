package orchestrator

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func writePartitionClusters(t *testing.T, root string, clusters []string, changed bool) {
	t.Helper()
	const selfURL = "https://fixture.invalid/partition"
	testutil.WriteFile(
		t,
		root,
		".taskfiles/bootstrap/Taskfile.yaml",
		"version: '3'\ntasks: {bootstrap: {cmds: [true]}}\n",
	)
	for _, cluster := range clusters {
		entry := "kubernetes/clusters/" + cluster
		testutil.WriteFile(
			t,
			root,
			entry+"/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n"+
				"metadata: {name: cluster, namespace: flux-system}\nspec: {url: "+selfURL+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			entry+"/instance.yaml",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\n"+
				"metadata: {name: flux, namespace: flux-system}\nspec:\n"+
				"  sync: {kind: GitRepository, name: cluster, url: "+selfURL+", ref: main, path: ./"+entry+"}\n",
		)
		testutil.WriteFile(t, root, entry+"/apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system",
			"  path: ./kubernetes/apps/"+cluster+"\n  sourceRef: {kind: GitRepository, name: cluster}\n"+
				"  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n"))
		testutil.WriteFile(t, root, entry+"/settings.yaml", "apiVersion: v1\nkind: ConfigMap\n"+
			"metadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: "+cluster+"}\n")
		testutil.WriteFile(t, root, "kubernetes/apps/"+cluster+"/kustomization.yaml", "resources: [cm.yaml]\n")
		value := "before"
		if changed && cluster == "main" {
			value = "after"
		}
		testutil.WriteFile(t, root, "kubernetes/apps/"+cluster+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\n"+
			"metadata: {name: workload}\ndata: {cluster: '${CLUSTER}', value: "+value+"}\n")
	}
}

func TestRenderTrees_ClusterPartitionRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	clusters := []string{"main", "test", "utility"}
	writePartitionClusters(t, root, clusters, false)
	tree := Tree{
		ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: root,
		SelfURLs: []string{"https://fixture.invalid/partition"},
	}
	var first []Rendered
	for repeat := range 2 {
		runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		assert.FatalIf(t, len(runs) != len(clusters), "rendered %d clusters, want %d", len(runs), len(clusters))
		for i, cluster := range clusters {
			run := runs[i]
			assert.Equal(t, run.ID.Cluster, manifest.ClusterID("kubernetes/clusters/"+cluster))
			assert.NoError(t, run.Err)
			assert.FatalIf(t, len(run.Result.Work) == 0, "cluster %s lost its work records", cluster)
			for _, record := range run.Result.Work {
				assert.Equal(t, record.Key.Tree, run.ID)
			}
			if i != 0 {
				assert.Equal(t, run.Store() == runs[i-1].Store(), false)
				assert.Equal(t, run.loader == runs[i-1].loader, false)
			}
			owner := testutil.KustomizationID("flux-system", "cluster-apps")
			declaration, ok := run.Store().Get[*manifest.Kustomization](owner)
			assert.FatalIf(t, !ok, "cluster %s lost its declaration", cluster)
			assert.Equal(t, declaration.Path, "./kubernetes/apps/"+cluster)
			settings, ok := run.Store().Get[*manifest.ConfigMap](manifest.NamedResource{
				Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "settings",
			})
			assert.FatalIf(t, !ok, "cluster %s lost its substitutions", cluster)
			assert.Equal(t, settings.Data["CLUSTER"], any(cluster))
			source := run.Store().GetArtifact(declaration.SourceID(), store.ArtifactContent)
			assert.FatalIf(t, source == nil, "cluster %s lost its source", cluster)
			artifact, ok := source.(*store.SourceArtifact)
			assert.FatalIf(t, !ok, "cluster %s source content is unavailable", cluster)
			assert.Equal(t, artifact.LocalRoot, root)
			assert.Equal(t, artifact.InputAlias, true)
			docs := outputByParent(run.Result.Manifests)[owner]
			assert.FatalIf(t, len(docs) != 1, "rendered %d workload documents, want 1", len(docs))
			assert.Diff(t, docs[0]["data"], map[string]any{"cluster": cluster, "value": "before"})
			assert.FatalIf(t, run.entryBinding.Entry == nil || run.entryBinding.Entry.Receipt == nil,
				"cluster %s lost its application receipt", cluster)
			if repeat != 0 {
				assert.Diff(t, canonicalBatches(run.Result), canonicalBatches(first[i].Result))
			}
		}
		first = runs
	}
	writePartitionClusters(t, root, clusters, true)
	tree.ExternalChanges = change.NewSet([]string{"kubernetes/apps/main/cm.yaml"})
	runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != len(clusters), "rendered %d clusters, want %d", len(runs), len(clusters))
	owner := testutil.KustomizationID("flux-system", "cluster-apps")
	for i, run := range runs {
		docs := outputByParent(run.Result.Manifests)[owner]
		if i == 0 {
			assert.FatalIf(t, len(docs) != 1, "rendered %d workload documents, want 1", len(docs))
			assert.Diff(t, docs[0]["data"], map[string]any{"cluster": "main", "value": "after"})
		} else {
			assert.Equal(t, len(docs), 0)
		}
	}
}

func TestRenderTrees_ClusterPartitionUnion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		before, after []string
		identity      manifest.ClusterID
	}{
		{"three_to_one", []string{"main", "test", "utility"}, []string{"main"}, "."},
		{"one_to_three", []string{"main"}, []string{"main", "test", "utility"}, "."},
		{"explicit_three_to_one", []string{"main", "test", "utility"}, []string{"main"}, "chosen"},
		{"explicit_one_to_three", []string{"main"}, []string{"main", "test", "utility"}, "chosen"},
		{"addition", []string{"main", "test"}, []string{"main", "test", "utility"}, "."},
		{"deletion", []string{"main", "test", "utility"}, []string{"main", "test"}, "."},
		{"rename", []string{"main", "test"}, []string{"main", "renamed"}, "."},
		{"singleton_rename", []string{"test"}, []string{"renamed"}, "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := [2]string{t.TempDir(), t.TempDir()}
			trees := make([]Tree, 0, 2)
			for side, clusters := range [][]string{tc.before, tc.after} {
				writePartitionClusters(t, roots[side], clusters, false)
				trees = append(trees, Tree{
					ID:       manifest.TreeID{Cluster: tc.identity, Side: manifest.TreeSide(side)},
					RepoRoot: roots[side], SelfURLs: []string{"https://fixture.invalid/partition"},
				})
			}
			union := append(slices.Clone(tc.before), tc.after...)
			slices.Sort(union)
			union = slices.Compact(union)
			runs, err := RenderTrees(t.Context(), trees, Config{Concurrency: 2})
			assert.NoError(t, err)
			assert.FatalIf(t, len(runs) != len(union)*2, "rendered %d sides, want %d", len(runs), len(union)*2)
			assert.Equal(t, slices.IsSortedFunc(runs, func(a, b Rendered) int { return a.ID.Compare(b.ID) }), true)
			for _, cluster := range union {
				for side, present := range [][]string{tc.before, tc.after} {
					id := manifest.TreeID{
						Cluster: manifest.ClusterID("kubernetes/clusters/" + cluster),
						Side:    manifest.TreeSide(side),
					}
					if tc.identity != "." && cluster == "main" {
						id.Cluster = tc.identity
					}
					index := slices.IndexFunc(runs, func(run Rendered) bool { return run.ID == id })
					assert.FatalIf(t, index < 0, "comparison side %v is missing", id)
					run := runs[index]
					assert.Equal(t, run.ID, id)
					absent := !slices.Contains(present, cluster)
					assert.Equal(t, run.Absent, absent)
					assert.Equal(t, run.Orchestrator == nil, absent)
					assert.NoError(t, run.Err)
				}
			}
		})
	}
}

func TestRenderTrees_ExpandedDuplicateBeforeLaunch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writePartitionClusters(t, root, []string{"main", "test", "utility"}, false)
	trees := []Tree{
		{ID: manifest.TreeID{Cluster: "first", Side: manifest.TreeHead}, RepoRoot: root},
		{ID: manifest.TreeID{Cluster: "second", Side: manifest.TreeHead}, RepoRoot: root},
	}
	var previous string
	for range 2 {
		var updates atomic.Int32
		runs, err := RenderTrees(t.Context(), trees, Config{
			Concurrency:    2,
			StatusListener: func(_ manifest.TreeID, _ manifest.DiagnosticIdentity, _ store.StatusInfo) { updates.Add(1) },
		})
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"expanded duplicate lost input classification: %v", err)
		assert.Equal(t, updates.Load(), int32(0))
		for _, run := range runs {
			assert.Equal(t, run.Orchestrator == nil, true)
			assert.Equal(t, len(run.Result.Manifests), 0)
		}
		assert.Equal(t, strings.Contains(err.Error(), "duplicate tree kubernetes/clusters/main/1"), true)
		if previous != "" {
			assert.Equal(t, err.Error(), previous)
		}
		previous = err.Error()
		slices.Reverse(trees)
	}
}

func TestRenderTrees_ClusterPartitionExplicitIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writePartitionClusters(t, root, []string{"main", "test", "utility"}, false)
	id := manifest.TreeID{Cluster: "chosen", Side: manifest.TreeHead}
	runs, err := RenderTrees(t.Context(), []Tree{
		{
			ID:       id,
			RepoRoot: root,
			Path: filepath.Join(
				root,
				"kubernetes",
				"clusters",
				"main",
			),
			SelfURLs: []string{"https://fixture.invalid/partition"},
		},
	}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1, "rendered %d clusters, want 1", len(runs))
	assert.Equal(t, runs[0].ID, id)
}
