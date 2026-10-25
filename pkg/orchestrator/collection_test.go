package orchestrator

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func writeCollectionEntry(t *testing.T, root, entry, value string) {
	t.Helper()
	testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [objects.yaml, ../shared.yaml]\n")
	testutil.WriteFile(t, root, "shared.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: apps}\ndata: {shared: content}\n")
	testutil.WriteFile(t, root, entry+"/objects.yaml", fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata: {name: cluster-vars, namespace: apps}
data: {value: %s}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: workload, namespace: apps}
spec:
  inputs: [{value: %s}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: output, namespace: apps}
      data: {value: '<< inputs.value >>'}
`, value, value))
}

func TestRenderTrees_CollectionPayloadIsolation(t *testing.T) {
	root := t.TempDir()
	writeCollectionEntry(t, root, "alpha", "first")
	writeCollectionEntry(t, root, "omega", "second")
	var first []*Result
	for run := range 2 {
		grouped, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: root}}, Config{Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(grouped), 2)
		for i, entry := range []string{"alpha", "omega"} {
			assert.Equal(t, grouped[i].ID, manifest.TreeID{Cluster: manifest.ClusterID(entry), Side: manifest.TreeHead})
			explicit, err := RenderTrees(t.Context(), []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: filepath.Join(root, entry), RepoRoot: root}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			assert.Diff(t, grouped[i].Result.Manifests, explicit[0].Result.Manifests)
			assert.Diff(t, grouped[i].Result.Failed, explicit[0].Result.Failed)
			vars := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "cluster-vars"}
			data, ok := grouped[i].Store().Get[*manifest.ConfigMap](vars)
			if !ok {
				t.Fatal("entry lost its native variables")
			}
			assert.Equal(t, data.Data["value"], any([]string{"first", "second"}[i]))
		}
		results := []*Result{grouped[0].Result, grouped[1].Result}
		if run == 0 {
			first = results
		} else {
			assert.Diff(t, results, first)
		}
	}
}

func TestRenderTrees_ClusterUnion(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after []string
	}{
		{"removed", []string{"alpha", "omega"}, []string{"alpha"}},
		{"added", []string{"alpha"}, []string{"alpha", "omega"}},
		{"renamed", []string{"alpha", "omega"}, []string{"alpha", "theta"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			for _, entry := range tc.before {
				writeCollectionEntry(t, before, entry, "old")
			}
			for _, entry := range tc.after {
				writeCollectionEntry(t, after, entry, "new")
			}
			runs, err := RenderTrees(t.Context(), []Tree{
				{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: before},
				{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: after},
			}, Config{Concurrency: 2})
			assert.NoError(t, err)
			roots := []manifest.NamedResource{{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "workload"}}
			for i := 0; i < len(runs); i += 2 {
				pair := runs[i : i+2]
				assert.Equal(t, pair[0].ID.Cluster, pair[1].ID.Cluster)
				assert.Equal(t, pair[0].ID.Side, manifest.TreeBase)
				assert.Equal(t, pair[1].ID.Side, manifest.TreeHead)
				for side, entries := range [][]string{tc.before, tc.after} {
					absent := !slices.Contains(entries, string(pair[side].ID.Cluster))
					assert.Equal(t, pair[side].Absent, absent)
					if absent && pair[side].Orchestrator != nil {
						t.Fatal("absent side constructed a tree")
					}
				}
				selected, err, _ := Select(pair, errors.Join(pair[0].Err, pair[1].Err), roots, nil)
				assert.NoError(t, err)
				changes, _, _, err := diff.Compare(
					diff.Snapshot{Manifests: selected[0].Manifests, Unavailable: selected[0].Unavailable},
					diff.Snapshot{Manifests: selected[1].Manifests, Unavailable: selected[1].Unavailable}, nil, diff.Options{Format: diff.FormatJSON})
				assert.NoError(t, err)
				if pair[0].Absent || pair[1].Absent {
					want := "removed"
					if pair[0].Absent {
						want = "added"
					}
					if !strings.Contains(string(changes), `"status": "`+want+`"`) || len(selected[0].Unavailable)+len(selected[1].Unavailable) != 0 {
						t.Fatalf("absent group lost %s: %s", want, changes)
					}
				}
			}
		})
	}
}

func TestRenderTrees_IndependentBootstrapFailure(t *testing.T) {
	failed, healthy := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, failed, "kustomization.yaml", "resources: [missing.yaml]\n")
	testutil.WriteFile(t, healthy, "kustomization.yaml", "resources: [object.yaml]\n")
	testutil.WriteFile(t, healthy, "object.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy}\n")
	runs, err := RenderTrees(t.Context(), []Tree{
		{ID: manifest.TreeID{Cluster: "failed", Side: manifest.TreeHead}, RepoRoot: failed},
		{ID: manifest.TreeID{Cluster: "healthy", Side: manifest.TreeHead}, RepoRoot: healthy},
	}, Config{Concurrency: 2})
	if err == nil || len(runs) != 2 || runs[0].Orchestrator != nil || runs[0].Result == nil || runs[1].Orchestrator == nil || runs[1].Err != nil || len(runs[1].Result.Manifests) == 0 {
		t.Fatalf("bootstrap failure affected sibling: runs=%+v error=%v", runs, err)
	}
}

func TestRenderTrees_DuplicateBeforeLaunch(t *testing.T) {
	var updates atomic.Int32
	tree := Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: t.TempDir()}
	results, err := RenderTrees(t.Context(), []Tree{tree, tree}, Config{Concurrency: 2, StatusListener: func(_ manifest.TreeID, _ manifest.NamedResource, _ store.StatusInfo) { updates.Add(1) }})
	if !errors.Is(err, manifest.ErrInput) || len(results) != 0 || updates.Load() != 0 {
		t.Fatalf("duplicate launched: %v %v", results, err)
	}
}
