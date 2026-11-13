package orchestrator

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testDependencyNamespaceHistory(t *testing.T, regime string) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			roots := [2]string{t.TempDir(), t.TempDir()}
			namespaces := [2]string{"old", "new"}
			if reverse {
				namespaces = [2]string{"new", "old"}
			}
			parent := func(namespace string) string {
				return testutil.KustomizationYAML(
					"parent",
					"apps",
					"  path: ./children\n  postBuild: {substitute: {TEAM: "+namespace+"}}\n",
				)
			}
			for side, root := range roots {
				testutil.WriteFile(t, root, "flux/parent.yaml", parent(namespaces[side]))
				writeDependencyNamespaceTree(t, root)
			}
			cacheDir := t.TempDir()
			if regime == "stale" {
				testutil.WriteFile(t, roots[0], "flux/parent.yaml", parent(namespaces[1]))
			}
			if regime == "present" || regime == "stale" {
				_, err := renderTreesForTest(
					t.Context(),
					t,
					[]Tree{
						{
							ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
							Path:     filepath.Join(roots[0], "flux"),
							RepoRoot: roots[0],
						},
					},
					Config{CacheDir: cacheDir, Concurrency: 2},
				)
				assert.NoError(t, err)
				testutil.WriteFile(t, roots[0], "flux/parent.yaml", parent(namespaces[0]))
			}
			if regime == "shallow" {
				repo := testutil.GitInit(t, roots[0])
				commit := testutil.GitCommitAll(
					t,
					repo,
					"base",
					&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
				)
				testutil.WriteFile(t, roots[0], ".git/shallow", commit.String()+"\n")
			}
			paired, err := renderTreesForTest(t.Context(), t, []Tree{
				{
					Path:     filepath.Join(roots[0], "flux"),
					RepoRoot: roots[0],
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				},
				{
					Path:     filepath.Join(roots[1], "flux"),
					RepoRoot: roots[1],
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				},
			}, Config{CacheDir: cacheDir, Concurrency: 2})
			assert.NoError(t, err)
			childID := testutil.KustomizationID("apps", "child")
			for side, run := range paired {
				child := run.Store().GetObject(childID).(*manifest.Kustomization)
				assert.Equal(t, child.DependsOn[0].Namespace, namespaces[side])
				status, known := run.Store().GetStatus(childID)
				assert.Equal(t, known, true)
				assert.Equal(t, status.Status, store.StatusReady)
				assert.Equal(
					t,
					run.selection.Snapshot().Reconcile(testutil.KustomizationID("apps", "unrelated")),
					false,
				)
				full, err := renderTreesForTest(
					t.Context(),
					t,
					[]Tree{
						{
							ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
							Path:     filepath.Join(roots[side], "flux"),
							RepoRoot: roots[side],
						},
					},
					Config{CacheDir: t.TempDir(), Concurrency: 2},
				)
				assert.NoError(t, err)
				assert.Diff(
					t,
					outputByParent(run.Result.Manifests)[childID],
					outputByParent(full[0].Result.Manifests)[childID],
				)
				assert.Diff(t, run.Result.Failed, full[0].Result.Failed)
			}
		})
	}
}

func writeDependencyNamespaceTree(t *testing.T, root string) {
	t.Helper()
	testutil.WriteFile(t, root, "children/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"children/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  path: ./work\n  dependsOn: [{name: ready, namespace: '${TEAM}'}]\n  postBuild: {substitute: {TEAM: '${TEAM}'}}\n",
		),
	)
	for _, namespace := range []string{"old", "new"} {
		testutil.WriteFile(
			t,
			root,
			"flux/ready-"+namespace+".yaml",
			testutil.KustomizationYAML("ready", namespace, "  path: ./ready-"+namespace+"\n"),
		)
		testutil.WriteFile(t, root, "ready-"+namespace+"/kustomization.yaml", "resources: []\n")
	}
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n"),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "work/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"work/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child, namespace: apps}\ndata: {value: '${TEAM}'}\n",
	)
}
