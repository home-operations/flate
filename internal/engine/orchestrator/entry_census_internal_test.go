package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_EntryQualificationUsesCensus(t *testing.T) {
	t.Parallel()
	for _, buildOnly := range []bool{false, true} {
		name := "file-indexed"
		if buildOnly {
			name = "build-only"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, changed := range []bool{false, true} {
				mode := "named"
				if changed {
					mode = "changed"
				}
				t.Run(mode, func(t *testing.T) {
					t.Parallel()
					testEntryQualificationUsesCensus(t, buildOnly, changed)
				})
			}
		})
	}
}

func testEntryQualificationUsesCensus(t *testing.T, buildOnly, changed bool) {
	t.Helper()
	root := t.TempDir()
	child := testutil.KustomizationID("apps", "child")
	builder := "resources: [child.yaml, parent.yaml]\n"
	if buildOnly {
		builder = "resources: [child.yaml]\nnamePrefix: qualified-\n"
		child.Name = "qualified-child"
	}
	tree := Tree{
		ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: filepath.Join(root, "entry"), RepoRoot: root,
	}
	for side, dir := range []string{root, t.TempDir()} {
		if !buildOnly {
			testutil.WriteFile(
				t, dir, "entry/parent.yaml",
				testutil.KustomizationYAML("parent", "apps", "  path: ./entry\n"),
			)
		}
		testutil.WriteFile(t, dir, "entry/kustomization.yaml", builder)
		testutil.WriteFile(
			t,
			dir,
			"entry/child.yaml",
			testutil.KustomizationYAML("child", "apps", "  path: ./payload\n"),
		)
		testutil.WriteFile(t, dir, "payload/kustomization.yaml", "resources: [value.yaml]\n")
		testutil.WriteFile(
			t,
			dir,
			"payload/value.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: "+[]string{"head", "base"}[side]+"}\n",
		)
		if side == 1 && changed {
			tree.BaseInput = testDirectoryInput(t, dir)
		}
	}
	assertEntryCensusResult(t, &tree, child, buildOnly)
}

func assertEntryCensusResult(t *testing.T, tree *Tree, child manifest.NamedResource, buildOnly bool) {
	t.Helper()
	o, err := newTestOrchestrator(t, *tree, Config{
		CacheDir: t.TempDir(), Concurrency: 2, RecordWork: true,
		Selection: selection.Request{Roots: []manifest.NamedResource{child}, Changed: tree.BaseInput != nil},
	})
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	assert.NoError(t, o.run(t.Context()))
	assert.Equal(t, o.store.GetDeclaration(child) != nil, true)
	assert.Equal(t, o.store.HasInventory(child), true)
	native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
	assert.Equal(t, o.store.HasInventory(native), buildOnly)
	assert.Equal(t, o.selection.Snapshot().Reconcile(native), buildOnly)
	if !buildOnly {
		builds := witnessWork(o.freezeWork(), manifest.WorkKustomize)
		assert.Equal(t, builds[native], uint64(0))
	}
}
