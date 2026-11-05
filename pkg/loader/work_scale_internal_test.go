package loader

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestTraversal_FileScaleWork(t *testing.T) {
	t.Parallel()
	for _, count := range []int{10, 100, 1000, 5000} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var resources strings.Builder
			for i := range count {
				name := fmt.Sprintf("cm-%04d.yaml", i)
				fmt.Fprintf(&resources, "- %s\n", name)
				testutil.WriteFile(
					t,
					root,
					name,
					fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm-%d}\n", i),
				)
			}
			testutil.WriteFile(t, root, "kustomization.yaml", "resources:\n"+resources.String())
			testutil.WriteFile(
				t,
				root,
				"unrelated.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n",
			)
			work := &manifest.WorkRecord{}
			walk := traversal{
				work:      work,
				documents: readerForTest(root),
				policy:    traversalScope,
				visited:   map[graphContext]struct{}{},
			}
			visits := map[string]int{}
			walk.walk(t.Context(), graphVisit{root: root, dir: "."}, func(v graphVisit) bool {
				visits[v.coordinate]++
				return true
			})
			assert.NoError(t, walk.err)
			assert.Equal(t, len(visits), count+1)
			assert.Equal(t, visits["unrelated.yaml"], 0)
			assert.Equal(t, visits["kustomization.yaml"], 1)
			for i := range count {
				assert.Equal(t, visits[fmt.Sprintf("cm-%04d.yaml", i)], 1)
			}
			assert.Equal(
				t,
				work.Counts,
				manifest.WorkCounts{FileEntries: uint64(count + 1), ReaderEdges: uint64(count)},
			)
		})
	}
}

func TestLoad_WorkCensusLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"} {
		testutil.WriteFile(t, root, name+".yaml", testutil.KustomizationYAML(name, "apps", "  path: ./"+name+"\n"))
		testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: []\n")
	}
	st := store.New()
	var recorder manifest.WorkRecorder
	recorder.Key.Tree = manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}
	st.SetWorkTree(recorder.Key.Tree, recorder.Scope())
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection.Roots = []manifest.NamedResource{{Namespace: "apps"}}
	_, err := l.Load(
		t.Context(),
		root,
		manifest.NamedResource{},
		recorder.Scope().Record(manifest.WorkPreparation, root),
	)
	assert.NoError(t, err)
	for range 2 {
		rows := l.Kustomizations()
		assert.Equal(t, len(rows), 9)
		for i := range rows {
			target := filepath.Join(root, rows[i].Obj.Named().Name)
			_, loadErr := l.Load(
				t.Context(),
				target,
				manifest.NamedResource{},
				recorder.Scope().Record(manifest.WorkPreparation, target),
			)
			assert.NoError(t, loadErr)
		}
	}
	failed := New(st)
	failed.SourceRoot = root
	failed.IgnoreFile = filepath.Join(root, "one")
	_, err = failed.Load(
		t.Context(),
		root,
		manifest.NamedResource{},
		recorder.Scope().Record(manifest.WorkPreparation, root),
	)
	if err == nil {
		t.Fatal("directory ignore file did not fail preparation")
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Equal(t, l.scopes == nil, true)
	var attempts, completed, failures uint64
	followed := 0
	for _, record := range recorder.Scope().Snapshot(0) {
		assert.Equal(t, record.Key.Tree, recorder.Key.Tree)
		attempts += record.Counts.Attempts
		completed += record.Counts.Completed
		failures += record.Counts.Failures
		if record.Key.Input != "" && record.Key.Input != root {
			assert.Equal(t, record.Counts.Attempts, uint64(1))
			followed++
		}
	}
	assert.Equal(t, followed, 9)
	assert.Equal(t, attempts, uint64(11))
	assert.Equal(t, completed, uint64(10))
	assert.Equal(t, failures, uint64(1))
}
