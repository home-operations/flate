package loader

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_DotDotPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path     string
		accepted bool
	}{
		{path: "..foo", accepted: true},
		{path: "..foo/bar", accepted: true},
		{path: "..foo/../../outside"},
		{path: "a/../../x"},
		{path: "./../x"},
		{path: ".."},
		{path: "../x"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			testLoaderDotDotPathsCase(t, &tc)
		},
		)
	}
}

func TestTraversal_RootCoordinates(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{"", ".", "./apps/..", "apps//child/../app", "/app", "https://example.com/app"} {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, filepath.Join(root, dir), "kustomization.yaml", "resources: []\n")
			w := traversal{documents: readerForTest(root), visited: map[graphContext]struct{}{}}
			context := graphContext{root: root, dir: dir}
			count := 0
			assert.FatalIf(t, !w.walk(t.Context(), graphVisit{graphContext: context}, func(v graphVisit) bool {
				assert.Equal(t, v.graphContext, context)
				count++
				return true
			}), "%s", "walk stopped before completing")
			assert.Equal(t, count, 1)
			assert.FatalIf(t, !w.walk(t.Context(), graphVisit{graphContext: context}, func(graphVisit) bool {
				count++
				return true
			}), "%s", "repeated walk stopped before completing")
			assert.Equal(t, count, 1)
		})
	}
}

func testLoaderDotDotPathsCase(t *testing.T, testCaseInput *struct {
	path     string
	accepted bool
},
) {
	t.Helper()

	root := filepath.Join(t.TempDir(), "repo")
	dir := filepath.Join(root, testCaseInput.path)
	testutil.WriteFile(
		t,
		dir,
		"kustomization.yaml",
		"configMapGenerator:\n- name: settings\n  literals: [key=value]\n",
	)
	documents := readerForTest(root)
	documents.read(dir)

	l := New(store.New())
	l.documents = documents
	var wantRoots []string
	if testCaseInput.accepted {
		{
			_, err := l.Load(t.Context(), dir, nil)
			assert.NoError(t, err)
		}
		wantRoots = []string{testCaseInput.path}
	}
	{
		got := l.packageRoots(root, nil)
		assert.ErrorIf(t, !slices.Equal(got, wantRoots), "packageRoots = %v, want %v", got, wantRoots)
	}

	w := traversal{documents: documents, visited: map[graphContext]struct{}{}}
	var files []string
	assert.FatalIf(
		t,
		!w.walk(t.Context(), graphVisit{root: root, dir: testCaseInput.path}, func(v graphVisit) bool {
			files = append(files, v.file)
			return true
		}),
		"%s",
		"walk stopped before completing",
	)
	var wantFiles []string
	if testCaseInput.accepted {
		wantFiles = []string{filepath.Join(dir, "kustomization.yaml")}
	}
	assert.ErrorIf(t, !slices.Equal(files, wantFiles), "walk files = %v, want %v", files, wantFiles)

	wantRef := ""
	if testCaseInput.accepted {
		wantRef = testCaseInput.path
	}
	if got, ok := kustpath.Resolve(
		".",
		testCaseInput.path,
		kustpath.Transformer,
	); ok != testCaseInput.accepted ||
		got != wantRef {
		t.Errorf("kustpath.Resolve = (%q, %v), want (%q, %v)", got, ok, wantRef, testCaseInput.accepted)
	}
	if got, ok := kustomizeinput.Local(
		".",
		".",
		testCaseInput.path,
	); ok != testCaseInput.accepted ||
		got != wantRef {
		t.Errorf("kustomizeinput.Local = (%q, %v), want (%q, %v)", got, ok, wantRef, testCaseInput.accepted)
	}
}
