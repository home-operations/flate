package loader

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_IndependentRoots(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"plain", "builder"} {
		t.Run(first+" first", func(t *testing.T) {
			t.Parallel()
			testLoaderIndependentRootsCase(t, &first)
		},
		)
	}

	t.Run(
		"retained native views",
		func(t *testing.T) { t.Parallel(); testLoaderIndependentRootsRetainedNativeViews(t) },
	)
	t.Run("admitted membership", func(t *testing.T) { t.Parallel(); testLoaderIndependentRootsAdmittedMembership(t) })
	for _, tc := range []struct {
		name    string
		bounded bool
		nested  bool
	}{
		{name: "independent"},
		{name: "bounded", bounded: true},
		{name: "nested", nested: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoaderIndependentRootsCase2(t, &tc)
		},
		)
	}
}

func testLoaderIndependentRootsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "plain/empty.txt", "")
	testutil.WriteFile(t, root, "builder/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"builder/ks.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
	)
	l := New(store.New())
	l.SourceRoot = root
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./builder"})
	second := "builder"
	if (*testCaseInput) == "builder" {
		second = "plain"
	}
	for _, dir := range []string{(*testCaseInput), second} {
		_, err := l.Load(t.Context(), filepath.Join(root, dir), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	l.Admit(root)
	child := testutil.KustomizationID("apps", "child")
	{
		_, ok := l.Index.Lookup(child)
		assert.FatalIf(t, !ok, "%s", "loaded child evidence missing")
	}
	assert.FatalIf(
		t,
		(l.Store.GetObject(child) != nil) != ((*testCaseInput) == "plain"),
		"%s",
		"later load changed first entry admission policy",
	)
	independent := New(store.New())
	independent.SourceRoot = root
	independent.Store.AddObject(l.Store.GetObject(testutil.KustomizationID("apps", "owner")))
	_, err := independent.Load(t.Context(), filepath.Join(root, "builder"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	independent.Admit(root)
	assert.FatalIf(
		t,
		independent.Store.GetObject(child) != nil,
		"%s",
		"independent loader inherited first entry policy",
	)
}

func testLoaderIndependentRootsRetainedNativeViews(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(t, root, name+"/ks.yaml", testutil.KustomizationYAML(name, "apps", "  {path: ./apps}\n"))
	}
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	artifactDirectory, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	artifact := artifactDirectory.Artifact()
	assert.NoError(t, artifactDirectory.Close())
	s := store.New()
	s.SetArtifact(manifest.BootstrapSourceID, artifact)
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, filepath.Join(root, "first"))
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	view := l.documents.views[root]
	observed, err := fs.Stat(view, "first")
	assert.NoError(t, err)
	assert.FatalIf(t, view == artifact.FS, "%s", "loader did not retain a private operation view")
	_, err = fs.ReadDir(view, "second")
	assert.NoError(t, err)
	_, err = fs.Stat(view, "second/late.yaml")
	assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "uncreated file error = %v, want not exist", err)
	testutil.WriteFile(
		t,
		root,
		"second/late.yaml",
		testutil.KustomizationYAML("late", "apps", "  {path: ./apps}\n"),
	)
	n, err = loadForTest(t.Context(), t, l, filepath.Join(root, "second"))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		n != 1,
		"second graph loaded %d objects, want one from retained directory observations",
		n,
	)
	assert.FatalIf(t, l.documents.views[root] != view, "%s", "unchanged artifact replaced the private view")
	retained, err := fs.Stat(l.documents.views[root], "first")
	assert.NoError(t, err)
	assert.FatalIf(t, retained != observed, "%s", "load discarded retained directory observations")
	for _, name := range []string{"first", "second"} {
		id := testutil.KustomizationID("apps", name)
		record, indexed := l.Index.Lookup(id)
		assert.FatalIf(
			t,
			!indexed || record.Path != filepath.Join(root, name, "ks.yaml") || s.GetObject(id) == nil,
			"%s graph was not loaded and indexed: %+v",
			name,
			record,
		)
	}
	{
		_, statErr := fs.Stat(l.documents.views[root], "second/late.yaml")
		assert.FatalIf(t, !errors.Is(statErr, fs.ErrNotExist), "load discarded the retained miss: %v", statErr)
	}
	other := New(s)
	_, err = other.Load(t.Context(), filepath.Join(root, "second"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		other.documents.views[root] == view || other.documents.views[root] == artifact.FS,
		"%s",
		"loaders shared their operation views",
	)
	{
		_, err := fs.Stat(other.documents.views[root], "second/late.yaml")
		assert.FatalIf(t, err != nil, "second loader inherited a private miss: %v", err)
	}
	assert.FatalIf(
		t,
		s.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != artifact,
		"%s",
		"loader replaced the source artifact",
	)
	{
		_, err := fs.Stat(artifact.FS, "second/late.yaml")
		assert.FatalIf(t, err != nil, "loader mutated the artifact's observations: %v", err)
	}
}

func testLoaderIndependentRootsAdmittedMembership(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "first.yaml", testutil.KustomizationYAML("first", "apps", "  {path: ./apps}\n"))
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admittedDirectory2, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	admitted := admittedDirectory2.Artifact()
	assert.NoError(t, admittedDirectory2.Close())
	input, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: admitted.FS},
	)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "second.yaml", testutil.KustomizationYAML("second", "apps", "  {path: ./apps}\n"))
	s := store.New()
	s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(s)
	n, err := loadForTest(t.Context(), t, l, root)
	assert.NoError(t, err)
	assert.Equal(t, n, 1)
	objects := s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
	assert.FatalIf(
		t,
		len(objects) != 1 || objects[0].Name != "first",
		"admitted objects = %v, want first only",
		objects,
	)
}

func testLoaderIndependentRootsCase2(t *testing.T, testCaseInput *struct {
	name    string
	bounded bool
	nested  bool
},
) {
	t.Helper()

	roots := []string{t.TempDir(), t.TempDir()}
	if testCaseInput.nested {
		roots[0] = filepath.Join(roots[1], "child")
	}
	ids := []manifest.NamedResource{
		testutil.KustomizationID("apps", "first"),
		testutil.KustomizationID("apps", "second"),
	}
	for i, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"ks.yaml",
			testutil.KustomizationYAML(ids[i].Name, ids[i].Namespace, "  {path: ./apps}\n"),
		)
	}
	l := New(store.New())
	if testCaseInput.bounded {
		l.SourceRoot = roots[0]
	}
	{
		_, err := loadForTest(t.Context(), t, l, roots[0])
		assert.NoError(t, err)
	}
	assert.FatalIf(t, l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil ||
		l.Store.GetObject(manifest.BootstrapSourceID) != nil, "%s", "standalone load published bootstrap identity")
	testLoaderIndependentRootsCase2Completion(
		t,
		&testLoaderIndependentRootsCase2CompletionState{testCaseInput: &testCaseInput, roots: &roots, ids: &ids, l: &l},
	)
}

type testLoaderIndependentRootsCase2CompletionState struct {
	testCaseInput **struct {
		name    string
		bounded bool
		nested  bool
	}
	roots *[]string
	ids   *[]manifest.NamedResource
	l     **Loader
}

func testLoaderIndependentRootsCase2Completion(t *testing.T, scope *testLoaderIndependentRootsCase2CompletionState) {
	t.Helper()
	view := (*scope.l).documents.views[(*scope.roots)[0]]
	observed, err := fs.Stat(view, ".")
	assert.NoError(t, err)
	for _, root := range []string{(*scope.roots)[1], (*scope.roots)[0]} {
		_, err := loadForTest(t.Context(), t, (*scope.l), root)
		if (*scope.testCaseInput).bounded && root == (*scope.roots)[1] {
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput),
				"outside SourceRoot error = %v, want unavailable source",
				err,
			)
			assert.FatalIf(
				t,
				(*scope.l).documents.views[(*scope.roots)[0]] != view,
				"%s",
				"outside SourceRoot load replaced the mounted view",
			)
			{
				_, indexed := (*scope.l).Index.Lookup(
					(*scope.ids)[1],
				)
				assert.FatalIf(
					t,
					indexed || (*scope.l).Store.GetObject((*scope.ids)[1]) != nil || len((*scope.l).Index.Records) != 1 ||
						len(
							retainedErrorsForTest((*scope.l)),
						) != 0,
					"%s",
					"unrelated root supplied objects or evidence outside SourceRoot",
				)
			}
		} else {
			assert.NoError(t, err)
		}
		retained, err := fs.Stat((*scope.l).documents.views[(*scope.roots)[0]], ".")
		assert.NoError(t, err)
		assert.FatalIf(t, retained != observed, "%s", "standalone binding discarded operation observations")
		assert.FatalIf(t, (*scope.l).Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil ||
			(*scope.l).Store.GetObject(
				manifest.BootstrapSourceID,
			) != nil, "%s", "independent load published bootstrap identity")
		if (*scope.testCaseInput).nested {
			inputFile := new((*scope.l).documents).input(filepath.Join((*scope.roots)[0], "ks.yaml"))
			input, file := inputFile.fs, inputFile.name
			assert.FatalIf(
				t,
				input != view || file != "ks.yaml",
				"child source binding = (%v, %q), want original view and ks.yaml",
				input,
				file,
			)
		}
		for i, id := range *scope.ids {
			if (*scope.testCaseInput).bounded && i == 1 {
				continue
			}
			r, indexed := (*scope.l).Index.Lookup(id)
			path := filepath.Join((*scope.roots)[i], "ks.yaml")
			file := filepath.ToSlash(path)
			if (*scope.testCaseInput).bounded {
				file = "ks.yaml"
			}
			assert.FatalIf(
				t,
				!indexed || r.Path != path || r.File != file || (*scope.l).Store.GetObject(id) == nil,
				"%s record = %+v, present = %t; want path %s, file %s and stored object",
				id,
				r,
				indexed,
				path,
				file,
			)
		}
	}
}
