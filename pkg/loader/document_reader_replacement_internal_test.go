package loader

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func testDocumentReaderSourceViewsAndReuseReplacedSource(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	for _, native := range []bool{false, true} {
		name := "map"
		if native {
			name = "native"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			l := New(st)
			l.SourceRoot = (*testCaseInput)
			var previous fs.FS
			var replacement *store.SourceArtifact
			for _, name := range []string{"first", "second"} {
				replaceSourceView(
					t,
					replacementViewInput{root: testCaseInput, native: native, store: st, loader: l, name: name},
					&previous,
					&replacement,
				)
			}
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: (*testCaseInput)})
			{
				_, err := loadForTest(t.Context(), t, l, (*testCaseInput))
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "nil replacement read host content: %v", err)
			}
			assert.FatalIf(t, len(l.Index.Records) != 0 ||
				len(
					st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization),
				) != 0, "%s", "nil replacement retained old objects or index records")
			st.SetArtifact(manifest.BootstrapSourceID, replacement)
			{
				n, err := loadForTest(t.Context(), t, l, (*testCaseInput))
				assert.FatalIf(t, err != nil || n != 1, "remounted source Load = %d, %v; want one object", n, err)
			}
			l.documents.read((*testCaseInput))

			st.SetArtifact(manifest.BootstrapSourceID, nil)
			l.mountSources()
			assert.FatalIf(t, new(l.documents).mountedRoot((*testCaseInput)) != "" || len(l.documents.cache) != 0 ||
				len(retainedErrorsForTest(l)) != 0 ||
				len(l.Index.Records) != 0 ||
				len(
					st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization),
				) != 0, "%s", "removed artifact retained a stale binding, document or index error")
		})
	}
}

func testDocumentReaderSourceViewsAndReuseLateNestedSource(t *testing.T) {
	t.Helper()

	root, unrelated := t.TempDir(), t.TempDir()
	for _, dir := range []string{root, unrelated} {
		testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("parent", "", "  {path: ./apps}\n"))
	}
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "child/ks.yaml", testutil.KustomizationYAML("stale", "", "  {path: ./apps}\n"))
	st := store.New()
	admitForTest(st, root)
	location, err := gitrepo.Locate(t.Context(), unrelated, unrelated)
	assert.NoError(t, err)
	artifactDirectory, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	artifact := artifactDirectory.Artifact()
	assert.NoError(t, artifactDirectory.Close())
	repo := &manifest.GitRepository{Name: "unrelated"}
	st.AddObject(repo)
	st.SetArtifact(repo.Named(), artifact)
	l := New(st)
	for _, dir := range []string{root, filepath.Join(root, "child"), unrelated} {
		{
			_, loadForTestErr := loadForTest(t.Context(), t, l, dir)
			assert.NoError(t, loadForTestErr)
		}
	}
	view := l.documents.views[unrelated]
	observed, err := fs.Stat(view, ".")
	assert.NoError(t, err)
	backing := t.TempDir()
	testutil.WriteFile(t, backing, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, backing, "ks.yaml", testutil.KustomizationYAML("nested", "apps", "  {path: ./apps}\n"))
	location, err = gitrepo.Locate(t.Context(), backing, backing)
	assert.NoError(t, err)
	artifactDirectory2, err := gittree.OpenDirectory(t.Context(), location)
	assert.NoError(t, err)
	artifact = artifactDirectory2.Artifact()
	assert.NoError(t, artifactDirectory2.Close())
	artifact.LocalPath = filepath.Join(root, "child")
	nested := &manifest.GitRepository{Name: "nested"}
	st.AddObject(nested)
	st.SetArtifact(nested.Named(), artifact)
	{
		n, loadForTestErr2 := loadForTest(t.Context(), t, l, filepath.Join(root, "child"))
		assert.FatalIf(
			t,
			loadForTestErr2 != nil || n != 1,
			"late mount Load = %d, %v; want one object",
			n,
			loadForTestErr2,
		)
	}
	assert.FatalIf(t, l.documents.views[artifact.LocalPath] == artifact.FS ||
		new(l.documents).mountedRoot(
			filepath.Join(root, "child", "ks.yaml"),
		) != artifact.LocalPath, "%s", "late native mount did not bind its own private operation view")
	id := testutil.KustomizationID("apps", "nested")
	record, indexed := l.Index.Lookup(id)
	assert.FatalIf(
		t,
		!indexed || record.Path != filepath.Join(root, "child", "ks.yaml") || st.GetObject(id) == nil,
		"nested mount did not supply its object and index: %+v",
		record,
	)
	id.Name = "stale"
	{
		_, indexed := l.Index.Lookup(id)
		assert.FatalIf(t, indexed || st.GetObject(id) != nil, "%s", "late nested mount retained parent-backed content")
	}
	retained, err := fs.Stat(l.documents.views[unrelated], ".")
	assert.FatalIf(
		t,
		err != nil || l.documents.views[unrelated] != view || retained != observed,
		"%s",
		"late nested mount replaced unrelated view observations",
	)
}

type replacementViewInput struct {
	root   *string
	native bool
	store  *store.Store
	loader *Loader
	name   string
}

//nolint:gocritic // The pointer retains the current filesystem for the next replacement comparison.
func replaceSourceView(
	t *testing.T,
	fixture replacementViewInput,
	previous *fs.FS,
	replacement **store.SourceArtifact,
) {
	t.Helper()
	input := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("namespace: " + fixture.name + "\nresources: [ks.yaml, child]\n")},
		"ks.yaml": {
			Data: []byte(testutil.KustomizationYAML(fixture.name, "", "  {path: ./apps}\n")),
		},
	}
	if fixture.name == "second" {
		input["child/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: replacement\n")}
	}
	artifact := &store.SourceArtifact{LocalPath: (*fixture.root), FS: input}
	if fixture.native {
		backing := t.TempDir()
		for file, contents := range input {
			testutil.WriteFile(t, backing, file, string(contents.Data))
		}
		location, err := gitrepo.Locate(t.Context(), backing, backing)
		assert.NoError(t, err)
		artifactDirectory3, err := gittree.OpenDirectory(t.Context(), location)
		assert.NoError(t, err)
		artifact = artifactDirectory3.Artifact()
		assert.NoError(t, artifactDirectory3.Close())
		artifact.LocalPath = (*fixture.root)
	}
	fixture.store.SetArtifact(manifest.BootstrapSourceID, artifact)
	{
		n, err := loadForTest(t.Context(), t, fixture.loader, (*fixture.root))
		assert.FatalIf(t, err != nil || n != 1, "%s Load = %d, %v; want one object", fixture.name, n, err)
	}
	view := fixture.loader.documents.views[(*fixture.root)]
	if fixture.native {
		assert.FatalIf(
			t,
			view == artifact.FS || view == *previous,
			"%s",
			"native replacement did not create a new private operation view",
		)
	} else {
		_, ok := view.(fstest.MapFS)
		assert.FatalIf(t, !ok, "%s", "non-native replacement wrapped the supplied filesystem")
	}
	*previous = view
	*replacement = artifact
	{
		k := fixture.loader.documents.read((*fixture.root)).body
		assert.FatalIf(
			t,
			k == nil || k.Namespace != fixture.name,
			"%s source retained a stale document: %v",
			fixture.name,
			k,
		)
	}
	child := fixture.loader.documents.read(filepath.Join((*fixture.root), "child")).body
	assert.FatalIf(t, fixture.name == "first" && child != nil ||
		fixture.name == "second" &&
			(child == nil || child.Namespace != "replacement"), "%s source retained a stale miss: %v", fixture.name, child)
	id := testutil.KustomizationID(fixture.name, fixture.name)
	{
		_, ok := fixture.loader.Index.Lookup(id)
		assert.FatalIf(
			t,
			!ok || fixture.store.GetObject(id) == nil,
			"%s replacement object was not stored and indexed",
			fixture.name,
		)
	}
	if fixture.name == "second" {
		id.Name, id.Namespace = "first", "first"
		{
			_, ok := fixture.loader.Index.Lookup(id)
			assert.FatalIf(
				t,
				ok || fixture.store.GetObject(id) != nil,
				"%s",
				"replacement retained the previous source's object or index",
			)
		}
	}
}

func TestDocumentReader_MountedRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	reader := documentReader{views: map[string]fs.FS{root: nil, nested: nil}}
	for _, tc := range []struct {
		name, file, want string
	}{
		{name: "root", file: root, want: root},
		{name: "child", file: filepath.Join(root, "file"), want: root},
		{name: "nested", file: filepath.Join(nested, "dir", "file"), want: nested},
		{name: "sibling prefix", file: filepath.Join(root, "nested-other", "file"), want: root},
		{name: "outside", file: root + "-other/file"},
		{name: "cleaned", file: nested + string(filepath.Separator) + string(filepath.Separator) + "file", want: nested},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, new(reader).mountedRoot(tc.file), tc.want)
		})
	}
}
