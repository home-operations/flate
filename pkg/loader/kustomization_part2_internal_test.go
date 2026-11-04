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
				input := fstest.MapFS{
					"kustomization.yaml": {Data: []byte("namespace: " + name + "\nresources: [ks.yaml, child]\n")},
					"ks.yaml": {
						Data: []byte(testutil.KustomizationYAML(name, "", "  {path: ./apps}\n")),
					},
				}
				if name == "second" {
					input["child/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: replacement\n")}
				}
				artifact := &store.SourceArtifact{LocalPath: (*testCaseInput), FS: input}
				if native {
					backing := t.TempDir()
					for file, contents := range input {
						testutil.WriteFile(t, backing, file, string(contents.Data))
					}
					location, err := gitrepo.Locate(t.Context(), backing, backing)
					{
						assert.NoError(t, err)
					}
					artifact, err = gittree.AdmitDirectory(t.Context(), location)
					{
						assert.NoError(t, err)
					}
					artifact.LocalPath = (*testCaseInput)
				}
				st.SetArtifact(manifest.BootstrapSourceID, artifact)
				{
					n, err := loadForTest(t.Context(), t, l, (*testCaseInput))
					assert.FatalIf(t, err != nil || n != 1, "%s Load = %d, %v; want one object", name, n, err)
				}
				view := l.documents.views[(*testCaseInput)]
				if native {
					{
						assert.FatalIf(
							t,
							view == artifact.FS || view == previous,
							"%s",
							"native replacement did not create a new private operation view",
						)
					}
				} else {
					_, ok := view.(fstest.MapFS)
					assert.FatalIf(t, !ok, "%s", "non-native replacement wrapped the supplied filesystem")
				}
				previous = view
				replacement = artifact
				{
					k, _ := l.documents.read((*testCaseInput))
					assert.FatalIf(
						t,
						k == nil || k.Namespace != name,
						"%s source retained a stale document: %v",
						name,
						k,
					)
				}
				child, _ := l.documents.read(filepath.Join((*testCaseInput), "child"))
				{
					assert.FatalIf(t, name == "first" && child != nil ||
						name == "second" &&
							(child == nil || child.Namespace != "replacement"), "%s source retained a stale miss: %v", name, child)
				}
				id := testutil.KustomizationID(name, name)
				{
					_, ok := l.Index.Lookup(id)
					assert.FatalIf(
						t,
						!ok || st.GetObject(id) == nil,
						"%s replacement object was not stored and indexed",
						name,
					)
				}
				if name == "second" {
					id.Name, id.Namespace = "first", "first"
					{
						_, ok := l.Index.Lookup(id)
						assert.FatalIf(
							t,
							ok || st.GetObject(id) != nil,
							"%s",
							"replacement retained the previous source's object or index",
						)
					}
				}
			}
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: (*testCaseInput)})
			{
				_, err := loadForTest(t.Context(), t, l, (*testCaseInput))
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "nil replacement read host content: %v", err)
			}
			{
				assert.FatalIf(t, len(l.Index.Records) != 0 ||
					len(
						st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization),
					) != 0, "%s", "nil replacement retained old objects or index records")
			}
			st.SetArtifact(manifest.BootstrapSourceID, replacement)
			{
				n, err := loadForTest(t.Context(), t, l, (*testCaseInput))
				assert.FatalIf(t, err != nil || n != 1, "remounted source Load = %d, %v; want one object", n, err)
			}
			l.documents.read((*testCaseInput))

			st.SetArtifact(manifest.BootstrapSourceID, nil)
			l.mountSources()
			{
				assert.FatalIf(t, new(l.documents).mountedRoot((*testCaseInput)) != "" || len(l.documents.cache) != 0 ||
					len(retainedErrorsForTest(l)) != 0 ||
					len(l.Index.Records) != 0 ||
					len(
						st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization),
					) != 0, "%s", "removed artifact retained a stale binding, document or index error")
			}
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
	{
		assert.NoError(t, err)
	}
	artifact, err := gittree.AdmitDirectory(t.Context(), location)
	{
		assert.NoError(t, err)
	}
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
	{
		assert.NoError(t, err)
	}
	backing := t.TempDir()
	testutil.WriteFile(t, backing, "kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, backing, "ks.yaml", testutil.KustomizationYAML("nested", "apps", "  {path: ./apps}\n"))
	location, err = gitrepo.Locate(t.Context(), backing, backing)
	{
		assert.NoError(t, err)
	}
	artifact, err = gittree.AdmitDirectory(t.Context(), location)
	{
		assert.NoError(t, err)
	}
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
	{
		assert.FatalIf(t, l.documents.views[artifact.LocalPath] == artifact.FS ||
			new(l.documents).mountedRoot(
				filepath.Join(root, "child/ks.yaml"),
			) != artifact.LocalPath, "%s", "late native mount did not bind its own private operation view")
	}
	id := testutil.KustomizationID("apps", "nested")
	record, indexed := l.Index.Lookup(id)
	{
		assert.FatalIf(
			t,
			!indexed || record.Path != filepath.Join(root, "child/ks.yaml") || st.GetObject(id) == nil,
			"nested mount did not supply its object and index: %+v",
			record,
		)
	}
	id.Name = "stale"
	{
		_, indexed := l.Index.Lookup(id)
		assert.FatalIf(t, indexed || st.GetObject(id) != nil, "%s", "late nested mount retained parent-backed content")
	}
	retained, err := fs.Stat(l.documents.views[unrelated], ".")
	{
		assert.FatalIf(
			t,
			err != nil || l.documents.views[unrelated] != view || retained != observed,
			"%s",
			"late nested mount replaced unrelated view observations",
		)
	}
}
