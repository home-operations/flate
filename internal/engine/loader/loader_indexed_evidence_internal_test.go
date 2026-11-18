package loader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoader_PreferExistingIndexedObject(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "first/kustomization.yaml", "resources: [ks.yaml]\n")
	for _, dir := range []string{"first", "second"} {
		testutil.WriteFile(t, root, dir+"/ks.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./"+dir+"\n"))
	}
	l := New(store.New())
	l.SourceRoot, l.PreferExisting = root, true
	for _, dir := range []string{"first", "second"} {
		_, err := l.Load(t.Context(), filepath.Join(root, dir), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
	}
	assert.NoError(t, l.Admit(t.Context(), root))
	id := testutil.KustomizationID("apps", "child")
	record, _ := l.Index.Lookup(id)
	if record.Path != filepath.Join(root, "first", "ks.yaml") || len(l.Kustomizations()) != 1 ||
		l.Kustomizations()[0].Obj.(*manifest.Kustomization).Path != "./first" {
		t.Fatalf("first indexed object was replaced: %+v, %+v", record, l.Kustomizations())
	}
}

func TestLoader_PreferExistingPreparedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source, namespace: apps}\nspec: {url: https://fixture.invalid/raw}\n",
	)
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	artifact := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root), InputAlias: true}
	st.SetArtifact(source.Named(), artifact)
	l := New(st)
	l.SourceRoot, l.PreferExisting = root, true
	record := manifest.DiscoveryRecord{Declaration: source, Builder: "kustomization.yaml"}
	l.Index.Records[source.Named()] = record
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.Admit(t.Context(), root))
	assert.Equal(t, st.GetDeclaration(source.Named()), manifest.BaseManifest(source))
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(artifact))
	got, ok := l.Index.Lookup(source.Named())
	assert.Equal(t, ok, true)
	assert.Diff(t, got, record)
}

func TestLoader_ProjectedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [objects.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"objects.yaml",
		testutil.KustomizationYAML(
			"child",
			"",
			"  path: ./leaf\n",
		)+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/source}\n",
	)
	l := New(store.New())
	l.SourceRoot = root
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.Admit(t.Context(), root))
	for _, cached := range []bool{true, false} {
		if !cached {
			l.candidates = nil
		}
		sources := 0
		for child := range l.Sources(t.Context()) {
			source := child.Obj
			sources++
			assert.Equal(t, source.Named().Namespace, "apps")
		}
		assert.Equal(t, sources, 1)
		owners := l.Kustomizations()
		assert.Equal(t, len(owners), 2)
		for _, owner := range owners {
			assert.Equal(t, owner.Obj.Named().Namespace, "apps")
		}
	}
	for _, obj := range l.documents.cache[filepath.Join(root, "objects.yaml")].objects {
		assert.Equal(t, obj.Obj.Named().Namespace, "")
	}
}
