package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
		_, err := l.Load(t.Context(), filepath.Join(root, dir), nil)
		assert.NoError(t, err)
	}
	l.Admit(root)
	id := testutil.KustomizationID("apps", "child")
	record, _ := l.Index.Lookup(id)
	if record.Path != filepath.Join(root, "first", "ks.yaml") || len(l.Kustomizations()) != 1 ||
		l.Kustomizations()[0].Obj.(*manifest.Kustomization).Path != "./first" {
		t.Fatalf("first indexed object was replaced: %+v, %+v", record, l.Kustomizations())
	}
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
	_, err := l.Load(t.Context(), root, nil)
	assert.NoError(t, err)
	l.Admit(root)
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
