package discovery_test

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSession_OnlyRequiredStandaloneConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{
		"one/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"two/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"one/cm.yaml":            {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: one, namespace: apps}\nspec: {resources: []}\n")},
		"two/cm.yaml":            {Data: []byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: two, namespace: apps}\nspec: {resources: []}\n")},
	}
	st := store.New()
	l := loader.New(st)
	session, err := discovery.Begin(t.Context(), discovery.Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Loader: l, Trees: testTrees(t), Selection: manifest.Selection{Changed: true}})
	assert.NoError(t, err)
	for _, object := range st.Objects() {
		if !manifest.KindOf(object).SourceArtifact {
			t.Fatalf("unexpected construction payload: %s", object.Named())
		}
	}
	advanced, err := session.Advance(t.Context(), &loader.Admission{Blocked: []string{"one"}})
	assert.NoError(t, err)
	assert.Equal(t, advanced, true)
	if st.GetObject(manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "two"}) != nil {
		t.Fatal("unrelated package was hydrated")
	}
	one := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "one"}
	built := l.PromotionDocuments(one)
	assert.Equal(t, len(built), 1)
	advanced, err = session.Advance(t.Context(), &loader.Admission{Objects: built})
	assert.NoError(t, err)
	assert.Equal(t, advanced, false)
	result, err := session.Freeze(t.Context())
	assert.NoError(t, err)
	again, err := session.Freeze(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, result, again)
	assert.Equal(t, result.Index, l.Index)
	assert.Equal(t, st.GetObject(one) != nil, true)
	if st.GetObject(manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "two"}) != nil {
		t.Fatal("freeze hydrated an unrelated package")
	}
}

func TestRun_RestrictedUsesSession(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{
		"one/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"two/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"one/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: one, namespace: apps}\ndata: {value: selected}\n")},
		"two/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: two, namespace: apps}\ndata: {value: unrelated}\n")},
	}
	st := store.New()
	l := loader.New(st)
	one := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "one"}
	two := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "two"}
	result, err := discovery.Run(t.Context(), discovery.Config{
		Input: &store.SourceArtifact{LocalPath: root, FS: input},
		Path:  root, Store: st, Loader: l, Trees: testTrees(t),
		Selection: manifest.Selection{Roots: []manifest.NamedResource{one}},
	})
	assert.NoError(t, err)
	assert.Equal(t, result.Index, l.Index)
	assert.Equal(t, len(l.PromotionDocuments(one)), 0)
	assert.Equal(t, len(l.PromotionDocuments(two)), 0)
	assert.Equal(t, st.GetObject(one) != nil, true)
	assert.Equal(t, st.GetObject(two), nil)
	assert.Equal(t, l.Options.Filter.ShouldReconcile(one), true)
	assert.Equal(t, l.Options.Filter.ShouldReconcile(two), false)
}
