package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_SourceKinds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, spec string
	}{
		{manifest.KindBucket, "{bucketName: fixtures, endpoint: minio.invalid}"},
		{manifest.KindGitRepository, "{url: https://example.invalid/repo.git}"},
		{manifest.KindHelmChart, "{chart: demo, sourceRef: {kind: HelmRepository, name: repo}}"},
		{manifest.KindExternalArtifact, "{}"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [source.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: "+tc.kind+"\nmetadata: {name: source}\nspec: "+tc.spec+"\n",
			)
			l := loaderFixture(store.New(), nil)
			l.SourceRoot = root
			{
				_, err := l.Load(t.Context(), root)
				assert.NoError(t, err)
			}
			raw := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Name: "source"}
			original := l.authoredObject(filepath.Join(root, "source.yaml"), raw).Obj
			{
				assert.FatalIf(t, original == nil, "%s", "source not loaded")
			}
			l.Admit(root)
			projected := raw
			projected.Namespace = "apps"
			assert.FatalIf(t, l.Store.GetObject(projected) == nil, "projected source %s missing", projected)
			{
				_, ok := l.Index.Lookup(projected)
				assert.FatalIf(t, !ok, "projected source %s not indexed", projected)
			}
			{
				assert.FatalIf(
					t,
					original.Named() != raw || l.Store.GetObject(raw) != nil,
					"%s",
					"raw source must remain immutable and leave the store",
				)
			}
			{
				_, ok := l.Index.Lookup(raw)
				assert.FatalIf(t, ok, "%s", "stale raw source record")
			}
		})
	}
}

func TestAdmit_LateFallback(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("owned_%t", owned), func(t *testing.T) {
			t.Parallel()
			testAdmitLateFallbackCase(t, &owned)
		},
		)
	}
}

func TestAdmit_ProjectionLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "namePrefix: pre-\nnameSuffix: -post\nresources: [object.yaml]\n")
	testutil.WriteFile(t, root, "object.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: before}\n")
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	before, err := l.Occurrences(t.Context(), root, "")
	assert.NoError(t, err)
	assert.Equal(t, len(before), 1)
	assert.Equal(t, before[0].Identity.Name, "pre-before-post")
	old := l.scopes
	assert.Equal(t, len(old.projections[0])+len(old.projections[1]), 1)
	testutil.WriteFile(t, root, "object.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: after}\n")
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)})
	l.mountSources()
	{
		assert.FatalIf(
			t,
			l.scopes == old || l.scopes == nil || l.scopes.projections[0] != nil || l.scopes.projections[1] != nil,
			"%s",
			"mounted replacement retained the old projection memo",
		)
	}
	_, err = l.Load(t.Context(), root)
	assert.NoError(t, err)
	after, err := l.Occurrences(t.Context(), root, "")
	assert.NoError(t, err)
	assert.Equal(t, len(after), 1)
	assert.Equal(t, after[0].Identity.Name, "pre-after-post")
	assert.Equal(t, len(l.scopes.projections[0])+len(l.scopes.projections[1]), 1)
	for scope, projections := range old.projections {
		for key := range projections {
			{
				_, retained := l.scopes.projections[scope][key]
				assert.FatalIf(t, retained, "%s", "replacement reused an old projection input")
			}
		}
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Equal(t, l.scopes, nil)
	assert.Equal(t, before[0].object.Named().Name, "before")
}

func testAdmitLateFallbackCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/objects.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: apps}
spec: {}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: inputs}
spec: {type: Static}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: chart}
spec: {chart: app, sourceRef: {kind: HelmRepository, name: repo}}
`)
	l := loaderFixture(store.New(), nil)
	l.SourceRoot = root
	if *testCaseInput {
		l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "owner", Path: "./apps"})
	}
	{
		_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "apps"))
		assert.NoError(t, err)
	}
	l.Admit(root)
	for _, obj := range l.Store.ListObjects("", "") {
		if obj.Named().Kind == manifest.KindKustomization {
			continue
		}
		want := ""
		if obj.Named().Namespace != want {
			t.Errorf("%s namespace = %q, want %q", obj.Named().Kind, obj.Named().Namespace, want)
		}
	}
	_, err := l.Load(t.Context(), filepath.Join(root, "apps"))
	assert.NoError(t, err)
	l.Admit(root)
	l.Store.AddObject(
		&manifest.Kustomization{Name: "owner", Namespace: "owner", Path: "./apps", TargetNamespace: "media"},
	)
	l.Admit(root)
	for _, id := range []manifest.NamedResource{
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "media", Name: "apps"},
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "media", Name: "inputs"},
		{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "media", Name: "chart"},
	} {
		assert.FatalIf(t, l.Store.GetObject(id) == nil, "late owner did not project %s", id)
		raw := id
		raw.Namespace = ""
		assert.FatalIf(t, l.Store.GetObject(raw) != nil, "late owner left raw %s", raw)
	}
}
