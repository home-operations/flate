package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOccurrenceClaims_CapturedSubsetAndIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pin := filepath.Join(root, ".artifacts/pin")
	st := store.New()
	source := &manifest.GitRepository{Name: "pin", Namespace: "sources"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: pin, LocalRoot: root, Key: "committed", Content: "committed", FS: fstest.MapFS{}})
	st.AddObject(&manifest.Kustomization{Name: "stored", Namespace: "apps", Path: "unrelated"})
	input := fstest.MapFS{}
	for _, side := range []string{"one", "two"} {
		input[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("namespace: " + side + "\nnamePrefix: " + side + "-\nresources: [owners.yaml]\n")}
		paths := []string{"captured-a", "captured-b"}
		if side == "two" {
			paths = []string{"captured-c", "excluded"}
		}
		body := testutil.KustomizationYAML("owner", "authored", "  path: ./"+paths[0]+"\n  sourceRef: {kind: GitRepository, name: pin, namespace: sources}\n") + "---\n" + testutil.KustomizationYAML("owner", "authored", "  path: ./"+paths[1]+"\n  sourceRef: {kind: GitRepository, name: pin, namespace: sources}\n")
		input[side+"/owners.yaml"] = &fstest.MapFile{Data: []byte(body)}
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	one, err := l.Occurrences(t.Context(), filepath.Join(root, "one"), "")
	assert.NoError(t, err)
	two, err := l.Occurrences(t.Context(), filepath.Join(root, "two"), "")
	assert.NoError(t, err)
	assert.Equal(t, len(one), 2)
	assert.Equal(t, len(two), 2)
	if len(one) != 2 || len(two) != 2 {
		t.Fatal("same-identity declarations lost occurrence evidence")
	}
	captured := append(slices.Clone(one), two[0])
	before := slices.Clone(captured)
	objects := st.ListObjects("", "")
	claims := l.OccurrenceClaims(root, captured)
	var want []manifest.KSClaim
	for _, prefix := range []string{".artifacts/pin/", ""} {
		for i, path := range []string{"captured-a", "captured-b", "captured-c"} {
			want = append(want, manifest.KSClaim{ID: captured[i].Identity, Prefix: prefix + path + "/"})
		}
	}
	assert.Diff(t, claims, want)
	assert.Equal(t, len(l.OccurrenceClaims(root, nil)), 0)
	assert.Equal(t, len(st.ListObjects("", "")), len(objects))
	for _, obj := range objects {
		assert.Equal(t, st.GetObject(obj.Named()), obj)
	}
	for i, occurrence := range captured {
		assert.Equal(t, occurrence.Declaration, before[i].Declaration)
		assert.Equal(t, occurrence.Identity, before[i].Identity)
		assert.Equal(t, occurrence.Authored, testutil.KustomizationID("authored", "owner"))
		assert.Equal(t, occurrence.Object, before[i].Object)
		assert.Equal(t, occurrence.Object.Named(), occurrence.Authored)
	}
	assert.Equal(t, captured[0].Identity, testutil.KustomizationID("one", "one-owner"))
	assert.Equal(t, captured[1].Identity, captured[0].Identity)
	assert.Equal(t, captured[2].Identity, testutil.KustomizationID("two", "two-owner"))
	assert.Diff(t, []OccurrenceID{captured[0].Declaration, captured[1].Declaration, captured[2].Declaration}, []OccurrenceID{
		{Source: root, File: "one/owners.yaml", Document: 0},
		{Source: root, File: "one/owners.yaml", Document: 1},
		{Source: root, File: "two/owners.yaml", Document: 0},
	})
}

func TestPackages_ScanProvenance(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{}
	for _, dir := range []string{"one/pkg", "two/pkg"} {
		input[dir+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
		input[dir+"/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\n")}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	packages, err := l.Packages(t.Context())
	assert.NoError(t, err)
	if len(packages) != 2 {
		t.Fatalf("package evidence: %+v", packages)
	}
	for i, dir := range []string{"one", "two"} {
		assert.Equal(t, packages[i].Path, filepath.ToSlash(filepath.Join(dir, "pkg")))
		assert.Diff(t, packages[i].Scans, []string{".", dir, dir + "/pkg"})
	}
}

func TestProjectIdentity_NamespaceOnly(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		object        manifest.BaseManifest
		want          manifest.NamedResource
		noAllocation  bool
	}{
		{name: "namespaced", version: "v1", object: &manifest.ConfigMap{Name: "values", Namespace: "authored"}, want: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "selected", Name: "values"}, noAllocation: true},
		{name: "foreign Namespace", version: "foreign.example/v2", object: &manifest.RawObject{APIVersion: "foreign.example/v2", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Group: "foreign.example", Kind: "Namespace", Namespace: "selected", Name: "_"}, noAllocation: true},
		{name: "core Namespace", version: "v1", object: &manifest.RawObject{APIVersion: "v1", Kind: "Namespace", Name: "_"}, want: manifest.NamedResource{Kind: "Namespace", Name: "selected"}},
		{name: "cluster scoped", version: "rbac.authorization.k8s.io/v1", object: &manifest.RawObject{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "policy"}, want: manifest.NamedResource{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "policy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := graphVisit{namespace: "selected"}
			got, err := projectIdentity(tc.object, view, tc.version)
			assert.NoError(t, err)
			assert.Equal(t, got, tc.want)
			if tc.noAllocation {
				allocations := testing.AllocsPerRun(1, func() {
					got, err = projectIdentity(tc.object, view, tc.version)
				})
				assert.NoError(t, err)
				assert.Equal(t, got, tc.want)
				assert.Equal(t, allocations, 0)
			}
		})
	}
}
