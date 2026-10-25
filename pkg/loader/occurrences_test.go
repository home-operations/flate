package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
