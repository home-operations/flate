package loader

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func loaderFixture(s *store.Store, files map[manifest.NamedResource]string) *Loader {
	l := New(s)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	l.documents = documentReader{cache: map[string]document{}}
	for id, file := range files {
		l.Index.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	return l
}

func stampForTest(s *store.Store, files map[manifest.NamedResource]string, root string) {
	loaderFixture(s, files).stampTransformerTargetNamespaces(root)
}

func rendererFixture(t *testing.T, l *Loader, root string, producers *manifest.ProducerIndex) *manifest.DiscoveryIndex {
	t.Helper()
	l.FinalizeIndex(root, producers)
	return l.Index
}

func renderers(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return r.Renderers
}

func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() {
		files[id] = r.File
	}
	return files
}

func refsForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource][]manifest.NamedResource {
	refs := map[manifest.NamedResource][]manifest.NamedResource{}
	for id, r := range index.Records {
		if len(r.Refs) != 0 {
			refs[id] = r.Refs
		}
	}
	return refs
}

func ownersForTest(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	owners, _ := index.OwnersOfFile(file)
	return owners
}

func declarationOwnersForTest(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return slices.Clone(r.DeclarationOwners)
}

func emissionParentForTest(index *manifest.DiscoveryIndex, file string) (manifest.NamedResource, bool) {
	for _, r := range index.Files() {
		if r.File == file && r.Parent != (manifest.NamedResource{}) {
			return r.Parent, true
		}
	}
	return manifest.NamedResource{}, false
}

func defaultNamespacesForTest(s *store.Store, files map[manifest.NamedResource]string) {
	l := loaderFixture(s, files)
	l.applyDefaultNamespaces()
	clear(files)
	for id, r := range l.Index.Files() {
		files[id] = r.File
	}
}

func TestFinalizeIndex_DataOwners(t *testing.T) {
	for _, tc := range []struct {
		name, namespace string
		owners          bool
	}{
		{"raw namespace bridge", "", true},
		{"file-backed explicit identity", "apps", true},
		{"projected renderer only", "other", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "raw/kustomization.yaml", "namespace: raw\nresources: [input.yaml]\n")
			testutil.WriteFile(t, root, "overlay/kustomization.yaml", "namespace: apps\nresources: [../raw]\n")
			testutil.WriteFile(t, root, "raw/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input, namespace: '"+tc.namespace+"'}\ndata: {value: fixture}\n")
			st := store.New()
			raw := &manifest.Kustomization{Name: "raw", Namespace: "flux-system", Path: "./raw"}
			overlay := &manifest.Kustomization{Name: "overlay", Namespace: "flux-system", Path: "./overlay"}
			st.AddObject(raw)
			st.AddObject(overlay)
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: tc.namespace}
			l := loaderFixture(st, map[manifest.NamedResource]string{id: "raw/input.yaml"})
			l.FinalizeIndex(root, nil)
			projected := id
			projected.Namespace = "apps"
			r, _ := l.Index.Lookup(projected)
			if !slices.Equal(r.Renderers, []manifest.NamedResource{overlay.Named()}) {
				t.Fatalf("projected renderers = %v", r.Renderers)
			}
			want := []manifest.NamedResource{overlay.Named(), raw.Named()}
			if !tc.owners {
				want = nil
			}
			if !slices.Equal(r.Owners, want) {
				t.Fatalf("materialized data owners = %v, want %v", r.Owners, want)
			}
			if got := l.Index.DataOwners(projected); !slices.Equal(got, want) {
				t.Fatalf("data owners = %v, want %v", got, want)
			}
		})
	}
}
