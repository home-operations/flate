package loader

import (
	"io/fs"
	"os"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func loaderFixture(s *store.Store, files map[manifest.NamedResource]string) *Loader {
	l := New(s)
	for id, file := range files {
		l.Index.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	return l
}

func stampForTest(s *store.Store, files map[manifest.NamedResource]string, root string) {
	l := loaderFixture(s, files)
	l.documents = readerForTest(root)
	cache := map[string]nsResolution{}
	for id, file := range files {
		if ks, ok := s.Get[*manifest.Kustomization](id); ok {
			predicted, _ := l.predictNamespaces(ks, root, manifest.DiscoveryRecord{File: file}, cache)
			s.AddObject(predicted)
		}
	}
}

func readerForTest(root string) documentReader {
	return documentReader{cache: map[string]document{}, views: map[string]fs.FS{root: os.DirFS(root)}}
}

func admitForTest(s *store.Store, root string) {
	s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
}

func rendererFixture(t *testing.T, l *Loader, root string) *manifest.DiscoveryIndex {
	t.Helper()
	admitForTest(l.Store, root)
	l.documents.mountSources(l.Store)
	l.FinalizeIndex(root)
	return l.Index
}

func renderers(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return testutil.ProductionOwners(r)
}

func filesForTest(index *manifest.DiscoveryIndex) map[manifest.NamedResource]string {
	files := map[manifest.NamedResource]string{}
	for id, r := range index.Files() {
		files[id] = r.File
	}
	return files
}

func ownersForTest(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	owners, _ := index.OwnersOfFile(file)
	return owners
}

func declarationOwnersForTest(index *manifest.DiscoveryIndex, id manifest.NamedResource) []manifest.NamedResource {
	r, _ := index.Lookup(id)
	return slices.Clone(testutil.ProductionOwners(r))
}

func emissionParentForTest(index *manifest.DiscoveryIndex, file string) (manifest.NamedResource, bool) {
	for _, r := range index.Files() {
		if r.File == file && r.Parent != (manifest.NamedResource{}) {
			return r.Parent, true
		}
	}
	return manifest.NamedResource{}, false
}

func TestFinalizeIndex_Productions(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, kind string
	}{
		{"raw namespace bridge", "", manifest.KindConfigMap},
		{"file-backed explicit identity", "apps", manifest.KindConfigMap},
		{"projected renderer only", "other", manifest.KindConfigMap},
		{"secret raw namespace bridge", "", manifest.KindSecret},
		{"secret explicit identity", "apps", manifest.KindSecret},
		{"secret projected renderer only", "other", manifest.KindSecret},
		{"non-data identity", "apps", manifest.KindHelmRelease},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "raw/kustomization.yaml", "namespace: raw\nresources: [input.yaml]\n")
			testutil.WriteFile(t, root, "overlay/kustomization.yaml", "namespace: apps\nresources: [../raw]\n")
			testutil.WriteFile(t, root, "raw/input.yaml", "apiVersion: v1\nkind: "+tc.kind+"\nmetadata: {name: input, namespace: '"+tc.namespace+"'}\ndata: {value: fixture}\n")
			st := store.New()
			raw := &manifest.Kustomization{Name: "raw", Namespace: "flux-system", Path: "./raw"}
			overlay := &manifest.Kustomization{Name: "overlay", Namespace: "flux-system", Path: "./overlay"}
			st.AddObject(raw)
			st.AddObject(overlay)
			id := manifest.NamedResource{Kind: tc.kind, Name: "input", Namespace: tc.namespace}
			l := loaderFixture(st, map[manifest.NamedResource]string{id: "raw/input.yaml"})
			rendererFixture(t, l, root)
			if st.GetObject(id) != nil {
				t.Fatal("indexed data must remain absent from the store")
			}
			projected := id
			projected.Namespace = "apps"
			r, _ := l.Index.Lookup(projected)
			want := []manifest.NamedResource{overlay.Named()}
			if got := testutil.ProductionOwners(r); !slices.Equal(got, want) {
				t.Fatalf("projected materializers: %v", got)
			}
			if len(r.Productions) == 0 || r.Productions[0].Authored != id || r.Productions[0].Path == "" {
				t.Fatalf("missing authored-file proof: %+v", r)
			}

		})
	}
}
