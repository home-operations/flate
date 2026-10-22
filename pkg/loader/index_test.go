package loader

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
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
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
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

func snapshotIndexForTest(index *manifest.DiscoveryIndex) manifest.DiscoveryIndex {
	snapshot := *index
	snapshot.Records = maps.Clone(index.Records)
	for id, r := range snapshot.Records {
		r.Refs = slices.Clone(r.Refs)
		r.Productions = slices.Clone(r.Productions)
		snapshot.Records[id] = r
	}
	snapshot.Claims = maps.Clone(index.Claims)
	for file, ids := range snapshot.Claims {
		snapshot.Claims[file] = slices.Clone(ids)
	}
	snapshot.Readers = maps.Clone(index.Readers)
	for file, ids := range snapshot.Readers {
		snapshot.Readers[file] = slices.Clone(ids)
	}
	snapshot.Errors = maps.Clone(index.Errors)
	return snapshot
}

func TestFinalizeIndex_Productions(t *testing.T) {
	t.Parallel()

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
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "raw/kustomization.yaml", "namespace: raw\nresources: [input.yaml]\n")
			testutil.WriteFile(t, root, "overlay/kustomization.yaml", "namespace: apps\nresources: [../raw]\n")
			testutil.WriteFile(t, root, "raw/input.yaml", "apiVersion: v1\nkind: "+tc.kind+"\nmetadata: {name: input, namespace: '"+tc.namespace+"'}\ndata: {value: fixture}\n")
			id := manifest.NamedResource{Kind: tc.kind, Name: "input", Namespace: tc.namespace}
			overlay := &manifest.Kustomization{Name: "overlay", Namespace: "flux-system", Path: "./overlay"}
			newLoader := func() *Loader {
				st := store.New()
				st.AddObject(&manifest.Kustomization{Name: "raw", Namespace: "flux-system", Path: "./raw"})
				st.AddObject(overlay.Clone())
				return loaderFixture(st, map[manifest.NamedResource]string{id: "raw/input.yaml"})
			}
			l := newLoader()
			rendererFixture(t, l, root)
			if l.Store.GetObject(id) != nil {
				t.Fatal("indexed data must remain absent from the store")
			}
			projected := id
			projected.Namespace = "apps"
			r, _ := l.Index.Lookup(projected)
			want := []manifest.NamedResource{overlay.Named()}
			if got := testutil.ProductionOwners(r); !slices.Equal(got, want) {
				t.Fatalf("projected materializers: %v", got)
			}
			assert.Diff(t, r.Productions, []manifest.Production{{Owner: overlay.Named(), Authored: id,
				Path: filepath.Join(root, "raw/input.yaml"), File: "raw/input.yaml", Source: manifest.BootstrapSourceID}})
			snapshot := snapshotIndexForTest(l.Index)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *l.Index, snapshot)
			fresh := newLoader()
			rendererFixture(t, fresh, root)
			assert.Diff(t, *fresh.Index, snapshot)
		})
	}
}

func TestFinalizeIndex_RecordCompletion(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
	first := testutil.KustomizationID("flux-system", "a")
	last := testutil.KustomizationID("flux-system", "z")
	source := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "source"}
	a := manifest.Production{Owner: first, Declaration: id, Authored: id, Path: "declared.yaml", File: "declared.yaml", Source: source}
	z := manifest.Production{Owner: last, Declaration: id, Authored: id, Path: "declared.yaml", File: "declared.yaml", Source: source}
	for _, tc := range []struct {
		name         string
		record, want manifest.DiscoveryRecord
	}{
		{name: "authored fallback", record: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml", File: "raw.yaml"},
			want: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml", File: "raw.yaml", Productions: []manifest.Production{{Authored: id, Path: "raw.yaml", File: "raw.yaml", Source: manifest.BootstrapSourceID}}}},
		{name: "empty path is file backed", record: manifest.DiscoveryRecord{Authored: id, File: "generator.yaml"},
			want: manifest.DiscoveryRecord{Authored: id, File: "generator.yaml", Productions: []manifest.Production{{Authored: id, File: "generator.yaml", Source: manifest.BootstrapSourceID}}}},
		{name: "no authored identity", record: manifest.DiscoveryRecord{Path: "raw.yaml", File: "raw.yaml"},
			want: manifest.DiscoveryRecord{Path: "raw.yaml", File: "raw.yaml"}},
		{name: "non file authored record", record: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml"},
			want: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml"}},
		{name: "existing file evidence is sorted", record: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml", File: "raw.yaml", Refs: []manifest.NamedResource{source}, Parent: first, Pinned: true, Excluded: true, Productions: []manifest.Production{z, a}},
			want: manifest.DiscoveryRecord{Authored: id, Path: "raw.yaml", File: "raw.yaml", Refs: []manifest.NamedResource{source}, Parent: first, Pinned: true, Excluded: true, Productions: []manifest.Production{a, z}}},
		{name: "existing non file evidence is sorted", record: manifest.DiscoveryRecord{Authored: id, Productions: []manifest.Production{z, a}},
			want: manifest.DiscoveryRecord{Authored: id, Productions: []manifest.Production{a, z}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			newLoader := func() *Loader {
				l := New(store.New())
				r := tc.record
				r.Productions = slices.Clone(r.Productions)
				r.Refs = slices.Clone(r.Refs)
				l.Index.Records[id] = r
				return l
			}
			l := newLoader()
			if err := l.FinalizeIndex(t.Context(), ""); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, l.Index.Records[id], tc.want)
			snapshot := snapshotIndexForTest(l.Index)
			if err := l.FinalizeIndex(t.Context(), ""); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *l.Index, snapshot)
			fresh := newLoader()
			if err := fresh.FinalizeIndex(t.Context(), ""); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *fresh.Index, snapshot)
		})
	}
}
