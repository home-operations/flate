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

func TestLoader_DiscoveryOnlyReferenceSelection(t *testing.T) {
	for _, tc := range []struct {
		name, document string
		id             manifest.NamedResource
		want           []manifest.NamedResource
	}{
		{
			name: "kustomization",
			document: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: apps}
spec:
  path: ./app
  sourceRef: {kind: GitRepository, name: cluster, namespace: sources}
  dependsOn: [{name: ordered}]
  postBuild:
    substituteFrom:
      - {kind: ConfigMap, name: settings}
      - {kind: ConfigMap, name: optional, optional: true}
      - {kind: Secret, name: credentials}
      - {kind: Unsupported, name: ignored}
      - {kind: ConfigMap, name: settings}
`,
			id: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"},
			want: []manifest.NamedResource{
				{Kind: manifest.KindGitRepository, Namespace: "sources", Name: "cluster"},
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
			},
		},
		{
			name: "helmrelease",
			document: `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: apps}
spec:
  chartRef: {kind: OCIRepository, name: chart, namespace: sources}
  dependsOn: [{name: ordered}]
  valuesFrom:
    - {kind: ConfigMap, name: settings}
    - {kind: Secret, name: credentials, optional: true}
    - {kind: ConfigMap, name: settings}
`,
			id: manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "app"},
			want: []manifest.NamedResource{
				{Kind: manifest.KindOCIRepository, Namespace: "sources", Name: "chart"},
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
				{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"},
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "app.yaml", tc.document)
			l := loaderFixture(store.New(), nil)
			l.Options.DiscoveryOnly = true
			if _, err := l.Load(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			record, ok := l.Index.Lookup(tc.id)
			if !ok || !slices.Equal(record.Refs, tc.want) {
				t.Fatalf("references = %v, present=%t, want %v", record.Refs, ok, tc.want)
			}
		})
	}
}

func TestLoader_SkipsNonResourcesAndForeignKinds(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "documents.yaml", `values: {key: value}
---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
---
apiVersion: unrelated.example/v1
kind: Kustomization
metadata: {name: lookalike, namespace: apps}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: settings, namespace: apps}
data: {key: value}
`)
	st := store.New()
	l := loaderFixture(st, nil)
	count, err := l.Load(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(l.Index.Records) != 1 {
		t.Fatalf("loaded %d objects with records %v, want only ConfigMap", count, l.Index.Records)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	if st.GetObject(id) == nil {
		t.Fatal("ConfigMap missing")
	}
}
