package loader

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_CompactOwnedDataAuthority(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := fstest.MapFS{
		"owner.yaml":                 {Data: []byte(testutil.KustomizationYAML("owner", "apps", "  path: ./payload\n  sourceRef: {kind: GitRepository, name: cluster}\n"))},
		"payload/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"payload/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\n")},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	source := &manifest.GitRepository{Name: "cluster", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, "mounted"), LocalRoot: root, InputAlias: true, FS: input})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.PreferExisting = true
	_, err = l.Load(t.Context(), filepath.Join(root, "mounted", "payload"))
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data"}
	record, found := l.Index.Lookup(id)
	assert.Equal(t, found, true)
	assert.Equal(t, l.AdmitsRoot(l.CatalogObject(id), record.Path), false)
	assert.Diff(t, testutil.ProductionOwners(record), []manifest.NamedResource{testutil.KustomizationID("apps", "owner")})
}

func TestLoader_ConstructionReplacesDerivedRecords(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	first := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "first", Namespace: "apps"}
	for _, name := range []string{"first", "second"} {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("namespace: apps\nconfigMapGenerator: [{name: " + name + ", literals: ['KEY=value']}]\n")},
		}})
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		_, found := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name})
		assert.Equal(t, found, true)
	}
	_, stale := l.Index.Lookup(first)
	assert.Equal(t, stale, false)
	snapshot := snapshotIndexForTest(l.Index)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Diff(t, *l.Index, snapshot)
}

func TestLoader_CompactSourceHydrationSurvivesAdmission(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://example.test/first.git, secretRef: {name: first-auth}}\n")}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	var first manifest.BaseManifest
	for child, err := range l.Sources(t.Context()) {
		assert.NoError(t, err)
		first = child.Obj
	}
	if first == nil {
		t.Fatal("source declaration was not hydrated")
	}
	l.Admit(root)
	for child, err := range l.Sources(t.Context()) {
		assert.NoError(t, err)
		assert.Equal(t, child.Obj, first)
	}
	assert.Equal(t, st.GetObject(first.Named()), nil)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://example.test/second.git, secretRef: {name: second-auth}}\n")},
	}})
	_, err = l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	for child, err := range l.Sources(t.Context()) {
		assert.NoError(t, err)
		assert.Equal(t, child.Obj == first, false)
		assert.Equal(t, child.Obj.(*manifest.GitRepository).URL, "https://example.test/second.git")
		assert.Equal(t, slices.ContainsFunc(l.locations[child.Obj.Named()].facts.refs, func(ref manifest.Reference) bool { return ref.Name == "second-auth" }), true)
	}
}

func TestLoader_CompactOccurrenceCoordinates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	file := filepath.Join(root, "bundle.yaml")
	input := fstest.MapFS{"bundle.yaml": {Data: []byte(`{}
---
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: same, namespace: apps}
  data: {VALUE: first}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: same, namespace: apps}
  data: {VALUE: second}
- apiVersion: helm.toolkit.fluxcd.io/v2
  kind: HelmRelease
  metadata: {name: unrelated, namespace: apps}
  spec:
    interval: 1m
    chart: {spec: {chart: example, sourceRef: {kind: HelmRepository, name: charts}}}
    values: {unrelated: value}
`)}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	facts := l.documents.cache[file].objects
	assert.Equal(t, len(facts), 3)
	for i, fact := range facts {
		if fact.Obj != nil || fact.Doc != nil || fact.fact == nil {
			t.Fatalf("census retained a payload: %+v", fact)
		}
		assert.Equal(t, fact.fact.Declaration.Document, 1)
		assert.Equal(t, fact.fact.Declaration.item, i)
	}
	assert.Equal(t, len(st.Objects()), 0)
	reader := l.documents
	reader.cache = nil
	reader.coordinate = &OccurrenceID{Document: 1, item: 1}
	children, err := reader.parseFile(file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(children), 1)
	assert.Equal(t, children[0].Obj.(*manifest.ConfigMap).Data["VALUE"], "second")
	id := children[0].Obj.Named()
	assert.Equal(t, l.IsFileIndexed(id), true)
	delete(l.Index.Records, id)
	assert.Equal(t, l.IsFileIndexed(id), false)
}

func TestLoader_CompactUncertaintyBeforeDiscard(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	file := filepath.Join(root, "producer.yaml")
	input := fstest.MapFS{"producer.yaml": {Data: []byte(`apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: "${CLUSTER_NAME:=biohazard}-vars", namespace: apps}
spec:
  target: {name: rendered-secret}
`)}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	facts := l.documents.cache[file].objects
	assert.Equal(t, len(facts), 1)
	fact := facts[0].fact
	if fact == nil || !fact.facts.uncertain || fact.Identity.Name != "" || fact.Identity.Namespace != "apps" {
		t.Fatalf("uncertainty lost before retention: %+v", fact)
	}
	assert.Equal(t, len(fact.facts.targets), 1)
	assert.Equal(t, fact.facts.targets[0].Name, "rendered-secret")
	assert.Equal(t, len(l.Index.Records), 0)
	assert.Equal(t, len(st.Objects()), 0)
}

func TestLoader_CompactContextualNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, field, namespace string }{
		{"replacement", "spec.targetNamespace", "apps"},
		{"unrelated_field", "spec.path", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := fstest.MapFS{
				"entry/kustomization.yaml":   {Data: []byte("namespace: apps\nresources: [owner.yaml, namespace.yaml]\nreplacements: [{path: ../replacement.yaml}]\n")},
				"entry/owner.yaml":           {Data: []byte(testutil.KustomizationYAML("owner", "", "  path: ./payload\n  sourceRef: {kind: GitRepository, name: cluster, namespace: apps}\n"))},
				"entry/namespace.yaml":       {Data: []byte("apiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n")},
				"replacement.yaml":           {Data: []byte(strings.ReplaceAll(nsReplacementRule, "spec.targetNamespace", tc.field))},
				"payload/kustomization.yaml": {Data: []byte("resources: [source.yaml]\n")},
				"payload/source.yaml":        {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart}\nspec: {url: oci://example.test/chart, ref: {tag: v1}}\n")},
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
			source := &manifest.GitRepository{Name: "cluster", Namespace: "apps"}
			st.AddObject(source)
			st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, "mounted"), LocalRoot: root, InputAlias: true, FS: input})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
			assert.NoError(t, err)
			l.Admit(root)
			owners := l.Kustomizations()
			assert.Equal(t, len(owners), 1)
			assert.Equal(t, owners[0].Obj.(*manifest.Kustomization).TargetNamespace, tc.namespace)
			l.PreferExisting = true
			_, err = l.Load(t.Context(), filepath.Join(root, "mounted", "payload"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: tc.namespace, Name: "chart"}
			_, found := l.Index.Lookup(id)
			assert.Equal(t, found, true)
		})
	}
}

func TestLoader_FreezeIncludesLateProducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data, namespace: apps}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Selection = manifest.Selection{Changed: true}
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.Path = "."
	owner.SourceRef.Kind, owner.SourceRef.Name, owner.SourceRef.Namespace = manifest.BootstrapSourceID.Kind, manifest.BootstrapSourceID.Name, manifest.BootstrapSourceID.Namespace
	st.AddObject(owner)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	owners, _ := l.Index.OwnersOfFile("cm.yaml")
	assert.Diff(t, owners, []manifest.NamedResource{owner.Named()})
}
