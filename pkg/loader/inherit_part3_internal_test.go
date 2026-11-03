package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
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

func TestRecordNamespaces_GeneratedCoverage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		role     kustomizeinput.Role
		deferred bool
		want     string
	}{
		{"resource", kustomizeinput.Resource, false, "inner"},
		{"implicit resource", kustomizeinput.Resource, true, "authored"},
		{"configuration", kustomizeinput.Configuration, false, "authored"},
		{"terminal", kustomizeinput.File, false, "authored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := loaderFixture(store.New(), nil)
			l.ensureScopes()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system"}
			l.Store.AddObject(owner)
			l.scopes.visits["child.yaml"] = []graphVisit{{
				namespace: "inner", deferMaterialization: tc.deferred, role: tc.role, owner: owner.Named(),
			}}
			r := manifest.DiscoveryRecord{Path: "child.yaml", Authored: testutil.HelmReleaseID("authored", "child")}
			assert.Diff(t, l.recordNamespaces(r, ""), []string{tc.want})
		})
	}
	resource := graphVisit{namespace: "local", role: kustomizeinput.Resource}
	membership := graphVisit{namespace: "member", role: kustomizeinput.Resource, membershipOnly: true, input: 2}
	inactive := resource
	inactive.input = 1
	deferred := resource
	deferred.deferMaterialization = true
	configuration := resource
	configuration.role = kustomizeinput.Configuration
	terminal := resource
	terminal.role = kustomizeinput.File
	selfOwned := resource
	selfOwned.owner = testutil.KustomizationID("", "child")
	other := resource
	other.namespace = "blue"
	empty := resource
	empty.namespace = ""
	for _, tc := range []struct {
		name   string
		visits []graphVisit
		want   []string
	}{
		{"membership first", []graphVisit{membership, resource}, []string{"local"}},
		{"membership last", []graphVisit{resource, membership}, []string{"local"}},
		{"membership alone", []graphVisit{membership}, []string{"member"}},
		{"inactive with membership", []graphVisit{inactive, membership}, []string{"member"}},
		{"deferred with membership", []graphVisit{deferred, membership}, []string{"member"}},
		{"configuration with membership", []graphVisit{configuration, membership}, []string{"member"}},
		{"terminal with membership", []graphVisit{terminal, membership}, []string{"member"}},
		{"self-owned with membership", []graphVisit{selfOwned, membership}, []string{"member"}},
		{"multiple authoritative", []graphVisit{membership, resource, membership, other}, []string{"blue", "local"}},
		{"multiple including empty", []graphVisit{membership, resource, empty, membership}, []string{"", "local"}},
		{"empty authoritative first", []graphVisit{empty, membership}, []string{""}},
		{"empty authoritative last", []graphVisit{membership, empty}, []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := loaderFixture(store.New(), nil)
			l.ensureScopes()
			l.scopes.inactive = true
			l.scopes.inputs = []scopeInput{{active: false}, {active: true}}
			l.scopes.visits["child.yaml"] = tc.visits
			r := manifest.DiscoveryRecord{Path: "child.yaml", Authored: testutil.KustomizationID("", "child")}
			assert.Diff(t, l.recordNamespaces(r, ""), tc.want)
		})
	}
}

func TestRecordNamespaces_PrefixEligibility(t *testing.T) {
	t.Parallel()
	self := testutil.KustomizationID("", "self")
	projected := testutil.KustomizationID("projected", "self")
	root := testutil.KustomizationID("control", "root")
	outer := testutil.KustomizationID("control", "outer")
	alpha := testutil.KustomizationID("control", "alpha")
	zeta := testutil.KustomizationID("control", "zeta")
	for _, tc := range []struct {
		name, file string
		claims     map[string][]manifest.NamedResource
		want       []string
	}{
		{name: "empty coordinate", claims: map[string][]manifest.NamedResource{"": {root}}, want: []string{"root"}},
		{name: "root file", file: "child.yaml", claims: map[string][]manifest.NamedResource{"": {root}}, want: []string{"root"}},
		{name: "parent relative root", file: "../child.yaml", claims: map[string][]manifest.NamedResource{"": {root}}, want: []string{"root"}},
		{name: "parent relative depth", file: "../apps/child.yaml", claims: map[string][]manifest.NamedResource{"": {root}, "../apps/": {outer}}, want: []string{"outer"}},
		{name: "exact directory", file: "apps", claims: map[string][]manifest.NamedResource{"": {root}, "apps/": {outer}}, want: []string{"root"}},
		{name: "directory slash", file: "apps/", claims: map[string][]manifest.NamedResource{"": {root}, "apps/": {outer}}, want: []string{"outer"}},
		{name: "self before depth", file: "apps/nested/child.yaml", claims: map[string][]manifest.NamedResource{"apps/": {outer}, "apps/nested/": {self, projected}}, want: []string{"outer"}},
		{name: "all eligible peers", file: "apps/nested/child.yaml", claims: map[string][]manifest.NamedResource{"apps/": {outer}, "apps/nested/": {self, zeta, projected, alpha}}, want: []string{"alpha", "zeta"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.ensureScopes()
			for _, id := range []manifest.NamedResource{root, outer, alpha, zeta, projected} {
				l.Store.AddObject(
					&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, TargetNamespace: id.Name},
				)
			}
			l.Index.Records[projected] = manifest.DiscoveryRecord{Authored: self}
			l.Index.Claims = tc.claims
			r := manifest.DiscoveryRecord{Authored: self, Path: "child.yaml", File: tc.file}
			assert.Diff(t, l.recordNamespaces(r, ""), tc.want)
		})
	}
}

func TestAdmit_BootstrapSiblingNamespaceProjection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [sibling.yaml]\ntransformers: [transformers]\n")
	testutil.WriteFile(
		t,
		root,
		"app/transformers/kustomization.yaml",
		"namespace: apps\nresources: [../../transformers]\n",
	)
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources: [transformer.yaml]\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", namespaceTransformer)
	testutil.WriteFile(
		t,
		root,
		"app/sibling.yaml",
		testutil.KustomizationYAML("sibling", "flux-system", "  path: ./leaf\n"),
	)
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)

	projected := testutil.KustomizationID("apps", "sibling")
	authored := testutil.KustomizationID("flux-system", "sibling")
	file := filepath.Join(root, "app/sibling.yaml")
	check := func() *manifest.Kustomization {
		t.Helper()
		ks, ok := l.Store.Get[*manifest.Kustomization](projected)
		{
			assert.FatalIf(t, !ok, "%s", "projected bootstrap sibling lost at admission fixpoint")
		}
		record, ok := l.Index.Lookup(projected)
		assert.FatalIf(
			t,
			!ok || record.Authored != authored || record.Path != file,
			"projected sibling provenance = %+v, indexed=%t",
			record,
			ok,
		)
		{
			assert.FatalIf(
				t,
				!l.AdmitsRoot(l.IndexedObject(projected, l.Index.Records[projected]).Obj, file),
				"%s",
				"projected bootstrap sibling is not admitted",
			)
		}
		{
			_, ok := l.Index.Lookup(authored)
			assert.FatalIf(
				t,
				ok || l.Store.GetObject(authored) != nil,
				"%s",
				"stale authored sibling identity survived projection",
			)
		}
		doc, err := ks.RenderDocument()
		assert.NoError(t, err)
		name, namespace := manifest.DocMetadata(doc)
		assert.FatalIf(
			t,
			name != "sibling" || namespace != "apps" || doc["spec"].(map[string]any)["targetNamespace"] != "apps",
			"projected sibling document = %v",
			doc,
		)
		return ks
	}
	first := check()
	candidate := l.candidates[projected].Obj
	assert.Equal(t, l.projectAuthored(root), false)
	assert.Equal(t, l.candidates[projected].Obj, candidate)
	assert.Equal(t, l.Store.GetObject(projected), manifest.BaseManifest(first))
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	{
		assert.FatalIf(t, check() != first, "%s", "repeat admission replaced the stable projected candidate")
	}
}

func TestAdmit_ExcludedBootstrapSibling(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "app/sibling.yaml", testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n"))
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	packages, err := l.Packages(t.Context())
	assert.NoError(t, err)
	for _, p := range packages {
		for _, o := range p.Occurrences {
			{
				assert.FatalIf(
					t,
					o.Identity.Name == "sibling",
					"%s",
					"unlisted bootstrap sibling entered package evidence",
				)
			}
		}
	}
	l.Admit(root)
	sibling := testutil.KustomizationID("apps", "sibling")
	{
		assert.FatalIf(t, l.Store.GetObject(sibling) != nil, "%s", "omitted bootstrap sibling admitted")
	}
	doc := l.candidates[sibling].Doc
	assert.FatalIf(
		t,
		doc == nil || doc["spec"].(map[string]any)["path"] != "./leaf",
		"bootstrap sibling lost authoritative document: %v",
		doc,
	)
	l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./app"})
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	{
		_, ok := l.Index.Lookup(
			sibling,
		)
		assert.FatalIf(t, !ok || l.Store.GetObject(sibling) != nil ||
			l.AdmitsRoot(
				l.IndexedObject(sibling, l.Index.Records[sibling]).Obj,
				filepath.Join(root, "app/sibling.yaml"),
			), "%s", "inactive declaration lost evidence or became runnable")
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

func TestRecordNamespaces_IndexedOwner(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"visit", "claim"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.ensureScopes()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "system", TargetNamespace: "apps"}
			occurrence := compactOccurrence(owner, manifest.ProjectProducer(owner))
			l.locations = map[manifest.NamedResource]Occurrence{owner.Named(): occurrence}
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{
				Authored: owner.Named(),
				Path:     "/input/flux/owner.yaml",
			}
			l.Index.Claims = map[string][]manifest.NamedResource{"app/": {owner.Named()}}
			record := manifest.DiscoveryRecord{
				Authored: manifest.NamedResource{
					Group: manifest.SourceDomain,
					Kind:  manifest.KindOCIRepository,
					Name:  "chart",
				},
				Path: "/input/app/source.yaml",
				File: "app/source.yaml",
			}
			if name == "visit" {
				l.scopes.visits[record.Path] = []graphVisit{{role: kustomizeinput.Resource}}
			}
			assert.Diff(t, l.recordNamespaces(record, ""), []string{"apps"})
		})
	}
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
