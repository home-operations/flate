package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testAdmitGraphProjectionEffectiveNamesIncludeNamespaceTransformation(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	for _, side := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			side+"/kustomization.yaml",
			"namespace: "+side+"\nnamePrefix: pre-\nnameSuffix: -post\nresources: [objects.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			side+"/objects.yaml",
			"apiVersion: v1\nkind: Namespace\nmetadata: {name: _}\n---\napiVersion: foreign.example/v2\nkind: Namespace\nmetadata: {name: _}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\n",
		)
	}
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	for _, side := range []string{"one", "two"} {
		occurrences, err := l.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, err)
		ids := make([]manifest.NamedResource, 0, len(occurrences))
		for i := range occurrences {
			o := occurrences[i]
			ids = append(ids, o.Identity)
			assert.FatalIf(t, o.object.Named() != o.Authored, "%s", "projection mutated an authored object")
		}
		assert.Diff(
			t,
			ids,
			[]manifest.NamedResource{
				{Kind: "Namespace", Name: side},
				{Group: "foreign.example", Kind: "Namespace", Namespace: side, Name: "_"},
				{Kind: manifest.KindConfigMap, Namespace: side, Name: "pre-values-post"},
			},
		)
	}
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	for id := range l.Index.Records {
		assert.FatalIf(t, id.Kind == "Namespace", "inventory-only resource entered the production index: %s", id)
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
			assert.Diff(t, l.recordNamespaces(new(r), ""), []string{tc.want})
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
			assert.Diff(t, l.recordNamespaces(new(r), ""), tc.want)
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
			assert.Diff(t, l.recordNamespaces(new(r), ""), tc.want)
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
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)

	projected := testutil.KustomizationID("apps", "sibling")
	authored := testutil.KustomizationID("flux-system", "sibling")
	file := filepath.Join(root, "app", "sibling.yaml")
	check := func() *manifest.Kustomization {
		t.Helper()
		ks, ok := l.Store.Get[*manifest.Kustomization](projected)
		assert.FatalIf(t, !ok, "%s", "projected bootstrap sibling lost at admission fixpoint")
		record, ok := l.Index.Lookup(projected)
		assert.FatalIf(
			t,
			!ok || record.Authored != authored || record.Path != file,
			"projected sibling provenance = %+v, indexed=%t",
			record,
			ok,
		)
		assert.FatalIf(
			t,
			!l.AdmitsRoot(l.IndexedObject(projected, l.Index.Records[projected]).Obj, file),
			"%s",
			"projected bootstrap sibling is not admitted",
		)
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
		metadata := manifest.DocMetadata(doc)
		name, namespace := metadata.Name, metadata.Namespace
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
	assert.FatalIf(t, check() != first, "%s", "repeat admission replaced the stable projected candidate")
}

func TestAdmit_ExcludedBootstrapSibling(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "app/sibling.yaml", testutil.KustomizationYAML("sibling", "apps", "  path: ./leaf\n"))
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	packages, err := l.Packages(t.Context())
	assert.NoError(t, err)
	for _, p := range packages {
		for _, o := range p.Occurrences {
			assert.FatalIf(
				t,
				o.Identity.Name == "sibling",
				"%s",
				"unlisted bootstrap sibling entered package evidence",
			)
		}
	}
	l.Admit(root)
	sibling := testutil.KustomizationID("apps", "sibling")
	assert.FatalIf(t, l.Store.GetObject(sibling) != nil, "%s", "omitted bootstrap sibling admitted")
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
				filepath.Join(root, "app", "sibling.yaml"),
			), "%s", "inactive declaration lost evidence or became runnable")
	}
}

func TestRecordNamespaces_IndexedOwner(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"visit", "claim"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.ensureScopes()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "system", TargetNamespace: "apps"}
			occurrence := compactOccurrence(owner, projectedProducerForTest(owner))
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
			assert.Diff(t, l.recordNamespaces(new(record), ""), []string{"apps"})
		})
	}
}
