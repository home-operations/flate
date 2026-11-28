package loader

import (
	"path/filepath"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
			assert.FatalIf(t, o.Object.Named() != o.Authored, "%s", "projection mutated an authored object")
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
	assert.NoError(t, l.Admit(t.Context(), root))
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
			setScopeVisitsForTest(l, "child.yaml", []graphVisit{{
				namespace: "inner", deferMaterialization: tc.deferred, role: tc.role, owner: owner.Named(),
			}})
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
			setScopeVisitsForTest(l, "child.yaml", tc.visits)
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
	assert.NoError(t, l.Admit(t.Context(), root))

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
			!selection.Root(
				rootAtForTest(l, l.IndexedObject(projected, l.Index.Records[projected]).Obj, file),
			).Admitted,
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
	assert.Equal(t, l.projectAuthored(root, nil), false)
	assert.Equal(t, l.candidates[projected].Obj, candidate)
	assert.Equal(t, l.Store.GetObject(projected), manifest.BaseManifest(first))
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.NoError(t, l.Admit(t.Context(), root))
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
	assert.NoError(t, l.Admit(t.Context(), root))
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
	assert.NoError(t, l.Admit(t.Context(), root))
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	{
		_, ok := l.Index.Lookup(
			sibling,
		)
		assert.FatalIf(t, !ok || l.Store.GetObject(sibling) != nil ||
			selection.Root(
				rootAtForTest(
					l,
					l.IndexedObject(sibling, l.Index.Records[sibling]).Obj,
					filepath.Join(root, "app", "sibling.yaml"),
				),
			).Admitted, "%s", "inactive declaration lost evidence or became runnable")
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
			occurrence := new(occurrenceFacts).compactOccurrence(owner, projectedProducerForTest(owner))
			l.locations = map[manifest.NamedResource]authoredOccurrence{owner.Named(): occurrence}
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
				setScopeVisitsForTest(l, record.Path, []graphVisit{{role: kustomizeinput.Resource}})
			}
			assert.Diff(t, l.recordNamespaces(new(record), ""), []string{"apps"})
		})
	}
}

func setScopeVisitsForTest(l *Loader, file string, visits []graphVisit) {
	delete(l.scopes.visits, file)
	for i := range visits {
		visit := visits[i]
		visit.file = file
		l.recordScopeVisits(&visit, []graphVisit{visit}, visit.membershipOnly, false)
	}
}

func TestProjectAuthoredVisits_PhysicalCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		active     bool
		membership bool
		contextual bool
		role       kustomizeinput.Role
		want       bool
	}{
		{name: "contextual", active: true, contextual: true, role: kustomizeinput.Resource, want: true},
		{name: "resource", active: true, role: kustomizeinput.Resource, want: true},
		{name: "inactive", role: kustomizeinput.Resource},
		{name: "membership", active: true, membership: true, role: kustomizeinput.Resource},
		{name: "control", active: true, role: kustomizeinput.File},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.ensureScopes()
			owner := testutil.KustomizationID("apps", "owner")
			root := graphVisit{root: l.SourceRoot, dir: "mounted", owner: owner, source: manifest.BootstrapSourceID}
			file := filepath.Join(l.SourceRoot, "mounted", "input.yaml")
			origin := filepath.Join(l.SourceRoot, "original", "input.yaml")
			l.recordScopeVisits(&root, []graphVisit{{
				file: file, origin: origin, coordinate: "input.yaml", localBase: "mounted",
				namespace: "apps", role: tc.role,
			}}, tc.membership, false)
			l.scopes.contributions[0].active = tc.active
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input"}
			occurrence := authoredOccurrence{Authored: id, Identity: id, facts: &occurrenceFacts{namespaced: true}}
			if tc.contextual {
				id.Name, id.Namespace = "${NAME}", "${NS}"
				occurrence.Authored, occurrence.Identity = id, id
				occurrence.Declaration = occurrenceID{Source: l.SourceRoot, File: "original/input.yaml", Document: 1}
				occurrence.facts.uncertain = true
				parent := &manifest.Kustomization{
					Name: owner.Name, Namespace: owner.Namespace,
					PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"NAME": "input", "NS": "apps"}},
				}
				l.Store.AddObject(parent)
				l.Index.Records[owner] = manifest.DiscoveryRecord{Declaration: parent}
				visit := l.scopes.contributions[0].visits[0]
				visit.owner, visit.source = owner, manifest.BootstrapSourceID
				resolution := substitutionResolution{loader: l}
				resolved, err := resolution.resolveOccurrence(t.Context(), &occurrence, &visit, "v1")
				assert.NoError(t, err)
				assert.Equal(t, resolved.facts.uncertain, false)
				l.scopes.contextual = map[occurrenceContext]authoredOccurrence{
					{occurrence.Declaration, visit.owner, visit.source, visit.graphContext, visit.file}: resolved,
				}
				_, indexed := l.Index.Lookup(id)
				assert.Equal(t, indexed, false)
			}
			for _, path := range []string{file, origin} {
				var identities []manifest.NamedResource
				record := manifest.DiscoveryRecord{Path: path, Authored: id}
				projected := l.projectAuthoredVisits(
					&record,
					&occurrence,
					"v1",
					func(o authoredOccurrence, p manifest.Production) {
						identities = append(identities, o.Identity)
						assert.Equal(t, o.Authored, id)
						assert.Equal(t, o.Declaration, occurrence.Declaration)
						assert.Equal(t, p.Owner, owner)
						assert.Equal(t, p.Source, manifest.BootstrapSourceID)
						assert.Equal(t, p.Path, path)
					},
				)
				assert.Equal(t, projected, tc.want)
				var want []manifest.NamedResource
				if tc.want {
					want = []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}}
				}
				assert.Diff(t, identities, want)
			}
		})
	}
}

func TestProjectAuthoredVisits_PendingSupplierEligibility(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"checkout supplier", "non-checkout claim", "eager checkout", "composed checkout",
		"inactive claim", "no pending build",
		"inactive supplier", "non-bootstrap supplier", "already owned scope",
		"unregistered root", "origin only", "unowned origin only", "control file",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.ensureScopes()
			l.scopes.roots = []string{"entry"}
			file := filepath.Join(l.SourceRoot, "owner.yaml")
			mount := filepath.Join(l.SourceRoot, "mount")
			artifact := &store.SourceArtifact{Provider: store.ProviderAlias, LocalRoot: l.SourceRoot, LocalPath: mount}
			l.documents.artifacts = map[string]*store.SourceArtifact{mount: artifact}
			id := testutil.KustomizationID("apps", "owner")
			source := (&manifest.GitRepository{Name: "fleet", Namespace: "apps"}).Named()
			claim := graphVisit{
				root: mount, dir: "claimed", owner: id, source: source, role: kustomizeinput.Resource,
				file: filepath.Join(mount, "owner.yaml"), origin: file, requiresBuild: true,
			}
			native := graphVisit{
				root: l.SourceRoot, dir: "entry", source: manifest.BootstrapSourceID,
				file: file, role: kustomizeinput.Resource,
			}
			unowned := name == "unowned origin only"
			switch name {
			case "non-checkout claim":
				artifact.Provider = store.ProviderCommit
			case "eager checkout":
				l.documents.eager = true
			case "composed checkout":
				artifact.Extent = store.ExtentComposed
			case "no pending build":
				claim.requiresBuild = false
			case "non-bootstrap supplier":
				native.source = source
			case "already owned scope":
				native.owner = testutil.KustomizationID("apps", "peer")
			case "unregistered root":
				l.scopes.roots = nil
			case "origin only", "unowned origin only":
				native.file, native.origin = filepath.Join(l.SourceRoot, "alias.yaml"), file
				if unowned {
					native.namePrefix = "entry-"
				}
			case "control file":
				native.role = kustomizeinput.File
			}
			l.recordScopeVisits(&claim, []graphVisit{claim}, false, false)
			l.recordScopeVisits(&native, []graphVisit{native}, !unowned, false)
			l.scopes.contributions[0].active = name != "inactive claim" && !unowned
			l.scopes.contributions[1].active = name != "inactive supplier"

			record := manifest.DiscoveryRecord{Path: file, File: "owner.yaml", Authored: id}
			occurrence := authoredOccurrence{Authored: id, Identity: id, facts: &occurrenceFacts{namespaced: true}}
			generated := manifest.NewEntryKustomization(
				manifest.BootstrapSourceID, "entry", manifest.NamedResource{},
			).Named()
			var supplied bool
			wantIdentity := id
			wantIdentity.Name = native.namePrefix + id.Name
			l.projectAuthoredVisits(&record, &occurrence, "v1", func(o authoredOccurrence, p manifest.Production) {
				if p.Owner == generated {
					supplied = true
					assert.Equal(t, o.Identity, wantIdentity)
					assert.Equal(t, p.Authored, id)
					assert.Equal(t, p.Source, manifest.BootstrapSourceID)
					assert.Equal(t, p.Path, file)
				}
			})
			assert.Equal(t, supplied, name == "checkout supplier" || unowned)
		})
	}
}

func TestProjectAuthoredVisits_ImportedRootIdentity(t *testing.T) {
	t.Parallel()
	const outerDir = "parent"
	const innerDir = "parent/child"
	type supplier struct {
		Name string
		Root string
	}
	for _, tc := range []struct {
		name        string
		innerPrefix string
		want        []supplier
	}{
		{
			name: "same identity", innerPrefix: "outer-",
			want: []supplier{{Name: "outer-owner", Root: outerDir}},
		},
		{
			name: "distinct identities", innerPrefix: "inner-",
			want: []supplier{{Name: "outer-owner", Root: outerDir}, {Name: "inner-owner", Root: innerDir}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.ensureScopes()
			l.scopes.roots = []string{outerDir, innerDir}
			file := filepath.Join(l.SourceRoot, innerDir, "owner.yaml")
			builder := filepath.Join(l.SourceRoot, innerDir, "kustomization.yaml")
			l.documents.cache = map[string]document{filepath.Dir(builder): {file: builder}}

			outer := graphVisit{root: l.SourceRoot, dir: outerDir, source: manifest.BootstrapSourceID}
			inner := graphVisit{root: l.SourceRoot, dir: innerDir, source: manifest.BootstrapSourceID}
			l.recordScopeVisits(&outer, []graphVisit{
				{file: builder, role: kustomizeinput.Resource, document: new(types.Kustomization)},
				{file: file, role: kustomizeinput.Resource, namePrefix: "outer-"},
			}, false, false)
			l.recordScopeVisits(&inner, []graphVisit{
				{file: file, role: kustomizeinput.Resource, namePrefix: tc.innerPrefix},
			}, false, false)
			l.scopes.contributions[0].active = true
			l.scopes.contributions[1].active = true

			id := testutil.KustomizationID("apps", "owner")
			record := manifest.DiscoveryRecord{Path: file, File: innerDir + "/owner.yaml", Authored: id}
			occurrence := authoredOccurrence{Authored: id, Identity: id, facts: &occurrenceFacts{namespaced: true}}
			var suppliers []supplier
			l.projectAuthoredVisits(&record, &occurrence, "v1", func(o authoredOccurrence, p manifest.Production) {
				assert.Equal(t, p.Source, manifest.BootstrapSourceID)
				suppliers = append(suppliers, supplier{Name: o.Identity.Name, Root: p.Owner.Name})
			})
			assert.Diff(t, suppliers, tc.want)
		})
	}
}
