package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_GraphProjection(t *testing.T) {
	t.Parallel()
	t.Run(
		"first evaluated template child",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionFirstEvaluatedTemplateChild(t) },
	)
	t.Run(
		"membership_namespace_authority",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionMembershipNamespaceAuthority(t) },
	)
	for _, tc := range []struct {
		name, externalName, localName, target string
		listed, local                         bool
	}{
		{"external first exact", "a-external", "z-local", "kube/clusters/main/flux", true, true},
		{"local first exact", "z-external", "a-local", "kube/clusters/main/flux", true, true},
		{"external first ancestor", "a-external", "z-local", "kube/clusters", true, true},
		{"local first ancestor", "z-external", "a-local", "kube/clusters", true, true},
		{"external only exact", "external", "local", "kube/clusters/main/flux", false, false},
		{"external only ancestor", "external", "local", "kube/clusters", false, false},
		{"unlisted self exact", "external", "local", "kube/clusters/main/flux", false, true},
		{"unlisted self ancestor", "external", "local", "kube/clusters", false, true},
	} {
		t.Run("cross source/"+tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmitGraphProjectionCase(t, &tc)
		},
		)
	}
	for _, tc := range []struct {
		name, source string
		namespaces   []string
		eligibility  fileEligibility
	}{
		{"local", "", []string{"authored", "projected"}, fileEligible},
		{"external", "  sourceRef: {kind: GitRepository, name: external}\n", []string{"authored"}, fileInactive},
	} {
		t.Run("spec_components/"+tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmitGraphProjectionCase2(t, &tc)
		},
		)
	}
	t.Run(
		"projection_compatibility",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionProjectionCompatibility(t) },
	)
	t.Run(
		"path witness construction lifetime",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionPathWitnessConstructionLifetime(t) },
	)

	t.Run("effective names include Namespace transformation", func(t *testing.T) {
		t.Parallel()
		testAdmitGraphProjectionEffectiveNamesIncludeNamespaceTransformation(t)
	},
	)

	t.Run(
		"bootstrap_document_owners",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionBootstrapDocumentOwners(t) },
	)
	t.Run(
		"inactive_unresolved_source",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionInactiveUnresolvedSource(t) },
	)
	t.Run(
		"plain_entry_under_inactive_claim",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionPlainEntryUnderInactiveClaim(t) },
	)
	t.Run(
		"inactive_alias_resource_outside_claim",
		func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionInactiveAliasResourceOutsideClaim(t) },
	)

	for _, included := range []bool{false, true} {
		t.Run(fmt.Sprintf("inactive_cycle/cross_package=%t", included), func(t *testing.T) {
			t.Parallel()
			testAdmitGraphProjectionCase3(t, &included)
		},
		)
	}

	for _, tc := range []struct {
		name   string
		hrDir  string
		pinned bool
		files  map[string]string
		scans  []string
		want   []string
		wantKS []string
		alias  bool
	}{
		{name: "inactive_namespace", files: map[string]string{
			"flux/kustomization.yaml": "resources: [enabled.yaml]\n",
			"flux/enabled.yaml":       testutil.KustomizationYAML("enabled", "flux-system", "  {path: ./apps, targetNamespace: media}\n"),
			"flux/disabled.yaml":      testutil.KustomizationYAML("disabled", "flux-system", "  {path: ./apps, targetNamespace: inactive}\n"),
			"apps/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"."}, want: []string{"media"}},
		{name: "named_ready", files: map[string]string{
			"flux/kustomization.yaml": "resources: [ks.yaml, ../apps]\n",
			"flux/ks.yaml":            testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, targetNamespace: media, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"apps/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "claim_only", files: map[string]string{
			"flux/ks.yaml": testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps}\n"),
		}, scans: []string{"flux", "apps"}, want: []string{""}},
		{name: "self_claim", files: map[string]string{
			"apps/kustomization.yaml": "resources: [ks.yaml, hr.yaml]\n",
			"apps/ks.yaml":            testutil.KustomizationYAML("owner", "", "  {path: ./apps, targetNamespace: media}\n"),
		}, scans: []string{"apps"}, want: []string{"media"}},
		{name: "pinned_namespace", pinned: true, files: map[string]string{
			"flux/ks.yaml":            testutil.KustomizationYAML("pinned", "flux-system", "  {path: ./apps, targetNamespace: apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"apps/kustomization.yaml": "namespace: dirty\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"apps"}},
		{name: "alias_namespace", alias: true, files: map[string]string{
			"flux/ks.yaml":            testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, targetNamespace: apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"apps/kustomization.yaml": "namespace: dirty\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"apps"}},
		{name: "standalone", files: map[string]string{
			"apps/kustomization.yaml": "namespace: media\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"media"}},
		{name: "outer_overlay", files: map[string]string{
			"apps/kustomization.yaml": "namespace: outer\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"outer"}},
		{name: "three_layer_overlay", hrDir: "leaf", files: map[string]string{
			"apps/kustomization.yaml": "namespace: outer\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: middle\nresources: [../leaf]\n",
			"leaf/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"outer"}},
		{name: "flux_target", files: map[string]string{
			"flux/ks.yaml":            testutil.KustomizationYAML("apps", "flux-system", "  {path: ./apps, targetNamespace: media}\n"),
			"apps/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "nested_claim_fallback", hrDir: "apps/nested", files: map[string]string{
			"flux/outer.yaml":                testutil.KustomizationYAML("outer", "flux-system", "  {path: ./apps}\n"),
			"flux/inner.yaml":                testutil.KustomizationYAML("inner", "flux-system", "  {path: ./apps/nested, targetNamespace: shifted}\n"),
			"apps/kustomization.yaml":        "resources: [nested]\n",
			"apps/nested/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"shifted"}},
		{name: "shared_base", files: map[string]string{
			"flux/red.yaml":           testutil.KustomizationYAML("red", "flux-system", "  {path: ./red}\n"),
			"flux/blue.yaml":          testutil.KustomizationYAML("blue", "flux-system", "  {path: ./blue}\n"),
			"red/kustomization.yaml":  "namespace: red\nresources: [../base]\n",
			"blue/kustomization.yaml": "namespace: blue\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "red", "blue"}, want: []string{"red", "blue"}},
		{name: "shared_fallbacks", files: map[string]string{
			"flux/red.yaml":           testutil.KustomizationYAML("red", "red", "  {path: ./base, targetNamespace: red}\n"),
			"flux/blue.yaml":          testutil.KustomizationYAML("blue", "blue", "  {path: ./base}\n"),
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "base"}, want: []string{"", "red"}},
		{name: "shared_transformer_contexts", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./output, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"other/kustomization.yaml":             "namespace: other\nresources: [../apps/ks.yaml]\n",
		}, scans: []string{"apps", "other"}, want: []string{"myns"}, wantKS: []string{"myns", "other"}},
		{name: "mixed_transformer_contexts", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./output, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"other/kustomization.yaml":             "resources: [../apps/ks.yaml]\n",
		}, scans: []string{"apps", "other"}, want: []string{"myns"}, wantKS: []string{"flux-system", "myns"}},
		{name: "self_claim_transformer_fallback", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
		}, scans: []string{"apps"}, want: []string{"myns"}, wantKS: []string{"myns"}},
		{name: "cross_tree", files: map[string]string{
			"apps/kustomization.yaml": "namespace: games\nresources: [ks.yaml]\n",
			"apps/ks.yaml":            testutil.KustomizationYAML("games", "", "  {path: ./base, dependsOn: [{name: prerequisite}]}\n"),
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"apps", "base"}, want: []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAdmitGraphProjectionCase4(t, &tc)
		},
		)
	}

	t.Run("subtree_availability", func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionSubtreeAvailability(t) })

	t.Run("stale_declaration", func(t *testing.T) { t.Parallel(); testAdmitGraphProjectionStaleDeclaration(t) })
}

func testAdmitGraphProjectionFirstEvaluatedTemplateChild(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: factory, namespace: apps}
spec:
  inputs: [{path: first, settings: accepted}, {path: second, settings: rejected}]
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child, namespace: apps}
    spec:
      path: '<< inputs.path >>'
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
      postBuild: {substituteFrom: [{kind: ConfigMap, name: '<< inputs.settings >>'}]}
`)
	for _, path := range []string{"first", "second"} {
		testutil.WriteFile(t, root, path+"/kustomization.yaml", "resources: []\n")
	}
	l := New(store.New())
	l.SourceRoot = root
	l.Options.Selection.Changed = true
	admitForTest(l.Store, root)
	_, err := l.Load(t.Context(), filepath.Join(root, "flux"))
	assert.NoError(t, err)
	child := testutil.KustomizationID("apps", "child")
	owner := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "factory",
	}
	for range 2 {
		l.Admit(root)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		assert.Equal(t, l.candidates[child].Obj.(*manifest.Kustomization).Path, "first")
		for reader := range l.Index.ReadersOfFile("second/cm.yaml") {
			assert.FatalIf(
				t,
				reader.Reader == owner || reader.Reader == child,
				"rejected child left reader edge: %+v",
				reader,
			)
		}
		facts := l.locations[owner].facts
		for i := range facts.templateInputs {
			input := facts.templateInputs[i]
			assert.FatalIf(
				t,
				input.Reference.Name == "rejected" || slices.Contains(input.Reference.Files, "second"),
				"rejected input survived projection: %+v",
				input,
			)
		}
		assert.Equal(t, len(facts.templateObjects), 1)
		assert.Equal(t, len(facts.targets), 1)
	}
}

func testAdmitGraphProjectionMembershipNamespaceAuthority(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name, externalName, localName string
	}{
		{"external_first", "a-external", "z-local"},
		{"local_first", "z-external", "a-local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"flux/kustomization.yaml",
				"resources: [local.yaml, external.yaml, source.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/local.yaml",
				testutil.KustomizationYAML(
					tc.localName,
					"flux-system",
					"  path: ./apps\n  targetNamespace: media\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"flux/external.yaml",
				testutil.KustomizationYAML(
					tc.externalName,
					"flux-system",
					"  path: ./apps\n  sourceRef: {kind: GitRepository, name: external}\n",
				),
			)
			testutil.WriteFile(
				t,
				root,
				"flux/source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec: {url: https://fixture.invalid/external}\n",
			)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: inner\nresources: [child.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/child.yaml",
				testutil.KustomizationYAML(
					"child",
					"flux-system",
					"  path: ./payload\n  dependsOn: [{name: prerequisite}]\n",
				),
			)
			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
			l := New(store.New())
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
			l.Admit(root)
			ext := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "external",
			}
			_, _, external := SourceView(l.Store, ext, root)
			{
				assert.FatalIf(t, !external, "%s", "foreign source was not classified external")
			}
			check := func() manifest.BaseManifest {
				t.Helper()
				var indexed, admitted []manifest.NamedResource
				for id := range l.Index.Records {
					if id.Name == "child" {
						indexed = append(indexed, id)
					}
				}
				for _, child := range l.Kustomizations() {
					if id := child.Obj.Named(); id.Name == "child" {
						admitted = append(admitted, id)
					}
				}
				slices.SortFunc(indexed, manifest.NamedResource.Compare)
				slices.SortFunc(admitted, manifest.NamedResource.Compare)
				id := testutil.KustomizationID("media", "child")
				want := []manifest.NamedResource{id}
				assert.Diff(t, indexed, want)
				assert.Diff(t, admitted, want)
				r, ok := l.Index.Lookup(id)
				assert.Equal(t, ok, true)
				assert.Equal(t, r.Authored, testutil.KustomizationID("flux-system", "child"))
				assert.Diff(t, r.Refs, []manifest.NamedResource(nil))
				obj := l.Store.GetObject(id)
				var refs []manifest.NamedResource
				for ref := range manifest.References(obj, manifest.ReferenceAll) {
					refs = append(refs, ref.NamedResource)
				}
				slices.SortFunc(refs, manifest.NamedResource.Compare)
				wantRefs := []manifest.NamedResource{
					testutil.KustomizationID("media", "prerequisite"),
					manifest.BootstrapSourceID,
				}
				slices.SortFunc(wantRefs, manifest.NamedResource.Compare)
				assert.Diff(t, refs, wantRefs)
				return obj
			}
			first := check()
			candidate := l.candidates[testutil.KustomizationID("media", "child")].Obj
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, check(), first)
			l.Admit(root)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, check(), first)
			assert.Equal(t, l.candidates[testutil.KustomizationID("media", "child")].Obj, candidate)
		})
	}
}

func testAdmitGraphProjectionCase(t *testing.T, testCaseInput *struct {
	name         string
	externalName string
	localName    string
	target       string
	listed       bool
	local        bool
},
) {
	t.Helper()

	root := t.TempDir()
	const entry = "kube/clusters/main/flux"
	resources := "resources: [external.yaml, ../../../deploy/app"
	if testCaseInput.listed {
		resources += ", local.yaml"
	}
	testutil.WriteFile(t, root, entry+"/kustomization.yaml", resources+"]\n")
	if testCaseInput.local {
		testutil.WriteFile(
			t,
			root,
			entry+"/local.yaml",
			testutil.KustomizationYAML(testCaseInput.localName, "flux-system", "  path: ./"+entry+"\n"),
		)
	}
	testutil.WriteFile(
		t,
		root,
		entry+"/external.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec: {url: https://fixture.invalid/external}\n---\n"+testutil.KustomizationYAML(
			testCaseInput.externalName,
			"flux-system",
			"  path: ./"+testCaseInput.target+"\n  sourceRef: {kind: GitRepository, name: external}\n",
		),
	)
	testutil.WriteFile(t, root, "kube/deploy/app/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"kube/deploy/app/ks.yaml",
		testutil.KustomizationYAML("app", "flux-system", "  path: ./payload\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"kube/deploy/app/disabled.yaml",
		testutil.KustomizationYAML("disabled", "flux-system", "  path: ./excluded\n"),
	)
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "excluded/kustomization.yaml", "resources: []\n")
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)
	ext := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "external",
	}
	_, _, external := SourceView(l.Store, ext, root)
	assert.Equal(t, external, true)
	assert.Equal(t, slices.Contains(l.scopes.roots, entry), testCaseInput.listed)
	assert.Equal(t, l.documents.cache[filepath.Join(root, entry)].buildExcluded, !testCaseInput.listed)
	names := make([]string, 0, len(l.Kustomizations()))
	for _, child := range l.Kustomizations() {
		names = append(names, child.Obj.Named().Name)
	}
	assert.Equal(t, slices.Contains(names, "app"), testCaseInput.listed)
	assert.Equal(t, slices.Contains(names, testCaseInput.localName), testCaseInput.listed)
	for _, name := range []string{"app", "disabled"} {
		id := testutil.KustomizationID("flux-system", name)
		record, indexed := l.Index.Lookup(id)
		assert.Equal(t, indexed, true)
		file := "ks.yaml"
		if name == "disabled" {
			file = "disabled.yaml"
		}
		assert.Equal(t, record.File, "kube/deploy/app/"+file)
		assert.Equal(
			t,
			l.AdmitsRoot(l.IndexedObject(id, l.Index.Records[id]).Obj, record.Path),
			name == "app" && testCaseInput.listed,
		)
		assert.Equal(t, l.Store.GetObject(id) != nil, name == "app" && testCaseInput.listed)
	}
}

func testAdmitGraphProjectionCase2(t *testing.T, testCaseInput *struct {
	name        string
	source      string
	namespaces  []string
	eligibility fileEligibility
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml, source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/owner.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./apps\n  components: [component]\n"+testCaseInput.source,
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec: {url: https://fixture.invalid/external}\n",
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: projected\nresources: []\n")
	testutil.WriteFile(
		t,
		root,
		"apps/component/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [child.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/component/child.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child, namespace: authored}\ndata: {value: component}\n",
	)
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	_, err = l.Load(t.Context(), filepath.Join(root, "apps/component"))
	assert.NoError(t, err)
	l.Admit(root)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "authored", Name: "child"}
	r, indexed := l.Index.Lookup(id)
	assert.FatalIf(t, !indexed, "component record missing: %s", id)
	assert.Diff(t, l.recordNamespaces(r, ""), testCaseInput.namespaces)
	assert.Equal(t, l.documents.cache[r.Path].eligible, testCaseInput.eligibility)
	if testCaseInput.name == "external" {
		visits := l.scopes.visits[r.Path]
		for i := range visits {
			v := visits[i]
			{
				assert.FatalIf(
					t,
					v.source.Name == "external",
					"%s",
					"external owner projected membership onto its component",
				)
			}
		}
	}
}

func testAdmitGraphProjectionProjectionCompatibility(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"app/kustomization.yaml",
		"namespace: apps\nresources: [sibling.yaml, source.yaml, ns.yaml]\nreplacements:\n"+nsReplacementRule,
	)
	testutil.WriteFile(
		t,
		root,
		"app/sibling.yaml",
		testutil.KustomizationYAML(
			"sibling",
			"flux-system",
			"  path: ./leaf\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}, {kind: Secret, name: credentials}, {kind: ConfigMap, name: optional, optional: true}]}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"app/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source}\nspec: {url: https://fixture.invalid/repo}\n",
	)
	testutil.WriteFile(t, root, "app/ns.yaml", "apiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n")
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	l.Admit(root)

	id := testutil.KustomizationID("apps", "sibling")
	candidate, ok := l.candidates[id].Obj.(*manifest.Kustomization)
	if !ok || candidate.TargetNamespace != "apps" || l.Store.GetObject(id) != candidate {
		t.Fatalf("initial projection = %+v, published=%v", candidate, l.Store.GetObject(id))
	}
	refs := l.Index.Records[id].Refs
	assert.Diff(
		t,
		refs,
		[]manifest.NamedResource{
			{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"},
			{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"},
		},
	)
	file := filepath.Join(root, "app/sibling.yaml")
	authored := l.authoredObject(file, testutil.KustomizationID("flux-system", "sibling")).Obj.(*manifest.Kustomization)
	snapshot := authored.Clone()
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	source, ok := l.candidates[sourceID].Obj.(*manifest.GitRepository)
	{
		assert.FatalIf(
			t,
			!ok || l.Store.GetObject(sourceID) != source,
			"%s",
			"projected source missing from candidate or store",
		)
	}
	publishedSource := source.WithNamespace(source.Namespace)
	l.Store.DeleteObject(sourceID)
	l.Store.AddObject(publishedSource)
	assert.Equal(t, l.Store.GetObject(sourceID), publishedSource)
	assert.Equal(t, l.projectAuthored(root), false)
	assert.Equal(t, l.candidates[id].Obj, manifest.BaseManifest(candidate))
	assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(candidate))
	assert.Equal(t, l.candidates[sourceID].Obj, manifest.BaseManifest(source))
	assert.Equal(t, l.Store.GetObject(sourceID), publishedSource)
	{
		got := l.Index.Records[id].Refs
		assert.FatalIf(
			t,
			len(got) != 2 || len(refs) != 2 || &got[0] != &refs[0],
			"%s",
			"stable projected identity replaced retained references",
		)
	}

	published := candidate.Clone()
	l.Store.DeleteObject(id)
	l.Store.AddObject(published)
	assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(published))
	assert.Equal(t, l.projectAuthored(root), false)
	assert.Equal(t, l.candidates[id].Obj, manifest.BaseManifest(candidate))
	assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(published))

	incompatible := candidate.Clone()
	incompatible.TargetNamespace = "media"
	l.Store.AddObject(incompatible)
	assert.Equal(t, l.projectAuthored(root), true)
	assert.Equal(t, l.candidates[id].Obj, manifest.BaseManifest(candidate))
	assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(candidate))

	stale := candidate.Clone()
	stale.TargetNamespace = "media"
	child := l.candidates[id]
	child.Obj = stale
	l.candidates[id] = child
	l.Store.AddObject(stale)
	assert.Equal(t, l.projectAuthored(root), true)
	refreshed, ok := l.candidates[id].Obj.(*manifest.Kustomization)
	assert.FatalIf(
		t,
		!ok || refreshed == stale || refreshed.TargetNamespace != "apps" || refreshed.Named() != id,
		"stale candidate masked prediction: %+v",
		refreshed,
	)
	assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(refreshed))
	assert.Equal(t, manifest.Equal(authored, snapshot), true)
	assert.Equal(t, l.authoredObject(file, snapshot.Named()).Obj, manifest.BaseManifest(authored))
}

func testAdmitGraphProjectionPathWitnessConstructionLifetime(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	for _, side := range []string{"left", "right"} {
		testutil.WriteFile(
			t,
			root,
			side+".yaml",
			testutil.KustomizationYAML("same", "apps", "  path: ./"+side+"\n"),
		)
	}
	l := New(store.New())
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root)
	assert.NoError(t, err)
	witnesses := l.PathObjects()
	assert.Equal(t, len(witnesses), 2)
	l.Admit(root)
	assert.Equal(t, len(l.PathObjects()), 1)
	paths := make([]string, 0, len(witnesses))
	for i := range witnesses {
		child := witnesses[i]
		_, path, _ := child.SourcePath()
		paths = append(paths, path)
	}
	assert.Diff(t, paths, []string{"./left", "./right"})
}
