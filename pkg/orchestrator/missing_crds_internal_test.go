package orchestrator

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

const issueResourceSet = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: flux-operator
  namespace: flux-system
spec:
  serviceAccountName: flux-operator
  dependsOn:
    - apiVersion: apiextensions.k8s.io/v1
      kind: CustomResourceDefinition
      name: helmreleases.helm.toolkit.fluxcd.io
  resources:
    - apiVersion: source.toolkit.fluxcd.io/v1
      kind: OCIRepository
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        url: oci://ghcr.io/controlplaneio-fluxcd/charts/flux-operator
        ref:
          semver: '*'
        verify:
          provider: cosign
          matchOIDCIdentity:
          - issuer: ^https://token\.actions\.githubusercontent\.com$
            subject: ^https://github\.com/controlplaneio-fluxcd/charts/\.github/workflows/release\.yml@refs/tags/v\d+\.\d+\.\d+$
    - apiVersion: helm.toolkit.fluxcd.io/v2
      kind: HelmRelease
      metadata:
        name: flux-operator
        namespace: flux-system
      spec:
        interval: 1h
        releaseName: flux-operator
        serviceAccountName: flux-operator
        chartRef:
          kind: OCIRepository
          name: flux-operator
        values:
          multitenancy:
            enabled: true
            defaultServiceAccount: default
          reporting:
            interval: 5m
`

const helmReleaseCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata: {name: helmreleases.helm.toolkit.fluxcd.io}
spec:
 group: helm.toolkit.fluxcd.io
 names: {kind: HelmRelease, plural: helmreleases, singular: helmrelease}
 scope: Namespaced
 versions:
 - name: v2
   served: true
   storage: true
   schema:
    openAPIV3Schema:
     type: object
     x-kubernetes-preserve-unknown-fields: true
`

func crdFixture(t *testing.T, mode string) string {
	t.Helper()
	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux/ks.yaml]\n")
	testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n")
	rs := strings.ReplaceAll(
		issueResourceSet,
		"        interval: 1h\n",
		"        suspend: true\n        interval: 1h\n",
	)
	testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
	switch mode {
	case "supplied":
		testutil.WriteFile(t, dir, "apps/crd.yaml", helmReleaseCRD)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- crd.yaml\n")
	case "indexed", "unreferenced":
		testutil.WriteFile(t, dir, "indexed/crd.yaml", helmReleaseCRD)
		if mode == "indexed" {
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- ../indexed/crd.yaml\n")
		}
	case "produced", "unordered":
		dependency := ""
		if mode == "produced" {
			dependency = "crds"
		}
		testutil.WriteFile(
			t,
			dir,
			"flux/ks.yaml",
			ksYAML("apps", "apps", dependency)+"---\n"+ksYAML("crds", "crds", ""),
		)
		testutil.WriteFile(t, dir, "crds/kustomization.yaml", "resources:\n- crd.yaml\n")
		testutil.WriteFile(t, dir, "crds/crd.yaml", helmReleaseCRD)
	case "helm produced", "sibling hr":
		testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
		if mode == "helm produced" {
			rs = strings.Replace(
				rs,
				"  dependsOn:\n",
				"  dependsOn:\n    - apiVersion: helm.toolkit.fluxcd.io/v2\n      kind: HelmRelease\n      name: crd-producer\n",
				1,
			)
		}
		testutil.WriteFile(t, dir, "apps/rs.yaml", rs)
		testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: crd-producer, namespace: flux-system}
spec:
 interval: 1h
 chart:
  spec:
   chart: charts/crds
   sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- rs.yaml\n- producer.yaml\n")
		testutil.WriteFile(t, dir, "charts/crds/Chart.yaml", "apiVersion: v2\nname: crds\nversion: 0.1.0\n")
		testutil.WriteFile(t, dir, "charts/crds/templates/crd.yaml", helmReleaseCRD)
	}
	return dir
}

func assertIssueChildren(t *testing.T, o *Orchestrator) {
	t.Helper()
	id := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "flux-operator",
	}
	info, ok := o.store.GetStatus(id)
	{
		assert.FatalIf(t, !ok || info.Status != store.StatusReady, "ResourceSet status=%+v", info)
	}
	art, ok := o.store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	if !ok || len(art.Manifests) != 2 {
		t.Fatalf(
			"ResourceSet artifact=%+v status=%+v present=%t inventory=%t",
			art,
			info,
			o.store.GetObject(id) != nil,
			o.store.HasInventory(id),
		)
	}
	docs, err := testutil.DecodeDocs(strings.NewReader(issueResourceSet))
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	spec := docs[0]["spec"].(map[string]any)
	children := spec["resources"].([]any)
	want := make([]map[string]any, 0, len(children))
	for _, child := range children {
		doc := child.(map[string]any)
		doc["spec"].(map[string]any)["suspend"] = true
		doc["metadata"].(map[string]any)["labels"] = map[string]any{
			"resourceset.fluxcd.controlplane.io/name":      "flux-operator",
			"resourceset.fluxcd.controlplane.io/namespace": "flux-system",
		}
		want = append(want, doc)
		obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
		{
			{
				diff := cmp.Diff(
					o.store.GetObject(obj.Named()),
					obj,
					cmp.AllowUnexported(manifest.Kustomization{}, manifest.OCIRepository{}, manifest.HelmChartSource{}),
					cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
					cmpopts.IgnoreUnexported(manifest.HelmRelease{}),
				)
				assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
			}
		}
	}
	slices.SortFunc(
		want,
		func(a, b map[string]any) int { return strings.Compare(manifest.DocKind(a), manifest.DocKind(b)) },
	)
	got := slices.Clone(art.Manifests)
	slices.SortFunc(got, func(a, b manifest.DocOccurrence) int {
		return strings.Compare(manifest.DocKind(a.Document), manifest.DocKind(b.Document))
	})
	assert.Diff(t, testutil.DocumentMaps(got), want)
}

func TestMissingCRDs_ChildrenAndProducers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		mode    string
		enabled bool
	}{
		{"absent", false},
		{"supplied", false},
		{"indexed", false},
		{"produced", false},
		{"helm produced", false},
		{"unreferenced", false},
		{"unordered", false},
		{"sibling hr", false},
		{"absent", true},
		{"unreferenced", true},
		{"produced", true},
		{"helm produced", true},
	} {
		t.Run(fmt.Sprintf("enabled_%t/%s", tc.enabled, tc.mode), func(t *testing.T) {
			t.Parallel()
			testMissingCRDsChildrenAndProducersCase(t, &tc)
		},
		)
	}
}

func acceptCRDGatesForTest(t *testing.T, st *store.Store, obj manifest.BaseManifest) {
	t.Helper()
	reads := new(store.ReadSet)
	reads.Init(st, obj.Named(), nil)
	reads.GetObject(obj.Named())
	for ref := range manifest.References(obj, manifest.ReferenceAll) {
		if ref.Gate == 0 || !ref.IsCRD() {
			continue
		}
		reads.GetObject(ref.NamedResource)
		reads.Require(ref.DependencyRef)
	}
	reads.Accept()
	assert.Equal(t, reads.Valid(), true)
	reads.Retire()
	for range st.Sweep() {
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestWarnMissingCRDs_NoAcceptedInputs(t *testing.T) {
	manifest.KindOf(&manifest.RawObject{APIVersion: "apiextensions.k8s.io/v1", Kind: manifest.KindCustomResourceDefinition}).
		Namespaced()
	var allocations [2]float64
	for shape, repeats := range []int{1, 32} {
		t.Run(fmt.Sprintf("references=%d", repeats*2), func(t *testing.T) {
			st := store.New()
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps"}
			for range repeats {
				for _, name := range []string{"a.example.com", "b.example.com"} {
					rs.DependsOn = append(
						rs.DependsOn,
						fluxopv1.Dependency{
							APIVersion: "apiextensions.k8s.io/v1",
							Kind:       manifest.KindCustomResourceDefinition,
							Name:       name,
						},
					)
				}
			}
			st.AddObject(rs)
			st.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusReady})
			st.RecordEmitted(rs.Named(), &store.Emission{Complete: true}, nil)
			o := &Orchestrator{
				store: st,
				cfg:   treeConfig{Config: Config{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}}},
			}
			allocations[shape] = testing.AllocsPerRun(10, o.warnMissingCRDs)
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
	t.Logf("allocations for 2/64 declared references without accepted inputs: %v", allocations)
	assert.Equal(t, allocations[1], allocations[0])
}

func TestWarnMissingCRDs_FinalEligibilityAndOrdering(t *testing.T) {
	t.Parallel()
	s := store.New()
	o := &Orchestrator{
		store: s,
		cfg: treeConfig{
			Config: Config{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}},
			Tree:   Tree{},
		},
	}
	crdA := manifest.NamedResource{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "a.example.com",
	}
	crdB := manifest.NamedResource{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "b.example.com",
	}
	ungatedCRD := manifest.NamedResource{Group: crdA.Group, Kind: crdA.Kind, Namespace: "ns"}
	for _, tc := range []struct {
		name                string
		status              store.Status
		hasStatus, artifact bool
	}{
		{"z", store.StatusReady, true, true},
		{"a", store.StatusReady, true, true},
		{"failed", store.StatusFailed, true, true},
		{"pending", store.StatusPending, true, true},
		{"no status", store.StatusReady, false, true},
		{"no artifact", store.StatusReady, true, false},
		{"source artifact", store.StatusReady, true, true},
		{"no accepted inputs", store.StatusReady, true, true},
		{"no CRD inputs", store.StatusReady, true, true},
	} {
		rs := &manifest.ResourceSet{
			Name:      tc.name,
			Namespace: "ns",
			DependsOn: []fluxopv1.Dependency{
				{APIVersion: crdB.Group + "/v1", Kind: crdB.Kind, Name: crdB.Name, Namespace: "explicit"},
				{APIVersion: crdA.Group + "/v1", Kind: crdA.Kind, Name: crdA.Name},
				{APIVersion: crdA.Group + "/v1", Kind: crdA.Kind, Name: crdA.Name, Namespace: "explicit"},
				{APIVersion: manifest.CoreAPIVersion, Kind: manifest.KindConfigMap, Name: "missing"},
			},
		}
		rs.DependsOn = append(rs.DependsOn,
			fluxopv1.Dependency{APIVersion: "example.test/v1", Kind: crdA.Kind, Name: crdA.Name},
			fluxopv1.Dependency{APIVersion: "v1", Kind: crdB.Kind, Name: crdB.Name})
		for _, dep := range rs.DependsOn {
			manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
		}
		s.AddObject(rs)
		if tc.hasStatus {
			s.UpdateStatus(rs.Named(), store.StatusInfo{Status: tc.status, Reason: "", Message: ""})
		}
		if tc.artifact {
			if tc.name == "source artifact" {
				s.SetArtifact(rs.Named(), &store.SourceArtifact{})
			} else {
				s.RecordEmitted(rs.Named(), &store.Emission{Complete: true}, nil)
			}
		}
		if tc.name == "no accepted inputs" {
			continue
		}
		if tc.name == "no CRD inputs" {
			reads := new(store.ReadSet)
			reads.Init(s, rs.Named(), nil)
			reads.GetObject(rs.Named())
			reads.RequireData(
				manifest.NamedResource{Group: "", Kind: manifest.KindConfigMap, Namespace: "ns", Name: "missing"},
			)
			reads.Accept()
			assert.Equal(t, reads.Valid(), true)
			reads.Retire()
			for range s.Sweep() {
			}
			continue
		}
		acceptCRDGatesForTest(t, s, rs)
	}
	testWarnMissingCRDsFinalEligibilityAndOrderingTail(
		t,
		&testWarnMissingCRDsFinalEligibilityAndOrderingTailInputs{
			o:          &o,
			crdA:       &crdA,
			crdB:       &crdB,
			s:          &s,
			ungatedCRD: &ungatedCRD,
		},
	)
}

func testWarnMissingCRDsFinalEligibilityAndOrderingTail(
	t *testing.T,
	tailCaseInput *testWarnMissingCRDsFinalEligibilityAndOrderingTailInputs,
) {
	t.Helper()
	(*tailCaseInput.o).
		warnMissingCRDs()
	want := slices.Grow([]manifest.Warning{}, 2-0)
	for _, crd := range []manifest.NamedResource{(*tailCaseInput.crdA), (*tailCaseInput.crdB)} {
		want = append(
			want,
			manifest.Warning{
				Resource: crd,
				Subject:  manifest.DiagnosticFor(crd, nil),
				Related: []manifest.NamedResource{
					{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "ns", Name: "a"},
					{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "ns", Name: "z"},
				},
				Category: manifest.WarnMissingCRD,
				Count:    1,
				Message:  "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet.fluxcd.controlplane.io/ns/a, ResourceSet.fluxcd.controlplane.io/ns/z",
			},
		)
	}
	assert.Diff(t, (*tailCaseInput.s).Warnings(), want)
	{
		tcValues := []struct {
			name         string
			declared     []manifest.NamedResource
			inputsFrom   []fluxopv1.InputProviderReference
			gated        []manifest.NamedResource
			semantic     []manifest.NamedResource
			required     []manifest.NamedResource
			warned       []manifest.NamedResource
			semanticOnly bool
		}{
			{name: "no accepted footprint", declared: []manifest.NamedResource{(*tailCaseInput.crdA)}},
			{name: "semantic input", semantic: []manifest.NamedResource{(*tailCaseInput.crdA)}, required: []manifest.NamedResource{(*tailCaseInput.crdA)}, semanticOnly: true},
			{
				name:       "ungated CRD reference",
				inputsFrom: []fluxopv1.InputProviderReference{{APIVersion: tailCaseInput.crdA.Group + "/v1", Kind: tailCaseInput.crdA.Kind}},
				semantic:   []manifest.NamedResource{(*tailCaseInput.ungatedCRD)}, required: []manifest.NamedResource{(*tailCaseInput.ungatedCRD)},
			},
			{
				name: "gated and semantic inputs", declared: []manifest.NamedResource{(*tailCaseInput.crdA)},
				gated: []manifest.NamedResource{(*tailCaseInput.crdA)}, semantic: []manifest.NamedResource{(*tailCaseInput.crdB)},
				required: []manifest.NamedResource{(*tailCaseInput.crdA), (*tailCaseInput.crdB)}, warned: []manifest.NamedResource{(*tailCaseInput.crdA)},
			},
			{
				name: "partial acceptance", declared: []manifest.NamedResource{(*tailCaseInput.crdA), (*tailCaseInput.crdB)},
				gated: []manifest.NamedResource{(*tailCaseInput.crdA)}, required: []manifest.NamedResource{(*tailCaseInput.crdA)},
				warned: []manifest.NamedResource{(*tailCaseInput.crdA)},
			},
		}
		for tcIndex := range tcValues {
			tc := tcValues[tcIndex]

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				testWarnMissingCRDsFinalEligibilityAndOrderingCase3(
					t,
					testWarnMissingCRDsFinalEligibilityAndOrderingCase3Inputs{tc: &tc},
				)
			},
			)

		}
	}
	for _, state := range []string{"object", "ready", "pending", "failed", "disabled", "foreign object", "foreign status"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testWarnMissingCRDsFinalEligibilityAndOrderingCase4(
				t,
				testWarnMissingCRDsFinalEligibilityAndOrderingCase4Inputs{crdA: &(*tailCaseInput.crdA), state: &state},
			)
		},
		)
	}
}

func testMissingCRDsChildrenAndProducersCase(
	t *testing.T,
	testCaseInput *struct {
		mode string

		enabled bool
	},
) {
	t.Helper()

	dir := crdFixture(t, testCaseInput.mode)
	var allowed []string
	if testCaseInput.enabled {
		allowed = []string{manifest.KindCustomResourceDefinition}
	}
	renderedSingleton1, err := RenderTrees(
		t.Context(),
		[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{CacheDir: t.TempDir(), Concurrency: 2, AllowMissingKinds: allowed},
	)
	{
		assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
	}
	o := renderedSingleton1[0].Orchestrator
	res := renderedSingleton1[0].Result
	if (testCaseInput.mode == "absent" || testCaseInput.mode == "unreferenced") &&
		!testCaseInput.enabled {
		{
			assert.FatalIf(
				t,
				err == nil || !strings.Contains(err.Error(), "dependency not found"),
				"strict absent error=%v",
				err,
			)
		}
		assert.Equal(
			t,
			o.store.GetArtifact(
				manifest.NamedResource{
					Group:     "fluxcd.controlplane.io",
					Kind:      manifest.KindResourceSet,
					Namespace: "flux-system",
					Name:      "flux-operator",
				},
				store.ArtifactContent,
			) == nil,
			true,
		)
		return
	}
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	assertIssueChildren(t, o)
	crd := manifest.NamedResource{
		Group: "apiextensions.k8s.io",
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "helmreleases.helm.toolkit.fluxcd.io",
	}
	var warnings []manifest.Warning
	{
		wValues := res.Warnings
		for wIndex := range wValues {
			w := wValues[wIndex]

			if w.Category == manifest.WarnMissingCRD {
				warnings = append(warnings, w)
			}
		}
	}
	if testCaseInput.mode == "absent" || testCaseInput.mode == "unreferenced" {
		assert.Diff(
			t,
			warnings,
			[]manifest.Warning{
				{
					Resource: crd,
					Subject:  manifest.DiagnosticFor(crd, nil),
					Related: []manifest.NamedResource{
						{
							Group:     "fluxcd.controlplane.io",
							Kind:      manifest.KindResourceSet,
							Namespace: "flux-system",
							Name:      "flux-operator",
						},
					},
					Category: manifest.WarnMissingCRD,
					Count:    1,
					Message:  "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet.fluxcd.controlplane.io/flux-system/flux-operator",
				},
			},
		)
		assert.Equal(t, o.store.GetObject(crd) == nil, true)
	} else {
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, o.store.GetObject(crd) != nil, true)
	}
}

func testWarnMissingCRDsFinalEligibilityAndOrderingCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		declared []manifest.
				NamedResource
		inputsFrom []fluxopv1.InputProviderReference
		gated      []manifest.
				NamedResource
		semantic     []manifest.NamedResource
		required     []manifest.NamedResource
		warned       []manifest.NamedResource
		semanticOnly bool
	},
) {
	t.Helper()

	st := store.New()
	rs := &manifest.ResourceSet{Name: "app", Namespace: "ns"}
	rs.InputsFrom = testCaseInput.inputsFrom
	for _, crd := range testCaseInput.declared {
		dep := fluxopv1.Dependency{
			APIVersion: crd.Group + "/v1",
			Kind:       crd.Kind,
			Name:       crd.Name,
			Namespace:  "explicit",
		}
		manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
		rs.DependsOn = append(rs.DependsOn, dep)
	}
	var owner manifest.BaseManifest = rs
	if testCaseInput.semanticOnly {
		owner = &manifest.Kustomization{Name: "app", Namespace: "ns"}
	}
	st.AddObject(owner)
	st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady})
	st.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
	st.SetArtifact(owner.Named(), &store.SourceArtifact{Key: "coexisting-content"})
	if len(testCaseInput.gated)+len(testCaseInput.semantic) != 0 {
		reads := new(store.ReadSet)
		reads.Init(st, owner.Named(), nil)
		reads.GetObject(owner.Named())
		for _, id := range testCaseInput.gated {
			reads.GetObject(id)
			reads.Require(manifest.DependencyRef{NamedResource: id})
		}
		for _, id := range testCaseInput.semantic {
			reads.RequireData(id)
		}
		reads.Accept()
		assert.Equal(t, reads.Valid(), true)
		reads.Retire()
		for range st.Sweep() {
		}
	}
	assert.Diff(
		t,
		slices.SortedFunc(st.RequiredInputs(owner.Named()), manifest.NamedResource.Compare), testCaseInput.required,
	)
	(&Orchestrator{store: st, cfg: treeConfig{Config: Config{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}}}}).warnMissingCRDs()
	var want []manifest.Warning
	if len(testCaseInput.warned) > 0 {
		want = make([]manifest.Warning, 0, len(testCaseInput.warned))
	}
	for _, crd := range testCaseInput.warned {
		want = append(want, manifest.Warning{
			Resource: crd,
			Subject:  manifest.DiagnosticFor(crd, nil),
			Related:  []manifest.NamedResource{owner.Named()},
			Category: manifest.WarnMissingCRD,
			Count:    1,
			Message: "CRD " + crd.String() + " absent from offline inputs; accepted for " + owner.Named().
				String(),
		})
	}
	assert.Diff(t, st.Warnings(), want)
}

type testWarnMissingCRDsFinalEligibilityAndOrderingCase2Inputs struct {
	crdA  *manifest.NamedResource
	state *string
}

func testWarnMissingCRDsFinalEligibilityAndOrderingCase2(
	t *testing.T,
	testCaseInput testWarnMissingCRDsFinalEligibilityAndOrderingCase2Inputs,
) {
	t.Helper()

	st := store.New()
	rs := &manifest.ResourceSet{
		Name:      "app",
		Namespace: "ns",
		DependsOn: []fluxopv1.Dependency{
			{
				APIVersion: testCaseInput.crdA.Group + "/v1",
				Kind:       testCaseInput.crdA.Kind,
				Name:       testCaseInput.crdA.Name,
				Namespace:  "explicit",
			},
		},
	}
	for _, dep := range rs.DependsOn {
		manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
	}
	st.AddObject(rs)
	st.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	st.RecordEmitted(rs.Named(), &store.Emission{Complete: true}, nil)
	cfg := treeConfig{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}, Tree: Tree{}}
	switch *testCaseInput.state {
	case "foreign object":
		st.AddObject(
			&manifest.RawObject{
				APIVersion: "example.test/v1",
				Kind:       testCaseInput.crdA.Kind,
				Name:       testCaseInput.crdA.Name,
			},
		)
	case "foreign status":
		foreign := (*testCaseInput.crdA)
		foreign.Group = "example.test"
		st.UpdateStatus(foreign, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	case "object":
		st.AddObject(
			&manifest.RawObject{
				APIVersion: testCaseInput.crdA.Group + "/v1",
				Kind:       testCaseInput.crdA.Kind,
				Name:       testCaseInput.crdA.Name,
			},
		)
	case "disabled":
		cfg.AllowMissingKinds = nil
	default:
		status := store.StatusReady
		if (*testCaseInput.state) == "pending" {
			status = store.StatusPending
		}
		if (*testCaseInput.state) == "failed" {
			status = store.StatusFailed
		}
		st.UpdateStatus((*testCaseInput.crdA), store.StatusInfo{Status: status, Reason: "", Message: ""})
	}
	acceptCRDGatesForTest(t, st, rs)
	(&Orchestrator{store: st, cfg: cfg}).warnMissingCRDs()
	want := 0
	if strings.HasPrefix((*testCaseInput.state), "foreign") {
		want = 1
	}
	assert.Equal(t, len(st.Warnings()), want)
}

type testWarnMissingCRDsFinalEligibilityAndOrderingCase3Inputs struct {
	tc *struct {
		name       string
		declared   []manifest.NamedResource
		inputsFrom []fluxopv1.
				InputProviderReference
		gated    []manifest.NamedResource
		semantic []manifest.NamedResource

		required []manifest.NamedResource

		warned       []manifest.NamedResource
		semanticOnly bool
	}
}

func testWarnMissingCRDsFinalEligibilityAndOrderingCase3(
	t *testing.T,
	testCaseInput testWarnMissingCRDsFinalEligibilityAndOrderingCase3Inputs,
) {
	t.Helper()

	testWarnMissingCRDsFinalEligibilityAndOrderingCase(
		t,
		testCaseInput.tc,
	)
}

type testWarnMissingCRDsFinalEligibilityAndOrderingCase4Inputs struct {
	crdA  *manifest.NamedResource
	state *string
}

func testWarnMissingCRDsFinalEligibilityAndOrderingCase4(
	t *testing.T,
	testCaseInput testWarnMissingCRDsFinalEligibilityAndOrderingCase4Inputs,
) {
	t.Helper()

	testWarnMissingCRDsFinalEligibilityAndOrderingCase2(
		t,
		testWarnMissingCRDsFinalEligibilityAndOrderingCase2Inputs(testCaseInput),
	)
}

type testWarnMissingCRDsFinalEligibilityAndOrderingTailInputs struct {
	o          **Orchestrator
	crdA       *manifest.NamedResource
	crdB       *manifest.NamedResource
	s          **store.Store
	ungatedCRD *manifest.NamedResource
}
