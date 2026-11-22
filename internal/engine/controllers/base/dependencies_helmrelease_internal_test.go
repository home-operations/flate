package base

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/admissiontest"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkCollectHRDeps(b *testing.B) {
	id := testutil.HelmReleaseID("apps", "target")
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		DependsOn: []helmv2.DependencyReference{{Name: id.Name, Namespace: id.Namespace}},
	}
	st := store.New()
	c := gateExecution(New(st, nil), manifest.NamedResource{})
	admissionLoader1, _ := admissiontest.New(
		b,
		admissiontest.Config{
			Changes: detectChanges(b, []string{"target.yaml"}, []string{"target.yaml"}),
			Index: &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}},
			},
			Inventory: st,
			Root:      "",
		},
	)
	admissiontest.Commit(b, admissionLoader1.Options.Session, nil)
	c.Configure(Options{Session: admissionLoader1.Options.Session, Loader: admissionLoader1})
	b.ReportAllocs()
	for b.Loop() {
		collectGates(c, hr, manifest.GateInputs)
	}
}

func TestController_CollectHRDepsOrderingParity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                      string
		active, selected, indexed bool
		present                   bool
		status                    store.Status
	}{
		{name: "unfiltered missing"},
		{name: "known indexed target", active: true, indexed: true},
		{name: "generated target", active: true, present: true},
		{name: "unknown retained", active: true},
		{name: "selected ready", active: true, selected: true, present: true, status: store.StatusReady},
		{name: "selected failed", active: true, selected: true, present: true, status: store.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testControllerCollectHRDepsOrderingParityCase(
				t,
				&tc,
			)
		},
		)
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestController_CollectHRDepsClone(t *testing.T) {
	c := gateExecution(New(store.New(), task.NewBounded(2)), manifest.NamedResource{})
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "demo",
		Namespace: "default",
		DependsOn: []helmv2.DependencyReference{
			{Namespace: "default", Name: "other"},
			{Name: "second"},
			{Name: "third"},
		},
	}
	assert.Equal(t, testing.AllocsPerRun(100, func() { collectGates(c, hr, manifest.GateInputs) }), float64(1))
	got := collectGates(c, hr, manifest.GateInputs)
	assert.Diff(
		t,
		got,
		[]manifest.DependencyRef{
			{NamedResource: testutil.HelmReleaseID("default", "other"), RequiresDeclaration: true},
			{NamedResource: testutil.HelmReleaseID("default", "second"), RequiresDeclaration: true},
			{NamedResource: testutil.HelmReleaseID("default", "third"), RequiresDeclaration: true},
		},
	)
	got[0].Name = "mutated"
	assert.Equal(t, hr.DependsOn[0].Name, "other")
}

func TestDependencies_HelmRelease_ArtifactStages(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{
		Name:      "consumer",
		Namespace: "apps",
		DependsOn: []helmv2.DependencyReference{{Name: "ordered", ReadyExpr: "dep.spec.suspend == false"}},
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "chart"},
		},
	}
	hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmChart, Name: "chart"}
	for _, tc := range []struct {
		name     string
		stage    manifest.GateStage
		artifact bool
	}{
		{"inputs", manifest.GateInputs, false},
		{"artifacts", manifest.GateArtifacts, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := gateExecution(New(store.New(), nil), manifest.NamedResource{})
			want := []manifest.DependencyRef{{
				Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "chart",
				Artifact: tc.artifact, ChartSource: true, RequiresDeclaration: true,
			}}
			if tc.stage == manifest.GateInputs {
				want = append(want, manifest.DependencyRef{
					NamedResource:       testutil.HelmReleaseID("apps", "ordered"),
					ReadyExpr:           new("dep.spec.suspend == false"),
					RequiresDeclaration: true,
				})
			}
			got := collectGates(c, hr, tc.stage)
			assert.Diff(t, got, want)
		})
	}
}

func TestDependencies_HelmRelease_DeclarationAvailabilityUsesCommonGate(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, nil)
	parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
	child := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "child",
		Namespace: "apps",
	}
	for _, object := range []manifest.BaseManifest{parent, child} {
		st.AddObject(object)
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Named(): {
			Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}},
		},
	}})
	st.UpdateStatus(
		parent.Named(),
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"},
	)
	_, _, next := st.GetEmission(parent.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: child}}
	st.RecordEmitted(parent.Named(), &next, nil)
	run := gateExecution(c, child.Named())
	deps := collectGates(run, child, manifest.GateInputs)
	assert.FatalIf(t, len(deps) != 0, "structural declaration owners leaked into authored dependencies: %v", deps)
	fresh := run.Reads.GetDeclaration(child.Named())
	err := run.Require(t.Context(), deps, manifest.DrainNone)
	ok := fresh != nil
	assert.FatalIf(
		t,
		!ok || fresh != child || err != nil,
		"accepted child waited for Pending owner: %v %t %v",
		fresh,
		ok,
		err,
	)
}

func testControllerCollectHRDepsOrderingParityCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		active   bool
		selected bool
		indexed  bool
		present  bool
		status   store.Status
	},
) {
	t.Helper()

	target := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "target",
		Namespace: "apps",
	}
	var changes *change.Set
	if testCaseInput.active {
		files := []string{"consumer.yaml"}
		if testCaseInput.selected {
			files = append(files, "target.yaml")
		}
		changes = detectChanges(t, files, files)
	}
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "consumer",
		Namespace: "apps",
		DependsOn: []helmv2.DependencyReference{
			{Name: target.Name, Namespace: target.Namespace, ReadyExpr: "dep.spec.suspend == false"},
		},
	}
	filterLoader, _ := admissiontest.New(
		t,
		admissiontest.Config{
			Changes: changes,
			Index: &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					hr.Named():     {File: "consumer.yaml"},
					target.Named(): {File: "target.yaml"},
				},
			},
			Inventory: store.New(),
			Root:      "",
		},
	)
	admissiontest.Commit(t, filterLoader.Options.Session, nil)
	filter := filterLoader.Options.Session
	st := store.New()
	c := gateExecution(New(st, task.NewBounded(2)), manifest.NamedResource{})
	c.Configure(
		Options{
			Session:   filter,
			Existence: indexedOrderingDeps{target.Named(): testCaseInput.indexed},
			Loader:    filterLoader,
		},
	)
	if testCaseInput.present {
		st.AddObject(target)
		st.UpdateStatus(
			target.Named(),
			store.StatusInfo{Status: testCaseInput.status, Reason: "", Message: "fixture"},
		)
	}
	decision := filter.Snapshot()
	want := []manifest.DependencyRef{{
		NamedResource: target.Named(), ReadyExpr: new("dep.spec.suspend == false"), RequiresDeclaration: true,
	}}
	assert.Diff(t, sortedDependencies(collectGates(c, hr, manifest.GateInputs)), sortedDependencies(want))
	assert.ErrorIf(
		t,
		filter.Snapshot() != decision || len(hr.DependsOn) != 1,
		"%v",
		"ordering collection must preserve keep and input edges",
	)
}
