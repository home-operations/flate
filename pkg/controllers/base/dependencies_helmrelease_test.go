package base

import (
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkCollectHRDeps(b *testing.B) {
	id := testutil.HelmReleaseID("apps", "target")
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []helmv2.DependencyReference{{Name: id.Name, Namespace: id.Namespace}}}
	st := store.New()
	c := gateExecution(New(st, nil), manifest.NamedResource{})
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"target.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "target.yaml"}}}, st, "")})
	b.ReportAllocs()
	for b.Loop() {
		c.dependencies(hr, manifest.GateInputs)
	}
}

func TestController_CollectHRDepsOrderingFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                      string
		active, selected, indexed bool
		present                   bool
		status                    store.Status
		retain                    bool
	}{
		{name: "unfiltered missing", retain: true},
		{name: "known file excluded", active: true, indexed: true},
		{name: "render generated excluded", active: true, present: true},
		{name: "unknown retained", active: true, retain: true},
		{name: "selected ready", active: true, selected: true, present: true, status: store.StatusReady, retain: true},
		{name: "selected failed", active: true, selected: true, present: true, status: store.StatusFailed, retain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			target := &manifest.HelmRelease{Name: "target", Namespace: "apps"}
			var changes *change.Set
			if tc.active {
				files := []string{"consumer.yaml"}
				if tc.selected {
					files = append(files, "target.yaml")
				}
				changes = change.NewSet(files)
			}
			hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", DependsOn: []helmv2.DependencyReference{{Name: target.Name, Namespace: target.Namespace, ReadyExpr: "dep.spec.suspend == false"}}}
			filter := change.NewFilter(changes, &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "consumer.yaml"}, target.Named(): {File: "target.yaml"}}}, store.New(), "")
			st := store.New()
			c := gateExecution(New(st, task.NewBounded(2)), manifest.NamedResource{})
			c.Configure(Options{Filter: filter, Existence: indexedOrderingDeps{target.Named(): tc.indexed}})
			if tc.present {
				st.AddObject(target)
				st.UpdateStatus(target.Named(), store.StatusInfo{Status: tc.status, Reason: "", Message: "fixture"})
			}
			size := len(filter.KeepNames())
			var want []manifest.DependencyRef
			if tc.retain {
				want = []manifest.DependencyRef{{NamedResource: target.Named(), ReadyExpr: new("dep.spec.suspend == false")}}
			}
			assert.Diff(t, sortedDependencies(c.dependenciesForTest(hr)), sortedDependencies(want))
			if len(filter.KeepNames()) != size || len(hr.DependsOn) != 1 {
				t.Error("ordering collection must preserve keep and input edges")
			}
		})
	}
}

func TestController_CollectHRDepsClone(t *testing.T) {
	c := gateExecution(New(store.New(), task.NewBounded(2)), manifest.NamedResource{})
	hr := &manifest.HelmRelease{Name: "demo", Namespace: "default", DependsOn: []helmv2.DependencyReference{{Namespace: "default", Name: "other"}, {Name: "second"}, {Name: "third"}}}
	assert.Equal(t, testing.AllocsPerRun(100, func() { c.dependencies(hr, manifest.GateInputs) }), float64(1))
	got := c.dependenciesForTest(hr)
	assert.Diff(t, got, []manifest.DependencyRef{{NamedResource: testutil.HelmReleaseID("default", "other")}, {NamedResource: testutil.HelmReleaseID("default", "second")}, {NamedResource: testutil.HelmReleaseID("default", "third")}})
	got[0].Name = "mutated"
	assert.Equal(t, hr.DependsOn[0].Name, "other")
}

func TestDependencies_HelmRelease_ArtifactStages(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{
		Name: "consumer", Namespace: "apps",
		Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "chart"}},
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
				Artifact: tc.artifact, ChartSource: true,
			}}
			got := c.dependencies(hr, tc.stage)
			assert.Diff(t, got, want)
		})
	}
}

func TestDependencies_HelmRelease_DeclarationAvailabilityUsesCommonGate(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, nil)
	parent := &manifest.Kustomization{Name: "publisher", Namespace: "apps"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{parent, child})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Named(): {Productions: []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}},
	}})
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"})
	_, _, next := st.GetEmission(parent.Named())
	defer next.Finish()
	next.Children = []store.Child{{Obj: child}}
	st.RecordEmitted(parent.Named(), &next, nil)
	run := gateExecution(c, child.Named())
	deps := run.dependencies(child, manifest.GateInputs)
	if len(deps) != 0 {
		t.Fatalf("structural declaration owners leaked into authored dependencies: %v", deps)
	}
	fresh := run.Reads.GetDeclaration(child.Named())
	err := run.Require(t.Context(), deps, manifest.DrainNone)
	ok := fresh != nil
	if !ok || fresh != child || err != nil {
		t.Fatalf("accepted child waited for Pending owner: %v %t %v", fresh, ok, err)
	}
}
