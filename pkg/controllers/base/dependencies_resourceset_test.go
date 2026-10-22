package base

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func BenchmarkCollectDeps_Scope(b *testing.B) {
	for _, target := range []struct{ api, kind string }{{"apiextensions.k8s.io/v1", manifest.KindCustomResourceDefinition}, {"v1", manifest.KindConfigMap}, {"example.com/v1", "Widget"}} {
		b.Run(target.kind, func(b *testing.B) {
			c := gateExecution(New(store.New(), task.NewBounded(2)), manifest.NamedResource{})
			rs := &manifest.ResourceSet{Name: "app", Namespace: "ns", DependsOn: []fluxopv1.Dependency{{APIVersion: target.api, Kind: target.kind, Name: "dependency"}}}
			manifest.KindOf(&manifest.RawObject{APIVersion: target.api, Kind: target.kind}).Namespaced()
			for b.Loop() {
				c.dependencies(rs, manifest.GateInputs)
			}
		})
	}
}

func TestDependencies_Resourceset_ScopeAndImmutability(t *testing.T) {
	s := store.New()
	c := gateExecution(New(s, task.NewBounded(2)), manifest.NamedResource{})
	for _, tc := range []struct{ api, kind, namespace, want string }{
		{"apiextensions.k8s.io/v1", manifest.KindCustomResourceDefinition, "", ""}, {"rbac.authorization.k8s.io/v1", "ClusterRole", "", ""}, {"v1", "Namespace", "", ""},
		{"apiextensions.k8s.io/v1", manifest.KindCustomResourceDefinition, "explicit", ""}, {"rbac.authorization.k8s.io/v1", "ClusterRole", "explicit", ""}, {"v1", "Namespace", "explicit", ""},
		{"v1", manifest.KindConfigMap, "", "apps"}, {"example.com/v1", "Widget", "other", "other"},
		{"v1", manifest.KindSecret, "", "apps"}, {"example.com/v1", "Widget", "", "apps"}, {"v1", manifest.KindConfigMap, "other", "other"},
		{"nvidia.com/v1", "ClusterPolicy", "", "apps"}, {"nvidia.com/v1", "ClusterPolicy", "explicit", "explicit"},
		{"foreign.example/v1", "ClusterPolicy", "", "apps"}, {"foreign.example/v1", "ClusterPolicy", "other", "other"},
		{"node.k8s.io/v1", "RuntimeClass", "", ""}, {"storage.k8s.io/v1", "CSIDriver", "", ""}, {"rbac.authorization.k8s.io/v999", "ClusterRole", "", "apps"}, {"foreign.example/v1", "ClusterRole", "", "apps"}, {"foreign.example/v1", "ClusterRole", "other", "other"},
	} {
		t.Run(tc.api+"/"+tc.kind+"/"+tc.namespace, func(t *testing.T) {
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{APIVersion: tc.api, Kind: tc.kind, Name: "dependency", Namespace: tc.namespace, Ready: true, ReadyExpr: "true"}}}
			manifest.KindOf(&manifest.RawObject{APIVersion: tc.api, Kind: tc.kind}).Namespaced()
			s.AddObject(rs)
			want := []manifest.DependencyRef{{Group: schema.FromAPIVersionAndKind(tc.api, tc.kind).Group, Kind: tc.kind, Name: "dependency", Namespace: tc.want, ReadyExpr: new("true")}}
			assert.Diff(t, c.dependenciesForTest(rs), want)
			assert.Equal(t, rs.DependsOn[0].Namespace, tc.namespace)
			stored, ok := s.Get[*manifest.ResourceSet](rs.Named())
			if !ok {
				t.Fatal("missing stored ResourceSet")
			}
			assert.Equal(t, stored.DependsOn[0].Namespace, tc.namespace)
		})
	}
}

func TestCollectDeps_ParentAndInputs(t *testing.T) {
	t.Parallel()
	for _, hasParent := range []bool{false, true} {
		t.Run(fmt.Sprint(hasParent), func(t *testing.T) {
			t.Parallel()
			c := gateExecution(New(store.New(), task.NewBounded(2)), manifest.NamedResource{})
			parent := testutil.KustomizationID("apps", "parent")
			parents := map[manifest.NamedResource]manifest.NamedResource{}
			if hasParent {
				parents[manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "app", Namespace: "apps"}] = parent
			}
			c.Configure(Options{Index: parentIndexForTest(parents)})
			dependency := &manifest.Kustomization{Name: "pending", Namespace: "apps"}
			c.Store.AddObject(dependency)
			c.Store.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusPending, Message: "working"})
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization, Name: "pending"}}, InputsFrom: []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: "named"}, {Kind: manifest.KindResourceSetInputProvider}}}
			rs.InputsFrom = append(rs.InputsFrom, rs.InputsFrom[0],
				fluxopv1.InputProviderReference{APIVersion: "example.test/v1", Name: "named"},
				fluxopv1.InputProviderReference{APIVersion: "v1", Name: "named"},
				fluxopv1.InputProviderReference{Kind: manifest.KindResourceSet, Name: "named"})
			want := []manifest.DependencyRef{{NamedResource: dependency.Named()}, {Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "named"}}
			want = append(want, want[1],
				manifest.DependencyRef{Group: "example.test", Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "named"},
				manifest.DependencyRef{Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "named"},
				manifest.DependencyRef{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "named"})
			manifest.KindOf(&manifest.RawObject{APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization}).Namespaced()
			if hasParent {
				c.Store.AddObject(rs)
				for _, name := range []string{"z-parent", "a-parent"} {
					owner := &manifest.Kustomization{Name: name, Namespace: "apps"}
					c.Store.AddObject(owner)
					c.Store.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: rs, Productions: []manifest.Production{
						{Owner: rs.Named(), File: "self.yaml"},
						{Path: "file.yaml"},
					}}}, Complete: true}, nil)
				}

			}
			deps := c.dependencies(rs, manifest.GateInputs)
			assert.Diff(t, deps, want)
			w := &depwait.Waiter{Store: c.Store}
			assert.Equal(t, rs.DependsOn[0].Ready, false)
			assert.Equal(t, w.Classify(t.Context(), deps[0], manifest.DrainNone).Kind, schedule.Blocked)
		})
	}
}
