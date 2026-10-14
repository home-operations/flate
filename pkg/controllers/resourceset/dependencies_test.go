package resourceset

import (
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestCollectDeps_ScopeAndImmutability(t *testing.T) {
	c, s := newController(t)
	for _, tc := range []struct{ api, kind, namespace, want string }{
		{"", manifest.KindCustomResourceDefinition, "", ""}, {"", "ClusterRole", "", ""}, {"", "Namespace", "", ""},
		{"", manifest.KindCustomResourceDefinition, "explicit", ""}, {"", "ClusterRole", "explicit", ""}, {"", "Namespace", "explicit", ""},
		{"", manifest.KindConfigMap, "", "apps"}, {"", "Widget", "other", "other"},
		{"", manifest.KindSecret, "", "apps"}, {"", "Widget", "", "apps"}, {"", manifest.KindConfigMap, "other", "other"},
		{"node.k8s.io/v1", "RuntimeClass", "", "apps"}, {"storage.k8s.io/v1", "CSIDriver", "", "apps"}, {"foreign.example/v1", "ClusterRole", "", ""},
	} {
		t.Run(tc.api+"/"+tc.kind+"/"+tc.namespace, func(t *testing.T) {
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{APIVersion: tc.api, Kind: tc.kind, Name: "dependency", Namespace: tc.namespace, Ready: true, ReadyExpr: "true"}}}
			s.AddObject(rs)
			want := []manifest.DependencyRef{{Kind: tc.kind, Name: "dependency", Namespace: tc.want, ReadyExpr: "true"}}
			assert.Diff(t, c.collectDeps(rs), want)
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
	for _, hasParent := range []bool{false, true} {
		t.Run(fmt.Sprint(hasParent), func(t *testing.T) {
			c := New(store.New(), task.NewBounded(2), true)
			parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
			calls := 0
			c.Configure(Options{ParentOf: func(manifest.NamedResource) (manifest.NamedResource, bool) { calls++; return parent, hasParent }})
			dependency := &manifest.Kustomization{Name: "pending", Namespace: "apps"}
			c.Store.AddObject(dependency)
			c.Store.UpdateStatus(dependency.Named(), store.StatusPending, "working")
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{Kind: manifest.KindKustomization, Name: "pending"}}, InputsFrom: []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: "named"}, {Kind: manifest.KindResourceSetInputProvider}}}
			rs.InputsFrom = append(rs.InputsFrom, rs.InputsFrom[0])
			want := []manifest.DependencyRef{{NamedResource: dependency.Named()}, {Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "named"}}
			want = append(want, want[1])
			if hasParent {
				want = append(want, manifest.DependencyRef{NamedResource: parent})
			}
			deps := c.collectDeps(rs)
			assert.Diff(t, deps, want)
			assert.Equal(t, calls, 1)
			w := &depwait.Waiter{Store: c.Store}
			assert.Equal(t, rs.DependsOn[0].Ready, false)
			assert.Equal(t, w.Classify(deps[0], manifest.DrainNone).Kind, depwait.ClassBlocked)
		})
	}
}
