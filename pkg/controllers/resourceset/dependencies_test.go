package resourceset

import (
	"fmt"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestCollectDeps_ScopeAndImmutability(t *testing.T) {
	c, s := newController(t)
	for _, tc := range []struct{ kind, namespace, want string }{
		{manifest.KindCustomResourceDefinition, "", ""}, {"ClusterRole", "", ""}, {"Namespace", "", ""},
		{manifest.KindCustomResourceDefinition, "explicit", ""}, {"ClusterRole", "explicit", ""}, {"Namespace", "explicit", ""},
		{manifest.KindConfigMap, "", "apps"}, {"Widget", "other", "other"},
		{manifest.KindSecret, "", "apps"}, {"Widget", "", "apps"}, {manifest.KindConfigMap, "other", "other"},
	} {
		t.Run(tc.kind+"/"+tc.namespace, func(t *testing.T) {
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{Kind: tc.kind, Name: "dependency", Namespace: tc.namespace, Ready: true, ReadyExpr: "true"}}}
			s.AddObject(rs)
			want := []manifest.DependencyRef{{Kind: tc.kind, Name: "dependency", Namespace: tc.want, Ready: true, ReadyExpr: "true", Use: "dependsOn"}}
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
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}}, InputsFrom: []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: "named"}, {Kind: manifest.KindResourceSetInputProvider}}}
			want := []manifest.DependencyRef{{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com", Use: "dependsOn", Ready: true}, {Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "named", Use: "inputsFrom", Content: true}}
			if hasParent {
				want = append(want, manifest.DependencyRef{NamedResource: parent, Ready: true})
			}
			assert.Diff(t, c.collectDeps(rs), want)
			assert.Equal(t, calls, 1)
		})
	}
}

func TestReconcile_DependsOnOmittedReadyWaitsForKustomization(t *testing.T) {
	for _, status := range []store.Status{store.StatusPending, store.StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			c, s := newController(t)
			dep := &manifest.Kustomization{Name: "platform", Namespace: "apps"}
			s.AddObject(dep)
			s.UpdateStatus(dep.Named(), status, "platform failed")
			rs := &manifest.ResourceSet{Name: "app", Namespace: "apps", DependsOn: []fluxopv1.Dependency{{Kind: manifest.KindKustomization, Name: dep.Name}}, ResourcesTemplate: "invalid: {{ undefined }}"}
			s.AddObject(rs)
			blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
			info, _ := s.GetStatus(rs.Named())
			if status == store.StatusPending {
				assert.Diff(t, blocked, []manifest.NamedResource{dep.Named()})
				assert.Equal(t, info.Status, store.StatusPending)
			} else {
				assert.Equal(t, len(blocked), 0)
				assert.Equal(t, info.Status, store.StatusFailed)
				if !strings.Contains(info.Message, "platform failed") {
					t.Fatalf("dependency failure = %q", info.Message)
				}
			}
			if s.GetArtifact(rs.Named()) != nil {
				t.Fatal("rendered ResourceSet before dependency readiness")
			}
			for ref := range manifest.References(rs) {
				if ref.Use == "dependsOn" {
					assert.Equal(t, ref.Ready, false)
				}
			}
			assert.Equal(t, rs.DependsOn[0].Ready, false)
		})
	}
}
