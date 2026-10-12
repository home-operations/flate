package kustomization

import (
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type indexedOrderingDeps map[manifest.NamedResource]bool

func (i indexedOrderingDeps) IsFileIndexed(id manifest.NamedResource) bool { return i[id] }
func (indexedOrderingDeps) Promote(manifest.NamedResource) bool            { return false }

func TestCollectDeps_OrderingFilter(t *testing.T) {
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
			st := store.New()
			target := &manifest.Kustomization{Name: "target", Namespace: "apps", Path: "./target"}
			if tc.present {
				st.AddObject(target)
				st.UpdateStatus(target.Named(), tc.status, "fixture")
			}
			var changes *change.Set
			if tc.active {
				files := []string{"consumer.yaml"}
				if tc.selected {
					files = append(files, "target.yaml")
				}
				changes = change.NewSet(files)
			}
			consumer := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: target.Name, Namespace: target.Namespace, ReadyExpr: "dep.spec.suspend == false"}},
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source", Namespace: "apps"},
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values"}}},
			}
			filter := change.NewFilter(changes, &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {File: "consumer.yaml"}, target.Named(): {File: "target.yaml"}}}, st)
			parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
			c := New(st, nil, nil, false)
			c.Configure(Options{Filter: filter, Existence: indexedOrderingDeps{target.Named(): tc.indexed}, ParentOf: mapResolver(map[manifest.NamedResource]manifest.NamedResource{consumer.Named(): parent})})
			size := len(filter.KeepNames())
			deps := c.collectDeps(consumer)
			if got := slices.ContainsFunc(deps, func(dep manifest.DependencyRef) bool {
				return dep.Use == "dependsOn" && dep.Name == consumer.DependsOn[0].Name
			}); got != tc.retain {
				t.Errorf("ordering edge retained = %v, want %v; deps = %v", got, tc.retain, deps)
			}
			for _, id := range []manifest.NamedResource{
				{Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}, parent,
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"},
			} {
				if !slices.ContainsFunc(deps, func(d manifest.DependencyRef) bool { return d.NamedResource == id }) {
					t.Errorf("content/parent dependency %s was pruned", id)
				}
			}
			if len(filter.KeepNames()) != size || len(consumer.DependsOn) != 1 {
				t.Error("ordering collection must preserve keep and input edges")
			}
		})
	}
}
