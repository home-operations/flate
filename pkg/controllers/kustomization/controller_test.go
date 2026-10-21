package kustomization

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type indexedOrderingDeps map[manifest.NamedResource]bool

func (i indexedOrderingDeps) IsFileIndexed(id manifest.NamedResource) bool { return i[id] }
func (indexedOrderingDeps) Promote(manifest.NamedResource) bool            { return false }

func TestCollectDeps_OrderingFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                      string
		active, selected, indexed bool
		present                   bool
		status                    store.Status
		retain                    bool
		noData                    bool
	}{
		{name: "unfiltered missing", retain: true},
		{name: "no data", retain: true, noData: true},
		{name: "known file excluded", active: true, indexed: true},
		{name: "render generated excluded", active: true, present: true},
		{name: "unknown retained", active: true, retain: true},
		{name: "selected ready", active: true, selected: true, present: true, status: store.StatusReady, retain: true},
		{name: "selected failed", active: true, selected: true, present: true, status: store.StatusFailed, retain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			target := &manifest.Kustomization{Name: "target", Namespace: "apps", Path: "./target"}
			if tc.present {
				st.AddObject(target)
				st.UpdateStatus(target.Named(), tc.status, "", "fixture")
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
				DependsOn: []kustomizev1.DependencyReference{{Name: target.Name, Namespace: target.Namespace, ReadyExpr: "dep.spec.suspend == false"}, {Name: target.Name, Namespace: target.Namespace, ReadyExpr: "true"}},
				SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source", Namespace: "apps"},
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}, {Kind: manifest.KindConfigMap, Name: "ignored"}, {Kind: "Unsupported", Name: "unsupported"}, {Kind: manifest.KindConfigMap, Name: "self"}, {Kind: manifest.KindConfigMap, Name: "values"}}},
			}
			if tc.noData {
				consumer.PostBuild = nil
			}
			filter := change.NewFilter(changes, &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {File: "consumer.yaml"}, target.Named(): {File: "target.yaml"}}}, st)
			parent := testutil.KustomizationID("apps", "parent")
			producers := []manifest.NamedResource{testutil.KustomizationID("apps", "second"), testutil.KustomizationID("apps", "first")}
			slices.SortFunc(producers, manifest.NamedResource.Compare)
			c := New(st, nil, nil, false)
			c.Configure(Options{Index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer.Named(): {Productions: []manifest.Production{{Owner: parent}}}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"}: {Productions: testutil.ProductionEvidence(producers)}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "self"}: {Productions: testutil.ProductionEvidence([]manifest.NamedResource{consumer.Named()})}}}, Filter: filter, Existence: indexedOrderingDeps{target.Named(): tc.indexed}, SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "ignored"}: &manifest.ConfigMap{}}}, sourcecache.RetryConfig{})
			size := len(filter.KeepNames())
			deps := c.collectDeps(consumer)
			var want []manifest.DependencyRef
			if tc.retain {
				want = append(want, manifest.DependencyRef{NamedResource: target.Named(), ReadyExpr: "dep.spec.suspend == false"}, manifest.DependencyRef{NamedResource: target.Named(), ReadyExpr: "true"})
			}
			want = append(want, manifest.DependencyRef{NamedResource: parent}, manifest.DependencyRef{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"})
			if !tc.noData {
				want = append(want, manifest.DependencyRef{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"})
				for _, producer := range producers {
					want = append(want, manifest.DependencyRef{NamedResource: producer})
				}
			}
			assert.Diff(t, deps, want)
			if len(filter.KeepNames()) != size || len(consumer.DependsOn) != 2 {
				t.Error("ordering collection must preserve keep and input edges")
			}
		})
	}
}
