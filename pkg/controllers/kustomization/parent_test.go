package kustomization

import (
	"errors"
	"github.com/home-operations/flate/pkg/depwait"
	"slices"
	"strings"
	"testing"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCollectDeps_AppendsStructuralParent(t *testing.T) {
	t.Parallel()

	parent := testutil.KustomizationID("flux-system", "cluster-apps")
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	for _, name := range []string{"predicted", "provisional seed", "file evidence", "self declared", "accepted supporter", "self declared with accepted supporter"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			index := parentIndexForTest(map[manifest.NamedResource]manifest.NamedResource{child.Named(): parent})
			if name != "predicted" {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Source: manifest.BootstrapSourceID}}
				index.Records[child.Named()] = r
			}
			if name == "file evidence" {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Owner: manifest.NamedResource{Kind: manifest.KindKustomization, Name: "file-owner"}, Path: "child.yaml"}}
				index.Records[child.Named()] = r
			}
			if strings.HasPrefix(name, "self declared") {
				r := index.Records[child.Named()]
				r.Productions = []manifest.Production{{Owner: child.Named(), Source: manifest.BootstrapSourceID}}
				index.Records[child.Named()] = r
			}
			st := store.New()
			want := parent
			if strings.Contains(name, "accepted supporter") {
				st.AddObject(child)
				st.SetDiscoveryIndex(index)
				other := &manifest.Kustomization{Name: "other", Namespace: child.Namespace}
				st.AddObject(other)
				st.RecordEmitted(other.Named(), &store.Emission{Children: []store.Child{{Obj: child}}, Complete: true})
				want = other.Named()
			}
			c := New(st, nil, nil, false)
			c.Configure(Options{Index: index}, sourcecache.RetryConfig{})
			deps := c.collectDeps(child)
			var count int
			for _, d := range deps {
				if d.NamedResource == want {
					count++
				}
			}
			if count != 1 || len(deps) != 1 {
				t.Fatalf("dependency %s occurrences=%d in deps %v", want, count, deps)
			}
		})
	}
}

func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	t.Parallel()
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil, nil, false)
	deps := c.collectDeps(child)
	if len(deps) != 0 {
		t.Errorf("expected no deps for KS without sourceRef/dependsOn/parent; got %+v", deps)
	}
}

func TestResolve_SubstitutionProducers(t *testing.T) {
	t.Parallel()
	consumer := testutil.KustomizationID("apps", "consumer")
	producer := testutil.KustomizationID("apps", "producer")
	cm := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	for _, tc := range []struct {
		name                    string
		ref                     manifest.SubstituteReference
		producers, owners, want []manifest.NamedResource
	}{
		{name: "required ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "Owners-only ConfigMap in full mode", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, owners: []manifest.NamedResource{producer}, want: []manifest.NamedResource{cm, producer}},
		{name: "optional ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "required Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}, producers: []manifest.NamedResource{producer}, want: []manifest.NamedResource{producer}},
		{name: "optional Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings", Optional: true}, producers: []manifest.NamedResource{producer}},
		{name: "unknown Secret", ref: manifest.SubstituteReference{Kind: manifest.KindSecret, Name: "settings"}},
		{name: "missing ConfigMap", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, want: []manifest.NamedResource{cm}},
		{name: "self producer", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap, Name: "settings"}, producers: []manifest.NamedResource{consumer}},
		{name: "empty name", ref: manifest.SubstituteReference{Kind: manifest.KindConfigMap}, producers: []manifest.NamedResource{producer}},
		{name: "unknown kind", ref: manifest.SubstituteReference{Kind: "Junk", Name: "settings"}, producers: []manifest.NamedResource{producer}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := manifest.NamedResource{Kind: tc.ref.Kind, Namespace: consumer.Namespace, Name: tc.ref.Name}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: testutil.ProductionEvidence(append(slices.Clone(tc.producers), tc.owners...))}}}
			c := New(store.New(), nil, nil, false)
			c.Configure(Options{Index: index}, sourcecache.RetryConfig{})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{tc.ref}}}

			var resolved depwait.Resolution
			err := c.Require(&depwait.Waiter{Parent: ks.Named(), Declaration: ks}, manifest.ReconcileAttempt{}, &resolved)
			blocked, waiting := errors.AsType[*depwait.ErrBlocked](err)
			var want []manifest.NamedResource
			for _, supplier := range append(slices.Clone(tc.producers), tc.owners...) {
				if supplier != consumer && tc.ref.Name != "" && (tc.ref.Kind == manifest.KindConfigMap || tc.ref.Kind == manifest.KindSecret) {
					want = append(want, supplier)
				}
			}
			if len(want) > 0 {
				if !waiting {
					t.Fatalf("supplier was not bound: %v", err)
				}
				var got []manifest.NamedResource
				for _, supplier := range blocked.Content.Suppliers {
					got = append(got, supplier.NamedResource)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("suppliers=%v want=%v", got, want)
				}
			} else if tc.ref.Kind == manifest.KindConfigMap && tc.ref.Name != "" && !tc.ref.Optional && !slices.Contains(tc.producers, consumer) {
				if !waiting || !slices.Contains(blocked.Deps, cm) {
					t.Fatalf("required input did not wait: %+v %v", resolved, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}

		})
	}
}

func TestCollectDeps_AppendsSubstituteFromConfigMapAndProducer(t *testing.T) {
	t.Parallel()
	consumerObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		}},
	}
	producerObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
	}
	consumer := consumerObj.Named()
	producer := producerObj.Named()
	// discovery-indexed CM (Namespace="") — kustomize's namespace
	// directive hasn't run yet at file-walk time.
	cmObj := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer: {File: "kubernetes/flux/cluster-apps.yaml"}, producer: {File: "kubernetes/flux/cluster-vars.yaml"}, cmID: {File: "kubernetes/flux/vars/cluster-settings.yaml", Productions: []manifest.Production{{Owner: producer, Authored: cmID, Path: "kubernetes/flux/vars/cluster-settings.yaml", File: "kubernetes/flux/vars/cluster-settings.yaml"}}}}, Claims: map[string][]manifest.NamedResource{"kubernetes/apps/": {consumer}, "kubernetes/flux/vars/": {producer}}}
	f := change.NewFilter(change.NewSet([]string{"kubernetes/apps/changed.yaml"}), index, controllertest.MapLister{consumer: consumerObj, producer: producerObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index}, sourcecache.RetryConfig{})

	var resolved depwait.Resolution
	err := c.Require(&depwait.Waiter{Parent: consumer, Declaration: consumerObj}, manifest.ReconcileAttempt{}, &resolved)
	blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
	wantCM := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if !ok || blocked.Content == nil || !slices.Contains(blocked.Content.Targets, wantCM) || len(blocked.Content.Suppliers) != 1 || blocked.Content.Suppliers[0].NamedResource != producer {
		t.Fatalf("changed-only admission=%+v %v", resolved, err)
	}

}

func TestCollectDeps_SkipsSelfProducer(t *testing.T) {
	t.Parallel()
	selfObj := &manifest.Kustomization{
		Name: "app", Namespace: "foo",
		Path: "kubernetes/foo/app",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "settings"},
		}},
	}
	self := selfObj.Named()
	cmObj := &manifest.ConfigMap{Name: "settings", Namespace: self.Namespace}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{self: {File: "kubernetes/foo/app/ks.yaml"}, cmID: {File: "kubernetes/foo/app/settings.yaml", Productions: testutil.ProductionEvidence([]manifest.NamedResource{self})}}, Claims: map[string][]manifest.NamedResource{"kubernetes/foo/app/": {self}}}
	f := change.NewFilter(change.NewSet([]string{"kubernetes/foo/app/changed.yaml"}), index, controllertest.MapLister{self: selfObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index}, sourcecache.RetryConfig{})
	if owners := c.Store.Productions(cmID, index); len(owners) != 1 || owners[0].Owner != self {
		t.Fatalf("missing self evidence: %v", owners)
	}

	err := c.Require(&depwait.Waiter{Parent: self, Declaration: selfObj}, manifest.ReconcileAttempt{}, &depwait.Resolution{})
	if err != nil {
		t.Fatalf("self input waited: %v", err)
	}
	selfObj = selfObj.Clone()
	selfObj.DependsOn = []kustomizev1.DependencyReference{{Name: self.Name}}
	err = c.Require(&depwait.Waiter{Parent: self, Declaration: selfObj, Dependencies: c.collectDeps(selfObj)}, manifest.ReconcileAttempt{Drain: manifest.DrainForce}, &depwait.Resolution{})
	if err == nil {
		t.Fatal("explicit self dependsOn was erased by content self exclusion")
	}

}

func TestCollectDeps_RuntimeDeclarationDependency(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, nil, true)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": "credentials", "template": map[string]any{"data": map[string]any{"X": "remote"}}}}}
	c.Emit(producer.Named(), &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true})
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "credentials"}}}}

	err := c.Require(&depwait.Waiter{Parent: consumer.Named(), Declaration: consumer, WipeSecrets: true}, manifest.ReconcileAttempt{}, &depwait.Resolution{})
	blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
	if !ok || blocked.Content == nil || len(blocked.Content.Suppliers) != 1 || blocked.Content.Suppliers[0].NamedResource != producer.Named() {
		t.Fatalf("declaration supplier wait=%v", err)
	}
	if slices.Contains(blocked.Deps, declaration.Named()) {
		t.Fatal("live-only declaration became a readiness dependency")
	}

}
