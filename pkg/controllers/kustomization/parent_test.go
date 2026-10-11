package kustomization

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestCollectDeps_AppendsStructuralParent locks the contract that
// when ParentOf records an enclosing KS, that parent is appended to
// the dependency list so the reconcile waits for the parent's Ready
// before rendering. See #102.
func TestCollectDeps_AppendsStructuralParent(t *testing.T) {
	parent := manifest.NamedResource{
		Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps",
	}
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{ParentOf: mapResolver(map[manifest.NamedResource]manifest.NamedResource{
		child.Named(): parent,
	})})
	deps := c.collectDeps(child)
	for _, d := range deps {
		if d.NamedResource == parent {
			return
		}
	}
	t.Errorf("parent %+v missing from deps %+v", parent, deps)
}

// TestCollectDeps_NoParentNoExtraDep guards against ParentOf-less
// controllers (e.g. unit-test setups) panicking on a nil resolver.
func TestCollectDeps_NoParentNoExtraDep(t *testing.T) {
	child := &manifest.Kustomization{Name: "karma", Namespace: "observability"}
	c := New(store.New(), nil, nil, false) // ParentOf nil
	deps := c.collectDeps(child)
	if len(deps) != 0 {
		t.Errorf("expected no deps for KS without sourceRef/dependsOn/parent; got %+v", deps)
	}
}

func TestCollectDeps_SubstitutionProducers(t *testing.T) {
	consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
	producer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "producer"}
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
			id := manifest.NamedResource{Kind: tc.ref.Kind, Namespace: consumer.Namespace, Name: tc.ref.Name}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Renderers: tc.producers, Owners: tc.owners}}}
			c := New(store.New(), nil, nil, false)
			c.Configure(Options{Index: index})
			ks := &manifest.Kustomization{Name: consumer.Name, Namespace: consumer.Namespace, PostBuildSubstituteFrom: []manifest.SubstituteReference{tc.ref}}
			var got []manifest.NamedResource
			for _, dep := range c.collectDeps(ks) {
				got = append(got, dep.NamedResource)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("dependencies = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCollectDeps_AppendsSubstituteFromConfigMapAndProducer pins the
// changed-only producer-KS depwait edge: when the substituteFrom
// ConfigMap is rendered by another Flux Kustomization, that producer
// is appended as its own dep so the consumer waits for the producer's
// reconcile (which materializes the CM) before rendering. See #418.
func TestCollectDeps_AppendsSubstituteFromConfigMapAndProducer(t *testing.T) {
	consumerObj := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		Path: "kubernetes/apps",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "cluster-settings"},
		},
	}
	producerObj := &manifest.Kustomization{
		Name: "cluster-vars", Namespace: "flux-system",
		Path: "kubernetes/flux/vars",
	}
	consumer := consumerObj.Named()
	producer := producerObj.Named()
	// DiscoveryOnly-indexed CM (Namespace="") — kustomize's namespace
	// directive hasn't run yet at file-walk time.
	cmObj := &manifest.ConfigMap{Name: "cluster-settings", Namespace: ""}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{consumer: {File: "kubernetes/flux/cluster-apps.yaml"}, producer: {File: "kubernetes/flux/cluster-vars.yaml"}, cmID: {File: "kubernetes/flux/vars/cluster-settings.yaml", Owners: []manifest.NamedResource{producer}}}, Claims: map[string][]manifest.NamedResource{"kubernetes/apps/": {consumer}, "kubernetes/flux/vars/": {producer}}}
	f := change.NewFilter(change.NewSet([]string{"kubernetes/apps/changed.yaml"}), index, testutil.MapLister{consumer: consumerObj, producer: producerObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index})
	deps := c.collectDeps(consumerObj)

	wantCM := manifest.NamedResource{
		Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings",
	}
	var sawCM, sawProducer bool
	for _, d := range deps {
		if d.NamedResource == wantCM {
			sawCM = true
		}
		if d.NamedResource == producer {
			sawProducer = true
		}
	}
	if !sawCM {
		t.Errorf("expected substituteFrom CM dep %+v in deps %+v", wantCM, deps)
	}
	if !sawProducer {
		t.Errorf("expected producer KS dep %+v in deps %+v", producer, deps)
	}
}

// TestCollectDeps_SkipsSelfProducer pins the bjw-s self-substitute
// pattern: a KS that produces its own substituteFrom ConfigMap must
// NOT be appended as a dep on itself (would self-loop in depwait).
// See #418.
func TestCollectDeps_SkipsSelfProducer(t *testing.T) {
	selfObj := &manifest.Kustomization{
		Name: "app", Namespace: "foo",
		Path: "kubernetes/foo/app",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "settings"},
		},
	}
	self := selfObj.Named()
	// CM rendered by self — file lives under self's spec.path so
	// ownersOf returns self in the producer index.
	cmObj := &manifest.ConfigMap{Name: "settings", Namespace: ""}
	cmID := cmObj.Named()

	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{self: {File: "kubernetes/foo/app/ks.yaml"}, cmID: {File: "kubernetes/foo/app/settings.yaml", Owners: []manifest.NamedResource{self}}}, Claims: map[string][]manifest.NamedResource{"kubernetes/foo/app/": {self}}}
	f := change.NewFilter(change.NewSet([]string{"kubernetes/foo/app/changed.yaml"}), index, testutil.MapLister{self: selfObj, cmID: cmObj})

	c := New(store.New(), nil, nil, false)
	c.Configure(Options{Filter: f, Index: index})
	deps := c.collectDeps(selfObj)

	for _, d := range deps {
		if d.NamedResource == self {
			t.Errorf("self-producer must not appear as its own dep; got %+v", deps)
		}
	}
}

// mapResolver wraps a static parent map into the func resolver shape
// the controllers consume. Used by tests that want to verify
// behavior on a known parent index without standing up the full
// orchestrator wiring (which combines this with the Store render ownership).
func mapResolver(m map[manifest.NamedResource]manifest.NamedResource) func(manifest.NamedResource) (manifest.NamedResource, bool) {
	return func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		parent, ok := m[id]
		return parent, ok
	}
}
