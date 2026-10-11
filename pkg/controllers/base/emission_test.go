package base

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// TestTrackedChild_KnownKinds documents which kinds the
// render-emission helper routes through AddObject vs. AddRendered.
// Pinning this matrix prevents a future contributor from accidentally
// widening or narrowing the set — a regression here would either
// re-introduce the two-phase emission race (by excluding a data kind)
// or trigger spurious reconciles on non-reconcilable kinds.
func TestTrackedChild_KnownKinds(t *testing.T) {
	cases := []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		{"Kustomization", &manifest.Kustomization{}, true},
		{"HelmRelease", &manifest.HelmRelease{}, true},
		{"HelmRepository", &manifest.HelmRepository{}, true},
		{"OCIRepository", &manifest.OCIRepository{}, true},
		{"GitRepository", &manifest.GitRepository{}, true},
		{"Bucket", &manifest.Bucket{}, true},
		{"HelmChartSource", &manifest.HelmChartSource{}, true},
		{"ExternalArtifact", &manifest.ExternalArtifact{}, true},
		{"ConfigMap", &manifest.ConfigMap{}, true},
		{"Secret", &manifest.Secret{}, true},
		{"ResourceSet", &manifest.ResourceSet{}, true},
		{"ResourceSetInputProvider", &manifest.ResourceSetInputProvider{}, true},
		{"RawObject falls through", &manifest.RawObject{Kind: "Service"}, false},
		{"raw Kustomization", &manifest.RawObject{Kind: manifest.KindKustomization}, false},
		{"raw HelmRelease", &manifest.RawObject{Kind: manifest.KindHelmRelease}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTrackedChild(tc.obj); got != tc.want {
				t.Errorf("isTrackedChild(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestEmit_DefaultsNamespace pins the fix for the unsubstituted-OCIRepository
// bug: a namespace-less rendered child must inherit the emitting parent's
// namespace, so it dedups against its namespace-inherited file-loaded copy
// instead of lingering at empty namespace. An explicit namespace is preserved.
func TestEmit_DefaultsNamespace(t *testing.T) {
	s := store.New()
	c := New(s, task.NewBounded(1), "test")
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "infra"}

	c.Emit(parent, []map[string]any{
		{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "OCIRepository",
			"metadata":   map[string]any{"name": "certmanager"},
			"spec":       map[string]any{"url": "oci://quay.io/c", "provider": "generic"},
		},
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cm", "namespace": "other"},
		},
	}, true, true)

	if s.GetObject(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "certmanager"}) == nil {
		t.Error("namespace-less OCIRepository was not defaulted to the parent namespace flux-system")
	}
	if obj := s.GetObject(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "", Name: "certmanager"}); obj != nil {
		t.Errorf("empty-namespace OCIRepository phantom present: %v", obj.Named())
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "other", Name: "cm"}) == nil {
		t.Error("explicit namespace not preserved for ConfigMap other/cm")
	}
}

func TestEmit_OwnershipPrecedesArrivals(t *testing.T) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindResourceSet} {
		for _, publish := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/publish=%t", kind, publish), func(t *testing.T) {
				s := store.New()
				c := New(s, nil, "test")
				parent := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "parent"}
				rs := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "rs"}
				consumer := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "child"}
				source := &manifest.OCIRepository{Name: "shared", Namespace: "apps", URL: "oci://example.test/chart"}
				s.AddObject(source)
				s.UpdateStatus(source.Named(), store.StatusReady, "unchanged")
				c.SetFilter(change.NewFilter(change.NewSet([]string{"parent.yaml"}), &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
				}, s))
				var arrivals, refires, consumers, sets int
				s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					arrivals++
					for _, child := range []manifest.NamedResource{rs, consumer, source.Named()} {
						if got, ok := s.EmittedParent(child); !ok || got != parent {
							t.Errorf("owner at arrival of %s = (%v, %v), want %s", id, got, ok, parent)
						}
					}
					if id == rs {
						sets++
					}
					if id == source.Named() {
						refires++
					}
					if id == consumer {
						consumers++
						info, _ := s.GetStatus(source.Named())
						if info.Status != store.StatusPending || !c.Filter().ShouldReconcile(consumer) {
							t.Errorf("consumer arrived before prerequisite was Pending and consumer was kept: %+v", info)
						}
					}
				}, false)
				c.Emit(parent, []map[string]any{
					{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "raw"}},
					{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": map[string]any{"name": "rs"}, "spec": map[string]any{}},
					{"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease", "metadata": map[string]any{"name": "child"}, "spec": map[string]any{"chartRef": map[string]any{"kind": "OCIRepository", "name": "shared"}}},
					{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "OCIRepository", "metadata": map[string]any{"name": "shared"}, "spec": map[string]any{"url": source.URL}},
				}, false, publish)
				if arrivals == 0 || refires == 0 || (consumers == 1) != publish || (sets == 1) != publish {
					t.Fatalf("arrivals=%d refires=%d consumers=%d sets=%d publish=%t", arrivals, refires, consumers, sets, publish)
				}
			})
		}
	}
}

// The file-loaded copy of the emitted consumer is already in the store, as after discovery.
func TestKeepEmitted_RefiresSourcesAndKustomizationsOnly(t *testing.T) {
	s := store.New()
	c := New(s, nil, "test")
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps",
		PostBuildSubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}}}
	release := &manifest.HelmRelease{Name: "release", Namespace: "apps",
		Chart: manifest.HelmChart{RepoKind: manifest.KindHelmChart, RepoNamespace: "apps", RepoName: "chart"}}
	for _, obj := range []manifest.BaseManifest{producer, chart, consumer, release} {
		s.AddObject(obj)
		s.UpdateStatus(obj.Named(), store.StatusReady, "unchanged")
	}
	c.SetFilter(change.NewFilter(change.NewSet([]string{"parent.yaml"}), &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			parent:   {File: "parent.yaml"},
			settings: {Owners: []manifest.NamedResource{producer.Named()}},
		},
	}, s))

	c.KeepEmitted(parent, consumer)
	c.KeepEmitted(parent, release)

	for _, tc := range []struct {
		obj  manifest.BaseManifest
		want store.Status
	}{{producer, store.StatusPending}, {chart, store.StatusReady}} {
		if info, _ := s.GetStatus(tc.obj.Named()); info.Status != tc.want {
			t.Errorf("%s status = %s, want %s", tc.obj.Named(), info.Status, tc.want)
		}
	}
}

// A ResourceSet re-emitting itself must neither overwrite its stored spec nor own itself.
func TestEmit_SelfCopyIsNotPublished(t *testing.T) {
	s := store.New()
	c := New(s, nil, "test")
	parent := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "parent"}

	c.Emit(parent, []map[string]any{
		{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": map[string]any{"name": "parent"}, "spec": map[string]any{}},
	}, false, true)

	if s.GetObject(parent) != nil {
		t.Error("self-copy was published")
	}
	if owner, ok := s.EmittedParent(parent); ok {
		t.Errorf("self-copy has owner %v", owner)
	}
}
