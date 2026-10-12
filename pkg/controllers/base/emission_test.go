package base

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestTrackedChild_EmitterKinds(t *testing.T) {
	cases := []struct {
		name   string
		obj    manifest.BaseManifest
		want   bool
		wantHR bool
	}{
		{"Kustomization", &manifest.Kustomization{}, true, false},
		{"HelmRelease", &manifest.HelmRelease{}, true, false},
		{"HelmRepository", &manifest.HelmRepository{}, true, true},
		{"OCIRepository", &manifest.OCIRepository{}, true, true},
		{"GitRepository", &manifest.GitRepository{}, true, true},
		{"Bucket", &manifest.Bucket{}, true, true},
		{"HelmChartSource", &manifest.HelmChartSource{}, true, true},
		{"ExternalArtifact", &manifest.ExternalArtifact{}, true, true},
		{"ConfigMap", &manifest.ConfigMap{}, true, false},
		{"Secret", &manifest.Secret{}, true, false},
		{"ResourceSet", &manifest.ResourceSet{}, true, false},
		{"ResourceSetInputProvider", &manifest.ResourceSetInputProvider{}, true, false},
		{"RawObject falls through", &manifest.RawObject{Kind: "Service"}, false, false},
		{"raw Kustomization", &manifest.RawObject{Kind: manifest.KindKustomization}, false, false},
		{"raw HelmRelease", &manifest.RawObject{Kind: manifest.KindHelmRelease}, false, false},
	}
	for _, tc := range cases {
		for _, helmRelease := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/helmRelease=%t", tc.name, helmRelease), func(t *testing.T) {
				want := tc.want
				if helmRelease {
					want = tc.wantHR
				}
				if got := isTrackedChild(tc.obj, helmRelease); got != want {
					t.Errorf("isTrackedChild(%T, %t) = %v, want %v", tc.obj, helmRelease, got, want)
				}
			})
		}
	}
}

// HelmRelease documents already carry the release namespace, which may differ from the emitter namespace.
func TestEmit_DefaultsNamespace(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want string
	}{
		{manifest.KindKustomization, "flux-system"},
		{manifest.KindResourceSet, "flux-system"},
		{manifest.KindHelmRelease, ""},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			s := store.New()
			c := New(s, nil, "test")
			parent := manifest.NamedResource{Kind: tc.kind, Namespace: "flux-system", Name: "infra"}
			children, _ := c.Emit(parent, []map[string]any{
				{
					"apiVersion": "source.toolkit.fluxcd.io/v1",
					"kind":       "OCIRepository",
					"metadata":   map[string]any{"name": "certmanager"},
					"spec":       map[string]any{"url": "oci://quay.io/c", "provider": "generic"},
				},
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm", "namespace": "release"},
				},
			}, true, true)
			if len(children) != 2 {
				t.Fatalf("children = %d, want 2", len(children))
			}
			for i, want := range []string{tc.want, "release"} {
				if got := children[i].Obj.Named().Namespace; got != want {
					t.Errorf("child %d namespace = %q, want %q", i, got, want)
				}
				if s.GetObject(children[i].Obj.Named()) == nil {
					t.Errorf("child %d was not published", i)
				}
			}
		})
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
						if info.Status != store.StatusPending || !c.filter.ShouldReconcile(consumer) {
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

func TestEmit_RetainsSanitizedInvalidDocuments(t *testing.T) {
	c := New(store.New(), nil, "test")
	docs := []map[string]any{
		{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "valid"}, "stringData": map[string]any{"VALUE": "embedded ENC[AES256_GCM,data:fixture]"}},
		{"apiVersion": "v1", "kind": "Secret", "stringData": map[string]any{"VALUE": "embedded ENC[AES256_GCM,data:fixture]"}},
	}
	parent := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "parent"}
	children, retained := c.Emit(parent, docs, true, true)
	if len(children) != 1 || len(retained) != 2 {
		t.Fatalf("children=%d retained=%d", len(children), len(retained))
	}
	for i := range docs {
		if retained[i]["stringData"].(map[string]any)["VALUE"] != "..PLACEHOLDER_VALUE.." || docs[i]["stringData"].(map[string]any)["VALUE"] == "..PLACEHOLDER_VALUE.." {
			t.Fatal("ciphertext retained or borrowed document mutated")
		}
	}
	_, replay := c.Emit(parent, retained, true, false)
	if &replay[0] != &retained[0] {
		t.Fatal("idempotent replay cloned the retained slice")
	}
}
