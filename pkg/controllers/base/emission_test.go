package base

import (
	"fmt"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEmit_DefaultsNamespace(t *testing.T) {
	for _, kind := range []string{manifest.KindKustomization, manifest.KindResourceSet, manifest.KindHelmRelease} {
		for _, fallback := range []string{"", "apps"} {
			t.Run(kind+"/default="+fallback, func(t *testing.T) {
				s := store.New()
				c := New(s, nil, "test")
				parent := manifest.NamedResource{Kind: kind, Namespace: "parent", Name: "infra"}
				docs := []map[string]any{
					{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "OCIRepository", "metadata": map[string]any{"name": "chart"}, "spec": map[string]any{"url": "oci://example.test/chart"}},
					{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm", "namespace": "release"}},
					{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "app"}},
					{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "role"}},
					{"apiVersion": "foreign.example/v1", "kind": "ClusterRole", "metadata": map[string]any{"name": "foreign"}},
				}
				before := make([]map[string]any, len(docs))
				for i, doc := range docs {
					before[i] = manifest.DeepCopyMap(doc)
				}
				opts := manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: fallback}
				children, retained := c.Emit(parent, docs, opts, true)
				assert.Equal(t, len(children), len(docs))
				for i, want := range []string{fallback, "release", fallback, "", fallback} {
					assert.Equal(t, children[i].Obj.Named().Namespace, want)
					if s.GetObject(children[i].Obj.Named()) == nil {
						t.Errorf("child %d was not published", i)
					}
				}
				assert.Diff(t, docs, before)
				if fallback != "" {
					for _, i := range []int{0, 2, 4} {
						before[i]["metadata"].(map[string]any)["namespace"] = fallback
					}
				}
				assert.Diff(t, retained, before)
				replayed, replay := c.Emit(parent, retained, opts, false)
				for i := range children {
					assert.Equal(t, replayed[i].Obj.Named(), children[i].Obj.Named())
				}
				assert.Diff(t, replay, before)
			})
		}
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
				source := &manifest.Bucket{Name: "shared", Namespace: "apps"}
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
						if s.GetObject(consumer) != nil {
							t.Error("render leaf admitted before ResourceSet publication")
						}
						info, _ := s.GetStatus(source.Named())
						if info.Status != store.StatusPending || !c.filter.ShouldReconcile(source.Named()) {
							t.Errorf("ResourceSet arrived before source publication: %+v", info)
						}
						sets++
						bucket, _ := s.Get[*manifest.Bucket](source.Named())
						updated := *bucket
						updated.Endpoint = "updated.invalid"
						s.AddObject(&updated)
					}
					if id == source.Named() {
						refires++
					}
					if id == consumer {
						if sets != 1 {
							t.Error("render leaf arrived before ResourceSet publication")
						}
						consumers++
						info, _ := s.GetStatus(source.Named())
						if info.Status != store.StatusPending || !c.filter.ShouldReconcile(consumer) {
							t.Errorf("consumer arrived before prerequisite was Pending and consumer was kept: %+v", info)
						}
					}
				}, false)
				c.Emit(parent, []map[string]any{
					{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "raw"}},
					{"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease", "metadata": map[string]any{"name": "child"}, "spec": map[string]any{"chartRef": map[string]any{"kind": "OCIRepository", "name": "shared"}}},
					{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "Bucket", "metadata": map[string]any{"name": "shared"}, "spec": map[string]any{"endpoint": "minio.invalid", "bucketName": "shared"}},
					{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": map[string]any{"name": "rs"}, "spec": map[string]any{}},
				}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Namespace}, publish)
				if arrivals == 0 || refires > 3 || (refires == 3) != publish || (consumers == 1) != publish || (sets == 1) != publish {
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
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}}}}
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
	for _, tc := range []struct {
		obj    manifest.BaseManifest
		refire bool
	}{
		{&manifest.GitRepository{Name: "child"}, true},
		{&manifest.OCIRepository{Name: "child"}, true},
		{&manifest.HelmRepository{Name: "child"}, true},
		{&manifest.Bucket{Name: "child"}, true},
		{&manifest.ExternalArtifact{Name: "child"}, true},
		{&manifest.Kustomization{Name: "child"}, true},
		{&manifest.HelmChartSource{Name: "child"}, false},
		{&manifest.HelmRelease{Name: "child"}, false},
		{&manifest.ResourceSet{Name: "child"}, false},
		{&manifest.ResourceSetInputProvider{Name: "child"}, false},
		{&manifest.ConfigMap{Name: "child"}, false},
		{&manifest.Secret{Name: "child"}, false},
	} {
		t.Run(tc.obj.Named().Kind, func(t *testing.T) {
			s := store.New()
			c := New(s, nil, "test")
			obj := tc.obj.WithNamespace("apps")
			s.AddObject(obj)
			s.UpdateStatus(obj.Named(), store.StatusReady, "unchanged")
			c.SetFilter(change.NewFilter(change.NewSet([]string{"parent.yaml"}), &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
			}, s))
			c.KeepEmitted(parent, obj)
			want := store.StatusReady
			if tc.refire {
				want = store.StatusPending
			}
			info, _ := s.GetStatus(obj.Named())
			assert.Equal(t, info.Status, want)
		})
	}
}

// A ResourceSet re-emitting itself must neither overwrite its stored spec nor own itself.
func TestEmit_SelfCopyIsNotPublished(t *testing.T) {
	s := store.New()
	c := New(s, nil, "test")
	parent := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "parent"}

	c.Emit(parent, []map[string]any{
		{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": map[string]any{"name": "parent"}, "spec": map[string]any{}},
	}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Namespace}, true)

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
	children, retained := c.Emit(parent, docs, manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: "effective"}, true)
	if len(children) != 1 || len(retained) != 2 {
		t.Fatalf("children=%d retained=%d", len(children), len(retained))
	}
	assert.Equal(t, children[0].Obj.Named().Namespace, "effective")
	for i := range docs {
		_, ns := manifest.DocMetadata(retained[i])
		want := ""
		if i == 0 {
			want = "effective"
		}
		assert.Equal(t, ns, want)
		if retained[i]["stringData"].(map[string]any)["VALUE"] != "..PLACEHOLDER_VALUE.." || docs[i]["stringData"].(map[string]any)["VALUE"] == "..PLACEHOLDER_VALUE.." {
			t.Fatal("ciphertext retained or borrowed document mutated")
		}
	}
	_, replay := c.Emit(parent, retained, manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: "effective"}, false)
	if &replay[0] != &retained[0] {
		t.Fatal("idempotent replay cloned the retained slice")
	}
	before := make([]map[string]any, len(retained))
	for i, doc := range retained {
		before[i] = manifest.DeepCopyMap(doc)
	}
	next, err := manifest.DecodeDocs(strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\n"))
	assert.NoError(t, err)
	obj, _, _, err := manifest.ParseDoc(next[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	assert.NoError(t, err)
	manifest.ReleaseIfNotRetained(next[0], obj)
	assert.Diff(t, retained, before)
}
