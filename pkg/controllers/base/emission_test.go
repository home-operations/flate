package base

import (
	"fmt"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize" // Standalone controller tests MUST install the process scope owner before parsing.
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestEmit_RetainsSanitizedInvalidDocuments(t *testing.T) {
	t.Parallel()

	for _, wipe := range []bool{false, true} {
		t.Run(fmt.Sprint(wipe), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			c := New(s, nil, "test")
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			s.AddObject(parent)
			doc := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "credentials"}, "stringData": map[string]any{"X": "ENC[AES256_GCM,data:encrypted]"}}
			invalid := map[string]any{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "invalid"}, "spec": "invalid"}
			children, docs := ParseChildren(parent.Named(), []map[string]any{doc, invalid}, manifest.ParseDocOptions{WipeSecrets: wipe, DefaultNamespace: "apps"})
			if len(children) != 1 || len(docs) != 2 {
				t.Fatalf("parse: children=%d docs=%d", len(children), len(docs))
			}
			if fmt.Sprintf("%p", children[0].Doc) != fmt.Sprintf("%p", docs[0]) {
				t.Fatal("child document does not share authoritative map")
			}
			next := &store.Emission{Children: children, Manifests: docs, Complete: true}
			c.Emit(parent.Named(), next)
			next.Children[0] = store.Child{}
			next.Manifests[0] = nil
			_, accepted := s.GetEmission(parent.Named())
			if len(accepted.Children) != 1 || accepted.Children[0].Obj == nil || accepted.Manifests[0] == nil {
				t.Fatal("caller slice headers mutate accepted inventory")
			}
		})
	}
}

func TestEmit_OwnershipPrecedesArrivals(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, "test")
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}}}
	filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s)
	c.Configure(Options{Filter: filter, Index: index})
	docs := []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings"}, "data": map[string]any{"VALUE": "value"}},
		{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "child"}, "spec": map[string]any{"path": "./app"}},
	}
	children, retained := ParseChildren(parent.Named(), docs, manifest.ParseDocOptions{DefaultNamespace: "apps"})
	arrivals := 0
	s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if payload == nil {
			return
		}
		arrivals++
		for _, child := range children {
			if s.GetObject(child.Obj.Named()) == nil {
				t.Error("sibling invisible during synchronous notification")
			}
			if !filter.ShouldReconcile(child.Obj.Named()) {
				t.Error("selection follows visibility")
			}
		}
		if len(s.Productions(id, index)) == 0 {
			t.Error("provenance follows visibility")
		}
	}, false)
	c.Emit(parent.Named(), &store.Emission{Children: children, Manifests: retained, Complete: true})
	if arrivals != 2 {
		t.Fatalf("arrivals=%d", arrivals)
	}
}

func TestEmit_SelfCopyIsNotPublished(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, "test")
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "effective"}
	s.AddObject(parent)
	docs := []map[string]any{{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "parent", "namespace": "apps"}, "spec": map[string]any{"path": "stale"}}}
	children, retained := ParseChildren(parent.Named(), docs, manifest.ParseDocOptions{})
	if len(children) != 0 {
		t.Fatal("self copy admitted")
	}
	c.Emit(parent.Named(), &store.Emission{Children: children, Manifests: retained, Complete: true})
	if s.GetObject(parent.Named()) != parent {
		t.Fatal("self copy overwrote effective input")
	}
}

func TestEmit_RuntimeProducerReplacement(t *testing.T) {
	t.Parallel()

	for _, wipe := range []bool{false, true} {
		t.Run(fmt.Sprint(wipe), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			c := New(s, nil, "test")
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			s.AddObject(parent)
			target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "credentials", Namespace: "apps"}
			var invalidations int
			s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if id == target {
					invalidations++
					if payload != nil {
						t.Error("declaration installed a data object")
					}
				}
			}, false)
			for _, key := range []string{"X", "Y"} {
				doc := map[string]any{"apiVersion": "external-secrets.io/v1", "kind": "ExternalSecret", "metadata": map[string]any{"name": "declaration", "namespace": "apps"}, "spec": map[string]any{"target": map[string]any{"name": target.Name, "template": map[string]any{"data": map[string]any{key: "remote"}}}}}
				children, docs := ParseChildren(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: wipe})
				c.Emit(parent.Named(), &store.Emission{Children: children, Manifests: docs, Complete: true})
				if s.GetObject(target) != nil {
					t.Fatal("runtime placeholder has a store writer")
				}
				data := values.NewStoreProvider(s, wipe).Data(target)
				if !wipe {
					if data != nil {
						t.Fatal("wiping disabled synthesized placeholder")
					}
					continue
				}
				value, ok, err := data.Value(key)
				if !ok || err != nil || value != manifest.ValuePlaceholder(key) {
					t.Fatalf("runtime key: %q %v %v", value, ok, err)
				}
				if key == "Y" {
					if _, ok, _ := data.Value("X"); ok {
						t.Fatal("old key retained")
					}
				}
			}
			c.Emit(parent.Named(), &store.Emission{Complete: true})
			if len(s.Productions(target, nil)) != 0 || values.NewStoreProvider(s, wipe).Data(target) != nil {
				t.Fatal("omitted declaration retained")
			}
			if invalidations != 3 {
				t.Fatalf("declaration target invalidations=%d want admission, key refresh and omission", invalidations)
			}

		})
	}
}

func TestEmit_RuntimeProducerTargets(t *testing.T) {
	t.Parallel()

	for _, wipe := range []bool{false, true} {
		for _, tc := range []struct {
			name, group, kind string
			targets           []manifest.NamedResource
		}{
			{"external", "external-secrets.io", "ExternalSecret", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "old"}}},
			{"sealed", "bitnami.com", "SealedSecret", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "old"}}},
			{"bucket", "objectbucket.io", "ObjectBucketClaim", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "declaration"}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "declaration"}}},
			{"foreign", "example.test", "ExternalSecret", nil}, {"group-less", "", "SealedSecret", nil},
		} {
			t.Run(fmt.Sprintf("%s/wipe=%t", tc.name, wipe), func(t *testing.T) {
				t.Parallel()

				s := store.New()
				c := New(s, nil, "test")
				root := &manifest.Kustomization{Name: "p", Namespace: "apps"}
				supporter := &manifest.Kustomization{Name: "q", Namespace: "apps"}
				s.AddObjects([]manifest.BaseManifest{root, supporter})
				apiVersion := "v1"
				if tc.group != "" {
					apiVersion = tc.group + "/v1"
				}
				spec := map[string]any{"target": map[string]any{"name": "old", "template": map[string]any{"data": map[string]any{"X": "remote"}}}, "template": map[string]any{"metadata": map[string]any{"name": "old"}}, "encryptedData": map[string]any{"X": "ciphertext"}}
				doc := map[string]any{"apiVersion": apiVersion, "kind": tc.kind, "metadata": map[string]any{"name": "declaration", "namespace": "apps"}, "spec": spec}
				publish := func(owner manifest.NamedResource, doc map[string]any) {
					t.Helper()
					children, docs := ParseChildren(owner, []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: wipe})
					c.Emit(owner, &store.Emission{Children: children, Manifests: docs, Complete: true})
				}
				publish(root.Named(), doc)
				for _, target := range tc.targets {
					if got := s.Productions(target, nil); len(got) != 1 || got[0].Declaration.Kind != tc.kind {
						t.Fatalf("runtime declaration evidence: %v", got)
					}
					data := values.NewStoreProvider(s, wipe).Data(target)
					known := wipe && tc.name != "bucket" && target.Kind == manifest.KindSecret
					if (data != nil) != known {
						t.Fatalf("known-key availability=%t want %t", data != nil, known)
					}
				}
				if len(tc.targets) == 0 {
					for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
						if got := s.Productions(manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "old"}, nil); len(got) != 0 {
							t.Fatal("lookalike produced a target")
						}
					}
				}
				if len(tc.targets) > 0 {
					independent := manifest.DeepCopyMap(doc)
					if tc.name != "bucket" {
						independent["metadata"].(map[string]any)["name"] = "independent"
					}
					publish(supporter.Named(), independent)
				}
				renamed := manifest.DeepCopyMap(doc)
				rspec := renamed["spec"].(map[string]any)
				rspec["target"].(map[string]any)["name"] = "next"
				rspec["target"].(map[string]any)["template"].(map[string]any)["data"] = map[string]any{"Y": "remote"}
				rspec["template"].(map[string]any)["metadata"].(map[string]any)["name"] = "next"
				rspec["encryptedData"] = map[string]any{"Y": "ciphertext"}
				if tc.name == "bucket" {
					renamed["metadata"].(map[string]any)["name"] = "next"
				}
				publish(root.Named(), renamed)
				for _, target := range tc.targets {
					if got := s.Productions(target, nil); len(got) != 1 || got[0].Owner != supporter.Named() {
						t.Fatalf("target rename changed independent contribution: %v", got)
					}
					target.Name = "next"
					if len(s.Productions(target, nil)) != 1 {
						t.Fatal("target rename lost new contribution")
					}
					data := values.NewStoreProvider(s, wipe).Data(target)
					if !wipe || tc.name == "bucket" {
						if data != nil {
							t.Fatal("unknown or wipe-disabled keys synthesized data")
						}
						continue
					}
					if data == nil {
						t.Fatal("renamed declaration lost known keys")
					}
					if value, ok, err := data.Value("Y"); value != "..PLACEHOLDER_Y.." || !ok || err != nil {
						t.Fatalf("replacement key: %q %t %v", value, ok, err)
					}
					if _, ok, _ := data.Value("X"); ok {
						t.Fatal("renamed declaration retained old key")
					}
				}
				c.Emit(root.Named(), &store.Emission{Complete: true})
				for _, target := range tc.targets {
					if got := s.Productions(target, nil); len(got) != 1 || got[0].Owner != supporter.Named() {
						t.Fatalf("omission withdrew independent support: %v", got)
					}
					target.Name = "next"
					if len(s.Productions(target, nil)) != 0 {
						t.Fatal("omission retained cluster-only target")
					}
				}
				c.Emit(supporter.Named(), &store.Emission{Complete: true})
				for _, target := range tc.targets {
					if len(s.Productions(target, nil)) != 0 || values.NewStoreProvider(s, wipe).Data(target) != nil {
						t.Fatal("last supporter withdrawal retained target data")
					}
				}
			})
		}
	}
}

func TestEmit_RuntimeProducerDataPrecedence(t *testing.T) {
	t.Parallel()

	for _, pending := range []string{"none", "renderer", "authored"} {
		for _, actual := range []bool{false, true} {
			t.Run(fmt.Sprintf("pending=%s/actual=%t", pending, actual), func(t *testing.T) {
				t.Parallel()

				s := store.New()
				c := New(s, nil, "test")
				root := &manifest.Kustomization{Name: "p", Namespace: "apps"}
				materializer := &manifest.Kustomization{Name: "materializer", Namespace: "apps"}
				s.AddObjects([]manifest.BaseManifest{root, materializer})
				target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "credentials", Namespace: "apps"}
				if pending != "none" {
					proof := manifest.Production{Source: manifest.BootstrapSourceID}
					if pending == "renderer" {
						proof.Owner = materializer.Named()
					} else {
						proof.Authored = target
						proof.Path = "source/credentials.yaml"
						proof.File = "credentials.yaml"
					}
					s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{proof}}}})
				}
				declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "declaration", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": target.Name, "template": map[string]any{"data": map[string]any{"X": "remote"}}}}}
				c.Emit(root.Named(), &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true})
				if actual {
					s.AddObject(&manifest.Secret{Name: target.Name, Namespace: target.Namespace, StringData: map[string]any{"X": "actual"}})
				}
				data := values.NewStoreProvider(s, true).Data(target)
				if pending != "none" && !actual {
					if data != nil {
						t.Fatal("declaration placeholder outranked pending materializer")
					}
					return
				}
				if data == nil {
					t.Fatal("usable data missing")
				}
				got, _, err := data.Value("X")
				want := "..PLACEHOLDER_X.."
				if actual {
					want = "actual"
				}
				if err != nil || got != want {
					t.Fatalf("value=%q want %q error=%v", got, want, err)
				}
			})
		}
	}
}

func TestEmit_MetadataRefreshFingerprintReuse(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, "test")
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	childID := testutil.KustomizationID("apps", "child")
	publish := func(label, path string) {
		t.Helper()
		doc := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "child", "namespace": "apps", "labels": map[string]any{"kustomize.toolkit.fluxcd.io/name": label}}, "spec": map[string]any{"path": path}}
		children, docs := ParseChildren(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
		_, next := s.GetEmission(parent.Named())
		defer next.Finish()
		next.Children, next.Manifests, next.Complete = children, docs, true
		c.Emit(parent.Named(), &next)
	}
	publish("first", "app")
	_, next := s.GetEmission(childID)
	fingerprint, _, _, _ := c.FingerprintDedup(childID, "effective-owner-input")
	next.Fingerprint, next.Complete = fingerprint, true
	s.RecordEmitted(childID, &next)
	next.Finish()
	before := s.GetArtifact(childID)
	s.UpdateStatus(childID, store.StatusReady, "", "")
	publish("second", "app")
	var arrivals, statuses int
	stopObjects := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	defer stopObjects()
	stopStatus := s.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { statuses++ }, false)
	defer stopStatus()
	if _, handled, _, err := c.FingerprintDedup(childID, "effective-owner-input"); !handled || err != nil || s.GetArtifact(childID) != before || arrivals != 0 || statuses != 0 {
		t.Fatalf("metadata refresh discarded current artifact or replayed work: handled=%t error=%v arrivals=%d statuses=%d", handled, err, arrivals, statuses)
	}
	publish("third", "different")
	if s.GetArtifact(childID) != nil {
		t.Fatal("changed effective input exposed stale current artifact")
	}
	if obj, accepted := s.GetEmission(childID); obj == nil || accepted.Fingerprint != fingerprint {
		t.Fatal("failed successor lost accepted inventory")
	}
}

func TestEmit_PartialRetainsUnvisitedDocuments(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, "test")
	root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	s.AddObject(root)
	invalid := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "invalid", "namespace": "apps"}, "spec": "invalid"}
	secret := map[string]any{"apiVersion": "v1", "kind": manifest.KindSecret, "metadata": map[string]any{"name": "secret", "namespace": "apps"}, "stringData": map[string]any{"X": "ENC[AES256_GCM,data:encrypted]"}}
	valid := manifest.DeepCopyMap(invalid)
	valid["spec"] = map[string]any{"path": "original"}
	children, docs := ParseChildren(root.Named(), []map[string]any{secret, valid, invalid}, manifest.ParseDocOptions{WipeSecrets: true})
	if len(children) != 2 || len(docs) != 3 {
		t.Fatal("invalid predecessor fixture")
	}
	_, next := s.GetEmission(root.Named())
	next.Children, next.Manifests, next.Complete = children, docs, true
	c.Emit(root.Named(), &next)
	next.Finish()
	_, inherited := s.GetEmission(root.Named())
	inherited.Children = nil
	c.Emit(root.Named(), &inherited)
	_, retained := s.GetEmission(root.Named())
	if diff := cmp.Diff(docs, retained.Manifests); diff != "" {
		t.Fatalf("inherited partial changed order or bytes (-want +got): %s", diff)
	}
	inherited.Finish()
	fresh := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": "fresh", "namespace": "apps"}, "data": map[string]any{"value": "first"}}
	incoming, incomingDocs := ParseChildren(root.Named(), []map[string]any{fresh}, manifest.ParseDocOptions{})
	_, partial := s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, incomingDocs
	c.Emit(root.Named(), &partial)
	partial.Finish()
	_, retained = s.GetEmission(root.Named())
	want := append([]map[string]any{fresh}, docs...)
	if diff := cmp.Diff(want, retained.Manifests); diff != "" {
		t.Fatalf("replacement-doc partial discarded predecessor (-want +got): %s", diff)
	}
	fresh = manifest.DeepCopyMap(fresh)
	fresh["data"].(map[string]any)["value"] = "second"
	incoming, incomingDocs = ParseChildren(root.Named(), []map[string]any{fresh}, manifest.ParseDocOptions{})
	_, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, incomingDocs
	c.Emit(root.Named(), &partial)
	partial.Finish()
	_, retained = s.GetEmission(root.Named())
	want = append([]map[string]any{fresh}, docs...)
	if diff := cmp.Diff(want, retained.Manifests); diff != "" {
		t.Fatalf("visited actual child did not replace only its documents (-want +got): %s", diff)
	}
	valid = manifest.DeepCopyMap(valid)
	valid["spec"].(map[string]any)["path"] = "updated"
	incoming, incomingDocs = ParseChildren(root.Named(), []map[string]any{valid}, manifest.ParseDocOptions{})
	_, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, incomingDocs
	c.Emit(root.Named(), &partial)
	partial.Finish()
	_, retained = s.GetEmission(root.Named())
	want = []map[string]any{valid, fresh, docs[0], invalid}
	if diff := cmp.Diff(want, retained.Manifests); diff != "" {
		t.Fatalf("same-identity invalid output was mistaken for admitted ownership (-want +got): %s", diff)
	}
	c.Emit(root.Named(), &store.Emission{Complete: true})
	_, retained = s.GetEmission(root.Named())
	if len(retained.Manifests) != 0 || len(retained.Children) != 0 {
		t.Fatal("complete empty retained unbound invalid output")
	}
}

func TestEmit_DefaultsNamespace(t *testing.T) {
	t.Parallel()
	for _, kind := range []manifest.KindInfo{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease}} {
		for _, fallback := range []string{"", "apps"} {
			t.Run(kind.Kind+"/default="+fallback, func(t *testing.T) {
				t.Parallel()
				s := store.New()
				c := New(s, nil, "test")
				parent := manifest.NamedResource{Group: kind.Group, Kind: kind.Kind, Namespace: "parent", Name: "infra"}
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
				s.AddObject(&manifest.RawObject{APIVersion: parent.Group + "/v1", Kind: parent.Kind, Name: parent.Name, Namespace: parent.Namespace})
				children, retained := ParseChildren(parent, docs, opts)
				c.Emit(parent, &store.Emission{Children: children, Manifests: retained, Complete: true})
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
				replayed, replay := ParseChildren(parent, retained, opts)
				c.Emit(parent, &store.Emission{Children: replayed, Manifests: replay, Complete: true})
				for i := range children {
					assert.Equal(t, replayed[i].Obj.Named(), children[i].Obj.Named())
				}
				assert.Diff(t, replay, before)
			})
		}
	}
}

func TestKeepEmitted_RefiresSourcesAndKustomizationsOnly(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil, "test")
	parent := testutil.KustomizationID("apps", "parent")
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}}}}
	release := &manifest.HelmRelease{Name: "release", Namespace: "apps",
		Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "chart"}}}
	for _, obj := range []manifest.BaseManifest{producer, chart, consumer, release} {
		s.AddObject(obj)
		s.UpdateStatus(obj.Named(), store.StatusReady, store.ReasonUnchanged, "unchanged")
	}
	foreignProducer := &manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindKustomization, Name: producer.Name, Namespace: producer.Namespace}
	s.AddObject(foreignProducer)
	s.UpdateStatus(foreignProducer.Named(), store.StatusReady, "", "foreign unchanged")
	foreignSettings := settings
	foreignSettings.Group = "example.test"
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			parent:          {File: "parent.yaml"},
			settings:        {Productions: []manifest.Production{{Owner: producer.Named(), Authored: settings, Path: "/fixtures/settings.yaml", File: "settings.yaml", Source: manifest.BootstrapSourceID}}},
			foreignSettings: {Productions: []manifest.Production{{Owner: foreignProducer.Named(), Authored: foreignSettings, Path: "/fixtures/foreign-settings.yaml", File: "foreign-settings.yaml", Source: manifest.BootstrapSourceID}}},
		},
	}
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s), Index: index})

	c.KeepEmitted(parent, consumer)
	c.KeepEmitted(parent, release)

	for _, tc := range []struct {
		obj  manifest.BaseManifest
		want store.Status
	}{{producer, store.StatusPending}, {chart, store.StatusReady}, {foreignProducer, store.StatusReady}} {
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
			t.Parallel()
			s := store.New()
			c := New(s, nil, "test")
			obj := tc.obj.WithNamespace("apps")
			s.AddObject(obj)
			s.UpdateStatus(obj.Named(), store.StatusReady, store.ReasonUnchanged, "unchanged")
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
			}
			c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s), Index: index})
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
