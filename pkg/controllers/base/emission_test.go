package base

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"testing"
	"testing/fstest"
	"weak"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize" // Standalone controller tests MUST install the process scope owner before parsing.
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

func TestEmit_RetainsSanitizedInvalidDocuments(t *testing.T) {
	t.Parallel()

	for _, wipe := range []bool{false, true} {
		t.Run(fmt.Sprint(wipe), func(t *testing.T) {
			t.Parallel()

			s := store.New()
			c := New(s, task.NewBounded(2))
			c.Configure(Options{SkipKinds: []string{"ConfigMap"}})
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", TargetNamespace: "apps"}
			s.AddObject(parent)
			doc := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "credentials"}, "stringData": map[string]any{"X": "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"}}
			skipped := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "skipped", "namespace": "apps"}}
			invalid := map[string]any{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "invalid"}, "spec": "invalid"}
			borrowed := []map[string]any{doc, skipped, invalid}
			before := []map[string]any{manifest.DeepCopyMap(doc), manifest.DeepCopyMap(skipped), manifest.DeepCopyMap(invalid)}
			children, docs, failures := c.ParseChildren(parent, testutil.DocumentOccurrences(borrowed), manifest.ParseDocOptions{WipeSecrets: wipe})
			if len(failures) != 1 || !errors.Is(failures[0].Err, manifest.ErrInput) {
				t.Fatalf("retained failures = %v", failures)
			}
			if len(children) != 2 || len(docs) != 2 {
				t.Fatalf("parse: children=%d docs=%d", len(children), len(docs))
			}
			want := "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
			if wipe {
				want = manifest.ValuePlaceholder("X")
			}
			assert.Equal(t, docs[0].Document["stringData"].(map[string]any)["X"], any(want))
			assert.Diff(t, docs[1].Document, invalid)
			assert.Diff(t, children[1].Doc, skipped)
			assert.Diff(t, borrowed, before)
			if fmt.Sprintf("%p", children[0].Doc) != fmt.Sprintf("%p", docs[0].Document) {
				t.Fatal("child document does not share authoritative map")
			}
			next := &store.Emission{Children: children, Manifests: docs, Failures: failures, Complete: true}
			c.Emit(parent.Named(), next, nil)
			children[0] = store.Child{}
			docs[0].Document = nil
			if next.Footprint() != nil || next.Complete {
				t.Fatal("failed inventory acquired a successful footprint")
			}
			_, _, accepted := s.GetEmission(parent.Named())
			if len(accepted.Children) != 2 || accepted.Children[0].Obj == nil || accepted.Manifests[0].Document == nil {
				t.Fatal("caller slice headers mutate accepted inventory")
			}
		})
	}
}

func TestEmit_OwnershipPrecedesArrivals(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			s, tasks := store.New(), task.NewBounded(workers)
			c := New(s, tasks)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", TargetNamespace: "tenant"}
			sibling := &manifest.Kustomization{Name: "sibling", Namespace: "tenant"}
			s.AddObjects([]manifest.BaseManifest{parent, sibling})
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}, sibling.Named(): {File: "sibling.yaml"}}}
			filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, "")
			c.Configure(Options{Filter: filter, Index: index})
			docs := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings"}, "data": map[string]any{"VALUE": "value"}},
				{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "child"}, "spec": map[string]any{"path": "./app"}},
			}
			children, retained, failures := c.ParseChildren(parent, testutil.DocumentOccurrences(docs), manifest.ParseDocOptions{})
			assert.Equal(t, len(failures), 0)
			type observation struct {
				id                       manifest.NamedResource
				selected, visible, owned bool
			}
			arrivals := make(chan observation, len(children))
			unsubscribe := s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if payload == nil {
					return
				}
				visible, selected := true, true
				for _, child := range children {
					visible = visible && s.GetObject(child.Obj.Named()) != nil
					selected = selected && filter.ShouldReconcile(child.Obj.Named())
				}
				arrivals <- observation{id, selected, visible, len(s.Productions(id, index)) > 0}
			}, false)
			defer unsubscribe()
			tasks.Go(t.Context(), "publisher", func(context.Context) {
				execution := newExecution(c, parent.Named())
				defer execution.Close()
				_, _, next := execution.Reads.GetEmission(parent.Named())
				defer next.Finish()
				next.Children, next.Manifests, next.Complete = children, retained, true
				if err := execution.Emit(parent.Named(), &next, nil); err != nil {
					t.Error(err)
				}
				if !execution.Reads.Valid() {
					t.Error("publication captured its own children as inputs")
				}
			})
			tasks.BlockTillDone()
			assert.Equal(t, tasks.Failures(), int64(0))
			assert.Equal(t, len(arrivals), 2)
			for range len(arrivals) {
				got := <-arrivals
				assert.Equal(t, got.id.Namespace, "tenant")
				assert.Equal(t, got.selected, true)
				assert.Equal(t, got.visible, true)
				assert.Equal(t, got.owned, true)
			}
			assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
			assert.Equal(t, s.GetObject(testutil.KustomizationID("", "child")), nil)
		})
	}
}

func TestEmit_PayloadProvenance(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	child, childDoc := emissionDependency(t, "child", "  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n")
	doc := map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": manifest.KindExternalArtifact, "metadata": map[string]any{"name": "output", "namespace": "apps"}, "spec": map[string]any{}}
	children, docs := parseChildrenForTest(parent.Named(), []map[string]any{childDoc, doc}, manifest.ParseDocOptions{})
	if len(children) != 2 {
		t.Fatalf("parsed children = %d, want 2", len(children))
	}
	target := children[1].Obj.Named()
	payload := &store.SourceArtifact{Kind: target.Kind, Key: "key", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}}
	_, _, attempt := s.GetEmission(parent.Named())
	defer attempt.Finish()
	attempt.Children, attempt.Manifests = children, testutil.DocumentOccurrences(docs)
	c.Emit(parent.Named(), &attempt, map[manifest.NamedResource]*store.SourceArtifact{target: payload})
	checkPayload := func() {
		t.Helper()
		if s.GetArtifact(target, store.ArtifactContent) != payload {
			t.Fatal("publication lost the accepted payload")
		}
		data, err := fs.ReadFile(s.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact).FS, "file")
		if err != nil || string(data) != "bytes" {
			t.Fatalf("accepted bytes = %q, %v", data, err)
		}
	}
	checkPayload()
	if attempt.Children[0].Obj != children[0].Obj || child.DependsOn[0].Name != "${PREFIX:-dev-}config" {
		t.Fatal("dependency substitution mutated the caller's children")
	}
	if got := s.GetObject(child.Named()).(*manifest.Kustomization).DependsOn[0].Name; got != "dev-config" {
		t.Fatalf("admitted dependency = %q, want dev-config", got)
	}
	attempt.Children, attempt.Complete, attempt.Fingerprint = children, true, manifest.Fingerprint("inputs")
	c.Emit(parent.Named(), &attempt, nil)
	checkPayload()
	if attempt.Children[0].Obj == children[0].Obj || attempt.Children[0].Obj.(*manifest.Kustomization).DependsOn[0].Name != "dev-config" || attempt.Footprint() == nil {
		t.Fatal("completed emission did not return its substituted accepted inventory")
	}
	if _, handled, err := newExecution(c, parent.Named()).FingerprintDedup(parent.Named(), "inputs", &store.Emission{}); !handled || err != nil {
		t.Fatalf("borrowed replay handled=%t err=%v", handled, err)
	}
	checkPayload()
	fresh, retained := parseChildrenForTest(parent.Named(), docs, manifest.ParseDocOptions{})
	c.Emit(parent.Named(), &store.Emission{Children: fresh, Manifests: testutil.DocumentOccurrences(retained), Fingerprint: attempt.Fingerprint, Complete: true}, nil)
	if s.GetObject(target) == nil || s.GetArtifact(target, store.ArtifactContent) != nil {
		t.Fatal("fresh equal declaration-only emission retained borrowed payload authority")
	}
}

func TestEmit_SelfCopyIsNotPublished(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "effective"}
	s.AddObject(parent)
	docs := []map[string]any{{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "parent", "namespace": "apps"}, "spec": map[string]any{"path": "stale"}}}
	children, retained := parseChildrenForTest(parent.Named(), docs, manifest.ParseDocOptions{})
	if len(children) != 0 {
		t.Fatal("self copy admitted")
	}
	c.Emit(parent.Named(), &store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(retained), Complete: true}, nil)
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
			c := New(s, nil)
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
				children, docs := parseChildrenForTest(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: wipe})
				c.Emit(parent.Named(), &store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true}, nil)
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
			c.Emit(parent.Named(), &store.Emission{Complete: true}, nil)
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
				c := New(s, nil)
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
					children, docs := parseChildrenForTest(owner, []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: wipe})
					c.Emit(owner, &store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true}, nil)
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
				c.Emit(root.Named(), &store.Emission{Complete: true}, nil)
				for _, target := range tc.targets {
					if got := s.Productions(target, nil); len(got) != 1 || got[0].Owner != supporter.Named() {
						t.Fatalf("omission withdrew independent support: %v", got)
					}
					target.Name = "next"
					if len(s.Productions(target, nil)) != 0 {
						t.Fatal("omission retained cluster-only target")
					}
				}
				c.Emit(supporter.Named(), &store.Emission{Complete: true}, nil)
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
				c := New(s, nil)
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
				c.Emit(root.Named(), &store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true}, nil)
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
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	childID := testutil.KustomizationID("apps", "child")
	publish := func(label, path string) {
		t.Helper()
		doc := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "child", "namespace": "apps", "labels": map[string]any{"kustomize.toolkit.fluxcd.io/name": label}}, "spec": map[string]any{"path": path}}
		children, docs := parseChildrenForTest(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
		_, _, next := s.GetEmission(parent.Named())
		defer next.Finish()
		next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
		c.Emit(parent.Named(), &next, nil)
	}
	publish("first", "app")
	_, _, next := s.GetEmission(childID)
	fingerprint, _, _ := newExecution(c, childID).FingerprintDedup(childID, "effective-owner-input", &store.Emission{})
	next.Fingerprint, next.Complete = fingerprint, true
	s.RecordEmitted(childID, &next, nil)
	next.Finish()
	before := s.GetArtifact(childID, store.ArtifactContent)
	s.UpdateStatus(childID, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	publish("second", "app")
	var arrivals, statuses int
	stopObjects := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	defer stopObjects()
	stopStatus := s.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) { statuses++ }, false)
	defer stopStatus()
	if _, handled, err := newExecution(c, childID).FingerprintDedup(childID, "effective-owner-input", &store.Emission{}); !handled || err != nil || s.GetArtifact(childID, store.ArtifactContent) != before || arrivals != 0 || statuses != 0 {
		t.Fatalf("metadata refresh discarded current artifact or replayed work: handled=%t error=%v arrivals=%d statuses=%d", handled, err, arrivals, statuses)
	}
	publish("third", "different")
	if s.GetArtifact(childID, store.ArtifactContent) != nil {
		t.Fatal("changed effective input exposed stale current artifact")
	}
	obj, _, accepted := s.GetEmission(childID)
	_, replayable := accepted.Replay(fingerprint)
	accepted.Finish()
	if obj == nil || !replayable {
		t.Fatal("failed successor lost accepted render identity")
	}
}

func TestFingerprintDedup_WarningsRebind(t *testing.T) {
	t.Parallel()
	for _, invalidate := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalidate=%t", invalidate), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, task.NewBounded(2))
			obj := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
			s.AddObject(obj)
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{obj.Named(): {File: "reader.yaml"}}}
			c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"reader.yaml"}), index, s, ""), Index: index})
			first := newExecution(c, obj.Named())
			first.Reads.GetObject(obj.Named())
			warning := manifest.Warning{Resource: obj.Named(), Category: manifest.WarnStaleValues, Message: "unused value"}
			first.Reads.AddWarning(warning)
			_, _, next := first.Reads.GetEmission(obj.Named())
			next.Children = []store.Child{{Obj: child}}
			next.Fingerprint, next.Complete = manifest.Fingerprint("equal projection"), true
			assert.NoError(t, first.Emit(obj.Named(), &next, nil))
			next.Finish()
			first.Reads.Accept()
			defer first.Close()
			if invalidate {
				changed := obj.Clone()
				changed.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "parent"}
				s.AddObject(changed)
			}
			assert.Equal(t, first.Valid(), !invalidate)
			second := newExecution(c, obj.Named())
			if _, handled, err := second.FingerprintDedup(obj.Named(), "equal projection", &store.Emission{}); !handled || err != nil {
				t.Fatalf("projection replay: handled=%t error=%v", handled, err)
			}
			second.Reads.Accept()
			defer second.Close()
			warning.Count = 1
			assert.Diff(t, s.Warnings(), []manifest.Warning{warning})
			assert.Equal(t, second.Valid(), true)
			s.SetIncluded(child.Named(), false)
			if !second.Valid() {
				t.Fatal("replayed output became a consumed input of its own producer")
			}
		})
	}
}

func TestEmit_PartialRetainsUnvisitedDocuments(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	s.AddObject(root)
	invalid := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "unvisited", "namespace": "apps"}, "spec": map[string]any{"path": "unvisited"}}
	secret := map[string]any{"apiVersion": "v1", "kind": manifest.KindSecret, "metadata": map[string]any{"name": "secret", "namespace": "apps"}, "stringData": map[string]any{"X": "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"}}
	valid := manifest.DeepCopyMap(invalid)
	valid["metadata"].(map[string]any)["name"] = "updated"
	valid["spec"] = map[string]any{"path": "original"}
	children, docs := parseChildrenForTest(root.Named(), []map[string]any{secret, valid, invalid}, manifest.ParseDocOptions{WipeSecrets: true})
	if len(children) != 3 || len(docs) != 3 {
		t.Fatal("invalid predecessor fixture")
	}
	_, _, next := s.GetEmission(root.Named())
	next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
	c.Emit(root.Named(), &next, nil)
	next.Finish()
	_, _, inherited := s.GetEmission(root.Named())
	inherited.Children = nil
	c.Emit(root.Named(), &inherited, nil)
	_, _, retained := s.GetEmission(root.Named())
	if diff := cmp.Diff(docs, testutil.DocumentMaps(retained.Manifests)); diff != "" {
		t.Fatalf("inherited partial changed order or bytes (-want +got): %s", diff)
	}
	inherited.Finish()
	fresh := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": "fresh", "namespace": "apps"}, "data": map[string]any{"value": "first"}}
	incoming, incomingDocs := parseChildrenForTest(root.Named(), []map[string]any{fresh}, manifest.ParseDocOptions{})
	_, _, partial := s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.Emit(root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want := append([]map[string]any{fresh}, docs...)
	if diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests)); diff != "" {
		t.Fatalf("replacement-doc partial discarded predecessor (-want +got): %s", diff)
	}
	fresh = manifest.DeepCopyMap(fresh)
	fresh["data"].(map[string]any)["value"] = "second"
	incoming, incomingDocs = parseChildrenForTest(root.Named(), []map[string]any{fresh}, manifest.ParseDocOptions{})
	_, _, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.Emit(root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want = append([]map[string]any{fresh}, docs...)
	if diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests)); diff != "" {
		t.Fatalf("visited actual child did not replace only its documents (-want +got): %s", diff)
	}
	valid = manifest.DeepCopyMap(valid)
	valid["spec"].(map[string]any)["path"] = "updated"
	incoming, incomingDocs = parseChildrenForTest(root.Named(), []map[string]any{valid}, manifest.ParseDocOptions{})
	_, _, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.Emit(root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want = []map[string]any{valid, fresh, docs[0], invalid}
	if diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests)); diff != "" {
		t.Fatalf("same-identity invalid output was mistaken for admitted ownership (-want +got): %s", diff)
	}
	c.Emit(root.Named(), &store.Emission{Complete: true}, nil)
	_, _, retained = s.GetEmission(root.Named())
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
				c := New(s, nil)
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
				children, retained := parseChildrenForTest(parent, docs, opts)
				c.Emit(parent, &store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(retained), Complete: true}, nil)
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
				replayed, replay := parseChildrenForTest(parent, retained, opts)
				c.Emit(parent, &store.Emission{Children: replayed, Manifests: testutil.DocumentOccurrences(replay), Complete: true}, nil)
				for i := range children {
					assert.Equal(t, replayed[i].Obj.Named(), children[i].Obj.Named())
				}
				assert.Diff(t, replay, before)
			})
		}
	}
}

func TestKeepEmitted_InvalidatesCapturedExclusion(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
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
		s.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"})
	}
	foreignProducer := &manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindKustomization, Name: producer.Name, Namespace: producer.Namespace}
	s.AddObject(foreignProducer)
	s.UpdateStatus(foreignProducer.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "foreign unchanged"})
	foreignSettings := settings
	foreignSettings.Group = "example.test"
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			parent:          {File: "parent.yaml"},
			settings:        {Productions: []manifest.Production{{Owner: producer.Named(), Authored: settings, Path: "/fixtures/settings.yaml", File: "settings.yaml", Source: manifest.BootstrapSourceID}}},
			foreignSettings: {Productions: []manifest.Production{{Owner: foreignProducer.Named(), Authored: foreignSettings, Path: "/fixtures/foreign-settings.yaml", File: "foreign-settings.yaml", Source: manifest.BootstrapSourceID}}},
		},
	}
	c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, ""), Index: index})

	captured := make(map[manifest.NamedResource]*Execution)
	for _, obj := range []manifest.BaseManifest{producer, chart, foreignProducer} {
		s.SetIncluded(obj.Named(), false)
		execution := newExecution(c, obj.Named())
		t.Cleanup(execution.Close)
		if execution.Reads.Admitted() {
			t.Fatalf("%s was admitted before its consumer", obj.Named())
		}
		captured[obj.Named()] = execution
	}

	c.KeepEmitted(parent, store.Child{Obj: consumer}, nil)
	c.KeepEmitted(parent, store.Child{Obj: release}, nil)

	for _, tc := range []struct {
		obj      manifest.BaseManifest
		included bool
	}{{producer, true}, {chart, true}, {foreignProducer, false}} {
		if captured[tc.obj.Named()].Reads.Valid() == tc.included {
			t.Errorf("%s captured exclusion invalidation does not match inclusion=%t", tc.obj.Named(), tc.included)
		}
		next := newExecution(c, tc.obj.Named())
		t.Cleanup(next.Close)
		if next.Reads.Admitted() != tc.included {
			t.Errorf("%s successor admission does not match inclusion=%t", tc.obj.Named(), tc.included)
		}
		if info, _ := s.GetStatus(tc.obj.Named()); info.Status != store.StatusReady {
			t.Errorf("%s terminal status changed: %+v", tc.obj.Named(), info)
		}
	}

	for _, obj := range []manifest.BaseManifest{
		&manifest.GitRepository{Name: "child"},
		&manifest.OCIRepository{Name: "child"},
		&manifest.HelmRepository{Name: "child"},
		&manifest.Bucket{Name: "child"},
		&manifest.ExternalArtifact{Name: "child"},
		&manifest.Kustomization{Name: "child"},
		&manifest.HelmChartSource{Name: "child"},
		&manifest.HelmRelease{Name: "child"},
		&manifest.ResourceSet{Name: "child"},
		&manifest.ResourceSetInputProvider{Name: "child"},
		&manifest.ConfigMap{Name: "child"},
		&manifest.Secret{Name: "child"},
	} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			c := New(s, nil)
			obj := obj.WithNamespace("apps")
			s.AddObject(obj)
			s.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"})
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent: {File: "parent.yaml"}},
			}
			c.Configure(Options{Filter: change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, ""), Index: index})
			s.SetIncluded(obj.Named(), false)
			execution := newExecution(c, obj.Named())
			t.Cleanup(execution.Close)
			if execution.Reads.Admitted() {
				t.Fatal("child admitted before publication")
			}
			c.KeepEmitted(parent, store.Child{Obj: obj}, nil)
			assert.Equal(t, execution.Reads.Valid(), false)
			next := newExecution(c, obj.Named())
			t.Cleanup(next.Close)
			assert.Equal(t, next.Reads.Admitted(), true)
			info, _ := s.GetStatus(obj.Named())
			assert.Equal(t, info.Status, store.StatusReady)
			assert.Equal(t, info.Outcome(), store.OutcomeUnchanged)
			assert.Equal(t, info.Message, "unchanged")
		})
	}
}

func newExecution(c *Controller, id manifest.NamedResource) *Execution {
	e := new(Execution)
	e.Init(c, id)
	return e
}

func TestEmit_ChangedCompletePayloadDoesNotInheritKey(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	first := newExecution(c, parent.Named())
	defer first.Close()
	_, _, initial := first.Reads.GetEmission(parent.Named())
	doc := map[string]any{"apiVersion": "v1", "kind": manifest.KindConfigMap, "metadata": map[string]any{"name": "output", "namespace": "apps"}, "data": map[string]any{"value": "one"}}
	children, retained := parseChildrenForTest(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
	initial.Children, initial.Manifests = children, testutil.DocumentOccurrences(retained)
	initial.Fingerprint, initial.Complete = manifest.Fingerprint("rendered input"), true
	first.Emit(parent.Named(), &initial, nil)
	initial.Finish()
	if !s.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission).OutputReads.Valid() {
		t.Fatal("initial output is not valid")
	}
	changed := manifest.DeepCopyMap(doc)
	changed["data"].(map[string]any)["value"] = "two"
	_, _, next := s.GetEmission(parent.Named())
	children, retained = parseChildrenForTest(parent.Named(), []map[string]any{changed}, manifest.ParseDocOptions{})
	next.Children, next.Manifests = children, testutil.DocumentOccurrences(retained)
	next.Complete = true
	c.Emit(parent.Named(), &next, nil)
	next.Finish()
	accepted := s.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission)
	if accepted.Fingerprint != "" {
		t.Errorf("changed complete output inherited key %q", accepted.Fingerprint)
	}
	if _, found := accepted.Replay(initial.Fingerprint); found {
		t.Error("changed complete output retained a false render mapping")
	}
	if diff := cmp.Diff([]map[string]any{changed}, testutil.DocumentMaps(accepted.Manifests)); diff != "" {
		t.Fatalf("changed payload was not published (-want +got): %s", diff)
	}
	second := newExecution(c, parent.Named())
	defer second.Close()
	if _, handled, err := second.FingerprintDedup(parent.Named(), "rendered input", &store.Emission{}); handled || err != nil {
		t.Fatalf("old render input reused changed bytes: handled=%t error=%v", handled, err)
	}
}

func TestExecution_ReplacementReclaimsBorrowedOutput(t *testing.T) {
	t.Parallel()
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			declaration := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "old"}
			st.AddObject(parent)
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			publish := func(obj *manifest.Kustomization) {
				st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: obj}}, Complete: true}, nil)
			}
			publish(declaration)
			tasks := task.NewBounded(2)
			controller := New(st, tasks)
			borrowed := make(chan weak.Pointer[store.ReadSet], 1)
			replaced := make(chan struct{})
			var previous weak.Pointer[store.ReadSet]
			var current *Execution
			tasks.Go(t.Context(), "predecessor", func(context.Context) {
				defer close(borrowed)
				first := new(Execution)
				first.Init(controller, declaration.Named())
				first.Reads.GetObject(declaration.Named())
				first.Reads.GetStatus(parent.Named())
				_, _, attempt := first.Reads.GetEmission(declaration.Named())
				attempt.Children = []store.Child{{Obj: &manifest.ConfigMap{Name: "output", Namespace: "apps", Data: map[string]any{"value": "old"}}}}
				attempt.Complete = true
				first.Emit(declaration.Named(), &attempt, nil)
				attempt.Finish()
				first.Reads.Accept()
				first.Retire()
				if output, ok := st.GetArtifact(declaration.Named(), store.ArtifactContent).(*store.Emission); !ok || output.OutputReads != &first.Reads {
					t.Error("predecessor lost its borrowed output")
					return
				}
				borrowed <- weak.Make(&first.Reads)
				tasks.YieldSlot(func() { <-replaced })
			})
			tasks.Go(t.Context(), "successor", func(context.Context) {
				defer close(replaced)
				var old weak.Pointer[store.ReadSet]
				var ok bool
				tasks.YieldSlot(func() { old, ok = <-borrowed })
				if !ok {
					return
				}
				previous = old
				changed := *declaration
				changed.Path = "new"
				publish(&changed)
				if st.GetArtifact(declaration.Named(), store.ArtifactContent) != nil {
					t.Error("changed parent input left the predecessor artifact visible")
					return
				}
				current = new(Execution)
				current.Init(controller, declaration.Named())
				current.Reads.GetObject(declaration.Named())
				_, _, attempt := current.Reads.GetEmission(declaration.Named())
				name := "output"
				if replacement {
					name = "replacement"
				}
				attempt.Children = []store.Child{{Obj: &manifest.ConfigMap{Name: name, Namespace: "apps", Data: map[string]any{"value": "new"}}}}
				attempt.Complete = true
				current.Emit(declaration.Named(), &attempt, nil)
				attempt.Finish()
				current.Reads.Accept()
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 || current == nil {
				t.Fatal("replacement did not complete")
			}
			defer current.Close()
			for range st.Sweep() {
			}
			runtime.GC()
			if previous.Value() != nil {
				t.Fatal("accepted replacement retained predecessor registration storage")
			}
			if !current.Valid() {
				t.Fatal("replacement retired current output provenance")
			}
			runtime.KeepAlive(st)
			runtime.KeepAlive(current)
		})
	}
}

func TestEmit_InvalidReads(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "A"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "A"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}}}
	filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, "")
	c.Configure(Options{Filter: filter, Index: index})
	run := newExecution(c, parent.Named())
	t.Cleanup(run.Close)
	_, _, next := run.Reads.GetEmission(parent.Named())
	defer next.Finish()
	next.Children, next.Complete = []store.Child{{Obj: child}}, true
	run.Reads.Bind(&next)
	replacement := parent.Clone()
	replacement.Path = "B"
	s.AddObject(replacement)
	if run.Reads.Valid() {
		t.Fatal("changed parent left captured reads valid")
	}
	if !filter.ShouldReconcile(parent.Named()) || filter.ShouldReconcile(child.Named()) || s.GetObject(child.Named()) != nil || s.HasInventory(parent.Named()) {
		t.Fatal("invalid emission fixture already selected or published the child")
	}
	c.Emit(parent.Named(), &next, nil)
	if filter.ShouldReconcile(child.Named()) {
		t.Error("invalid reads extended changed-only selection")
	}
	if s.GetObject(child.Named()) != nil || s.GetDeclaration(child.Named()) != nil || s.HasInventory(parent.Named()) {
		t.Error("invalid reads published a child or parent inventory")
	}
}

func TestEmit_InputReplacementSelection(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "A"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "A"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}}}
	filter := change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s, "")
	c.Configure(Options{Filter: filter, Index: index})
	run := newExecution(c, parent.Named())
	_, _, captured := run.Reads.GetEmission(parent.Named())
	next := &captured
	next.Children, next.Complete = []store.Child{{Obj: child}}, true
	replacement := parent.Clone()
	replacement.Path = "B"
	s.AddObject(replacement)
	run.Emit(parent.Named(), next, nil)
	if run.Reads.Valid() {
		t.Fatal("stale input succeeded")
	}
	if filter.ShouldReconcile(child.Named()) || s.GetObject(child.Named()) != nil {
		t.Fatal("stale input extended selection")
	}
	accepted := child.Clone()
	accepted.Path = "B"
	fresh := captureEmission(s, parent.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}, Complete: true})
	newExecution(c, parent.Named()).Emit(parent.Named(), fresh, nil)
	if !filter.ShouldReconcile(accepted.Named()) || s.GetDeclaration(accepted.Named()) != accepted {
		t.Fatal("ordinary replacement did not publish and extend selection")
	}
}

func TestFingerprintDedup_AcceptedReceipt(t *testing.T) {
	t.Parallel()
	for _, branch := range []string{"current", "borrowed"} {
		for _, receipt := range []string{"previous", "current", "invalid_input", "aba_input", "retired_input"} {
			t.Run(branch+"/"+receipt, func(t *testing.T) {
				t.Parallel()
				st := store.New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				dependency := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "first"}}
				st.AddObjects([]manifest.BaseManifest{owner, dependency})
				controller := New(st, nil)
				first := newExecution(controller, owner.Named())
				defer first.Retire()
				first.Reads.GetObject(dependency.Named())
				doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "output", "namespace": "apps"}}
				children, docs := testutil.ParseChildren(controller, owner, []map[string]any{doc}, manifest.ParseDocOptions{})
				next := store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Fingerprint: manifest.Fingerprint("content"), Complete: true, Render: manifest.NewRenderReceipt("previous", func(a, b any) bool { return a == b })}
				first.Emit(owner.Named(), &next, nil)
				if branch == "borrowed" {
					progress := newExecution(controller, owner.Named())
					defer progress.Retire()
					partial := store.Emission{}
					progress.Emit(owner.Named(), &partial, nil)
					if st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission).Complete {
						t.Fatal("fixture did not preserve a borrowed complete predecessor")
					}
				}
				if receipt == "retired_input" {
					first.Reads.Retire()
					for range st.Sweep() {
					}
				}
				if receipt == "invalid_input" || receipt == "aba_input" || receipt == "retired_input" {
					st.AddObject(&manifest.ConfigMap{Name: dependency.Name, Namespace: dependency.Namespace, Data: map[string]any{"value": "changed"}})
				}
				if receipt == "aba_input" {
					st.AddObject(dependency)
				}
				if receipt == "retired_input" && !first.Reads.Valid() {
					t.Fatal("fixture did not retain an immutable projection without active subscriptions")
				}
				second := newExecution(controller, owner.Named())
				defer second.Retire()
				attempt := store.Emission{}
				want := "previous"
				switch receipt {
				case "current":
					attempt.Render = manifest.NewRenderReceipt("current", func(a, b any) bool { return a == b })
					want = "current"
				case "invalid_input", "aba_input", "retired_input":
					want = ""
				}
				_, handled, err := second.FingerprintDedup(owner.Named(), "content", &attempt)
				assert.NoError(t, err)
				assert.Equal(t, handled, true)
				var expected *manifest.RenderReceipt
				if want != "" {
					expected = manifest.NewRenderReceipt(want, func(a, b any) bool { return a == b })
				}
				assert.Diff(t, attempt.Render, expected)
				assert.Equal(t, attempt.Reads, &second.Reads)
				assert.Diff(t, st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission).Render, expected)
				if attempt.Footprint() == nil || !attempt.Complete {
					t.Fatal("replay did not hand back exact complete acceptance")
				}
				ids := []manifest.NamedResource{}
				for id := range attempt.Footprint() {
					ids = append(ids, id)
				}
				assert.Diff(t, ids, []manifest.NamedResource{children[0].Obj.Named()})
			})
		}
	}
}

func TestEmit_RejectedAttemptClearsFootprint(t *testing.T) {
	t.Parallel()
	for _, caller := range []string{"store", "controller"} {
		t.Run(caller, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			dependency := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "first"}}
			st.AddObjects([]manifest.BaseManifest{owner, dependency})
			controller := New(st, nil)
			run := newExecution(controller, owner.Named())
			defer run.Retire()
			run.Reads.GetObject(dependency.Named())
			next := store.Emission{Complete: true, Render: manifest.NewRenderReceipt("accepted", func(a, b any) bool { return a == b })}
			run.Emit(owner.Named(), &next, nil)
			if next.Footprint() == nil {
				t.Fatal("fixture did not accept complete-empty output")
			}
			before := st.GetArtifact(owner.Named(), store.ArtifactContent)
			st.AddObject(&manifest.ConfigMap{Name: dependency.Name, Namespace: dependency.Namespace, Data: map[string]any{"value": "changed"}})
			if caller == "store" {
				st.RecordEmitted(owner.Named(), &next, nil)
			} else {
				controller.Emit(owner.Named(), &next, nil)
			}
			if next.Footprint() != nil || st.GetArtifact(owner.Named(), store.ArtifactContent) != before {
				t.Fatal("early rejection retained caller acceptance or replaced inventory")
			}
		})
	}
}

func TestEmit_FailedAuthorityReplacesSuccess(t *testing.T) {
	t.Parallel()
	for _, branch := range []string{"current", "borrowed"} {
		t.Run(branch, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			s.AddObject(owner)
			c := New(s, task.NewBounded(2))
			valid := map[string]any{"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease", "metadata": map[string]any{"name": "child", "namespace": "apps"}, "spec": map[string]any{"interval": "10m", "chart": map[string]any{"spec": map[string]any{"chart": "demo", "sourceRef": map[string]any{"kind": "HelmRepository", "name": "charts"}}}}}
			first := newExecution(c, owner.Named())
			defer first.Close()
			children, docs, failures := c.ParseChildren(owner, testutil.DocumentOccurrences([]map[string]any{valid}), manifest.ParseDocOptions{})
			attempt := store.Emission{Children: children, Manifests: docs, Failures: failures, Complete: true, Fingerprint: manifest.Fingerprint("content")}
			if err := first.Emit(owner.Named(), &attempt, nil); err != nil {
				t.Fatal(err)
			}
			first.finish(nil)
			childID := manifest.DocIdentity(valid)
			if s.GetObject(childID) == nil {
				t.Fatal("valid predecessor missing")
			}
			if branch == "borrowed" {
				progress := newExecution(c, owner.Named())
				defer progress.Close()
				if err := progress.Emit(owner.Named(), &store.Emission{}, nil); err != nil {
					t.Fatal(err)
				}
			}
			invalid := manifest.DeepCopyMap(valid)
			invalid["spec"].(map[string]any)["interval"] = []any{}
			sibling := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "healthy", "namespace": "apps"}}
			var diagnostic string
			for _, phase := range []string{"failure", "repeat"} {
				run := newExecution(c, owner.Named())
				defer run.Close()
				outcomes := []manifest.DocOccurrence{{Document: invalid, Ordinal: 3}, {Document: sibling, Ordinal: 4}}
				if phase == "repeat" {
					data, err := manifest.EncodeRender(outcomes)
					if err != nil {
						t.Fatal(err)
					}
					outcomes, err = manifest.DecodeRender(data)
					if err != nil {
						t.Fatal(err)
					}
				}
				children, docs, failures = c.ParseChildren(owner, outcomes, manifest.ParseDocOptions{})
				attempt = store.Emission{Children: children, Manifests: docs, Failures: failures, Complete: true, Fingerprint: manifest.Fingerprint("content")}
				err := run.Emit(owner.Named(), &attempt, nil)
				if !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("%s accepted invalid output: %v", phase, err)
				}
				run.finish(err)
				if diagnostic == "" {
					diagnostic = err.Error()
				}
				assert.Equal(t, err.Error(), diagnostic)
				current := s.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
				if current.Complete || attempt.Footprint() != nil || len(current.Failures) != 1 || len(current.Manifests) != 2 {
					t.Fatalf("failed inventory: %+v", current)
				}
				if _, hit := current.Replay(manifest.Fingerprint("content")); hit {
					t.Fatal("failure replayed predecessor success")
				}
				if s.GetObject(childID) != nil || s.GetObject(manifest.DocIdentity(sibling)) == nil {
					t.Fatal("failed authority restored old child or lost valid sibling")
				}
				info, _ := s.GetStatus(owner.Named())
				assert.Equal(t, info.Status, store.StatusFailed)
			}
			corrected := newExecution(c, owner.Named())
			defer corrected.Close()
			var next store.Emission
			_, hit, err := corrected.FingerprintDedup(owner.Named(), "content", &next)
			if hit || err != nil {
				t.Fatalf("failed fingerprint bypassed admission: %t %v", hit, err)
			}
			next.Children, next.Manifests, next.Failures = c.ParseChildren(owner, testutil.DocumentOccurrences([]map[string]any{valid}), manifest.ParseDocOptions{})
			next.Complete, next.Fingerprint = true, manifest.Fingerprint("content")
			if err := corrected.Emit(owner.Named(), &next, nil); err != nil {
				t.Fatal(err)
			}
			corrected.finish(nil)
			info, _ := s.GetStatus(owner.Named())
			if info.Status != store.StatusReady || s.GetObject(childID) == nil || next.Footprint() == nil {
				t.Fatalf("correction did not recover: %+v", info)
			}
		})
	}
}

func TestParseChildren_RetainsRendererFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		skipKinds []string
		wantDocs  int
	}{
		{name: "included", wantDocs: 2},
		{name: "excluded", skipKinds: []string{"List"}, wantDocs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := &manifest.HelmRelease{Name: "owner", Namespace: "apps"}
			invalid := map[string]any{"apiVersion": "v1", "kind": "List", "items": "invalid"}
			sibling := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "healthy", "namespace": "apps"}}
			failure := fmt.Errorf("%w: normalize helm list", manifest.ErrInput)
			c := Controller{SkipKinds: tc.skipKinds}
			children, docs, failures := c.ParseChildren(owner, []manifest.DocOccurrence{{Document: invalid, Ordinal: 2, Err: failure}, {Document: sibling, Ordinal: 3}}, manifest.ParseDocOptions{})
			if len(children) != 1 || len(docs) != tc.wantDocs || len(failures) != 1 || failures[0].Ordinal != 2 || !errors.Is(failures[0].Err, failure) {
				t.Fatalf("renderer failure was dropped: children=%v docs=%v failures=%v", children, docs, failures)
			}
			assert.Equal(t, children[0].Obj.Named().Name, "healthy")
			assert.Diff(t, docs[len(docs)-1].Document, sibling)
			if tc.wantDocs == 2 {
				assert.Diff(t, docs[0].Document, invalid)
			}
		})
	}
}
