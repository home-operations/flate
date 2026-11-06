package base

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/admissiontest"

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

	t.Run("admission", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		c := New(s, task.NewBounded(2))
		c.Configure(Options{SkipKinds: []string{"ConfigMap"}})
		parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", TargetNamespace: "apps"}
		s.AddObject(parent)
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]any{"name": "credentials"},
			"stringData": map[string]any{"X": "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"},
		}
		skipped := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "skipped", "namespace": "apps"},
		}
		invalid := map[string]any{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": "invalid"},
			"spec":       "invalid",
		}
		borrowed := []map[string]any{doc, skipped, invalid}
		before := []map[string]any{
			manifest.DeepCopyMap(doc),
			manifest.DeepCopyMap(skipped),
			manifest.DeepCopyMap(invalid),
		}
		result1 := (&Execution{Controller: c}).ParseChildren(
			parent,
			testutil.DocumentOccurrences(borrowed),
			manifest.ParseDocOptions{},
		)
		children, docs, failures := result1.Children, result1.Visible, result1.Failures
		assert.FatalIf(t, len(failures) != 1 || !errors.Is(failures[0].Err, manifest.ErrInput),
			"retained failures = %v", failures)
		assert.FatalIf(t, len(children) != 2 || len(docs) != 2,
			"parse: children=%d docs=%d", len(children), len(docs))

		if _, present := docs[0].Document["stringData"].(map[string]any)["X"]; present {
			t.Fatal("encrypted key retained")
		}

		assert.Diff(t, docs[1].Document, invalid)
		assert.Diff(t, children[1].Doc, skipped)
		assert.Diff(t, borrowed, before)
		assert.FatalIf(
			t,
			fmt.Sprintf("%p", children[0].Doc) != fmt.Sprintf("%p", docs[0].Document),
			"%s",
			"child document does not share authoritative map",
		)

		next := &store.Emission{Children: children, Manifests: docs, Failures: failures, Complete: true}
		c.emit(t.Context(), parent.Named(), next, nil)
		children[0] = store.Child{}
		docs[0].Document = nil
		assert.FatalIf(
			t,
			next.Footprint() != nil || next.Complete,
			"%s",
			"failed inventory acquired a successful footprint",
		)

		_, _, accepted := s.GetEmission(parent.Named())
		assert.FatalIf(
			t,
			len(accepted.Children) != 2 || accepted.Children[0].Obj == nil || accepted.Manifests[0].Document == nil,
			"%s",
			"caller slice headers mutate accepted inventory",
		)
	})
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
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					parent.Named():  {File: "parent.yaml"},
					sibling.Named(): {File: "sibling.yaml"},
				},
			}
			filterLoader := admissiontest.New(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"parent.yaml"}),
					Index:     index,
					Inventory: s,
					Root:      "",
				},
			)
			admissiontest.Commit(t, filterLoader, nil)
			filter := filterLoader.Options.Filter
			c.Configure(Options{Filter: filter, Index: index, Loader: filterLoader})
			s.SetIncluded(parent.Named(), true, true)
			docs := []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "settings"},
					"data":       map[string]any{"VALUE": "value"},
				},
				{
					"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
					"kind":       "Kustomization",
					"metadata":   map[string]any{"name": "child"},
					"spec":       map[string]any{"path": "./app"},
				},
			}
			result2 := (&Execution{Controller: c}).ParseChildren(
				parent,
				testutil.DocumentOccurrences(docs),
				manifest.ParseDocOptions{},
			)
			children, retained, failures := result2.Children, result2.Visible, result2.Failures

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

				{
					err := execution.Emit(t.Context(), parent.Named(), &next, nil)
					assert.ErrorIf(t, err != nil, "%v", err)
				}

				assert.ErrorIf(t, !execution.Reads.Valid(), "%v", "publication captured its own children as inputs")
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
	migratedResult1 := emissionDependency(t, "child", "  dependsOn: [{name: '${PREFIX:-dev-}config'}]\n")
	child, childDoc := migratedResult1.object, migratedResult1.document
	doc := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       manifest.KindExternalArtifact,
		"metadata":   map[string]any{"name": "output", "namespace": "apps"},
		"spec":       map[string]any{},
	}
	migratedResult2 := parseChildrenForTest(parent.Named(), []map[string]any{childDoc, doc}, manifest.ParseDocOptions{})
	children, docs := migratedResult2.Children, migratedResult2.Documents

	assert.FatalIf(t, len(children) != 2, "parsed children = %d, want 2", len(children))

	target := children[1].Obj.Named()
	payload := &store.SourceArtifact{
		Kind:   target.Kind,
		Key:    "key",
		Digest: "digest",
		FS:     fstest.MapFS{"file": {Data: []byte("bytes")}},
	}
	_, _, attempt := s.GetEmission(parent.Named())
	defer attempt.Finish()
	attempt.Children, attempt.Manifests = children, testutil.DocumentOccurrences(docs)
	c.emit(t.Context(), parent.Named(), &attempt, map[manifest.NamedResource]*store.SourceArtifact{target: payload})
	checkPayload := func() {
		t.Helper()

		assert.FatalIf(
			t,
			s.GetArtifact(target, store.ArtifactContent) != payload,
			"%v",
			"publication lost the accepted payload",
		)

		data, err := fs.ReadFile(s.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact).FS, "file")

		assert.FatalIf(t, err != nil || string(data) != "bytes", "accepted bytes = %q, %v", data, err)
	}
	checkPayload()

	assert.FatalIf(
		t,
		attempt.Children[0].Obj != children[0].Obj || child.DependsOn[0].Name != "${PREFIX:-dev-}config",
		"%v",
		"dependency substitution mutated the caller's children",
	)

	{
		got := s.GetObject(child.Named()).(*manifest.Kustomization).DependsOn[0].Name
		assert.FatalIf(t, got != "dev-config", "admitted dependency = %q, want dev-config", got)
	}

	attempt.Children, attempt.Complete, attempt.Fingerprint = children, true, manifest.Fingerprint("inputs")
	c.emit(t.Context(), parent.Named(), &attempt, nil)
	checkPayload()

	assert.FatalIf(t, attempt.Children[0].Obj == children[0].Obj ||
		attempt.Children[0].Obj.(*manifest.Kustomization).DependsOn[0].Name != "dev-config" ||
		attempt.Footprint() == nil, "%v", "completed emission did not return its substituted accepted inventory")

	{
		result3, err := newExecution(
			c,
			parent.Named(),
		).FingerprintDedup(t.Context(), parent.Named(), "inputs", &store.Emission{})
		handled := result3.Reused

		assert.FatalIf(t, !handled ||
			err != nil, "borrowed replay handled=%t err=%v", handled, err)
	}

	checkPayload()
	migratedResult3 := parseChildrenForTest(parent.Named(), docs, manifest.ParseDocOptions{})
	fresh, retained := migratedResult3.Children, migratedResult3.Documents
	c.emit(
		t.Context(),
		parent.Named(),
		&store.Emission{
			Children:    fresh,
			Manifests:   testutil.DocumentOccurrences(retained),
			Fingerprint: attempt.Fingerprint,
			Complete:    true,
		},
		nil,
	)

	assert.FatalIf(
		t,
		s.GetObject(target) == nil || s.GetArtifact(target, store.ArtifactContent) != nil,
		"%v",
		"fresh equal declaration-only emission retained borrowed payload authority",
	)
}

func TestEmit_SelfCopyIsNotPublished(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "effective"}
	s.AddObject(parent)
	docs := []map[string]any{
		{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": "parent", "namespace": "apps"},
			"spec":       map[string]any{"path": "stale"},
		},
	}
	migratedResult4 := parseChildrenForTest(parent.Named(), docs, manifest.ParseDocOptions{})
	children, retained := migratedResult4.Children, migratedResult4.Documents

	assert.FatalIf(t, len(children) != 0, "%v", "self copy admitted")

	c.emit(
		t.Context(),
		parent.Named(),
		&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(retained), Complete: true},
		nil,
	)

	assert.FatalIf(t, s.GetObject(parent.Named()) != parent, "%v", "self copy overwrote effective input")
}

func TestEmit_RuntimeProducerReplacement(t *testing.T) {
	t.Parallel()

	t.Run("admission", func(t *testing.T) {
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
				assert.ErrorIf(t, payload != nil, "%s", "declaration installed a data object")
			}
		}, false)
		for _, key := range []string{"X", "Y"} {
			doc := map[string]any{
				"apiVersion": "external-secrets.io/v1",
				"kind":       "ExternalSecret",
				"metadata":   map[string]any{"name": "declaration", "namespace": "apps"},
				"spec": map[string]any{
					"target": map[string]any{
						"name":     target.Name,
						"template": map[string]any{"data": map[string]any{key: "remote"}},
					},
				},
			}
			migratedResult5 := parseChildrenForTest(parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
			children, docs := migratedResult5.Children, migratedResult5.Documents
			c.emit(
				t.Context(),
				parent.Named(),
				&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
				nil,
			)
			assert.FatalIf(t, s.GetObject(target) != nil, "%s", "runtime declaration has a data writer")

			data := values.NewStoreProvider(s).Data(target)
			assert.FatalIf(t, data != nil, "%s", "runtime declaration fabricated data")
		}
		c.emit(t.Context(), parent.Named(), &store.Emission{Complete: true}, nil)
		assert.FatalIf(
			t,
			len(s.Productions(target, nil)) != 0 || values.NewStoreProvider(s).Data(target) != nil,
			"%s",
			"omitted declaration retained",
		)
		assert.FatalIf(t, invalidations != 3,
			"declaration target invalidations=%d want admission, key refresh and omission", invalidations)
	})
}

func TestEmit_RuntimeProducerTargets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, group, kind string
		targets           []manifest.NamedResource
	}{
		{"external", "external-secrets.io", "ExternalSecret", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "old"}}},
		{"sealed", "bitnami.com", "SealedSecret", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "old"}}},
		{"bucket", "objectbucket.io", "ObjectBucketClaim", []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "declaration"}, {Kind: manifest.KindConfigMap, Namespace: "apps", Name: "declaration"}}},
		{"foreign", "example.test", "ExternalSecret", nil},
		{"group-less", "", "SealedSecret", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEmitRuntimeProducerTargetsCase1(t, &tc)
		})
	}
}

func testEmitRuntimeProducerTargetsCase1(t *testing.T, tc *struct {
	name    string
	group   string
	kind    string
	targets []manifest.NamedResource
},
) {
	t.Helper()

	s := store.New()
	c := New(s, nil)
	root := &manifest.Kustomization{Name: "p", Namespace: "apps"}
	supporter := &manifest.Kustomization{Name: "q", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{root, supporter})
	apiVersion := "v1"
	if tc.group != "" {
		apiVersion = tc.group + "/v1"
	}
	spec := map[string]any{
		"target": map[string]any{
			"name":     "old",
			"template": map[string]any{"data": map[string]any{"X": "remote"}},
		},
		"template":      map[string]any{"metadata": map[string]any{"name": "old"}},
		"encryptedData": map[string]any{"X": "ciphertext"},
	}
	doc := map[string]any{
		"apiVersion": apiVersion,
		"kind":       tc.kind,
		"metadata":   map[string]any{"name": "declaration", "namespace": "apps"},
		"spec":       spec,
	}
	publish := func(owner manifest.NamedResource, doc map[string]any) {
		t.Helper()
		migratedResult6 := parseChildrenForTest(owner, []map[string]any{doc}, manifest.ParseDocOptions{})
		children, docs := migratedResult6.Children, migratedResult6.Documents
		c.emit(
			t.Context(),
			owner,
			&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
			nil,
		)
	}
	publish(root.Named(), doc)
	for _, target := range tc.targets {
		if got := s.Productions(target, nil); len(got) != 1 || got[0].Declaration.Kind != tc.kind {
			t.Fatalf("runtime declaration evidence: %v", got)
		}
		data := values.NewStoreProvider(s).Data(target)
		known := false
		assert.FatalIf(t, (data != nil) != known,
			"known-key availability=%t want %t", data != nil, known)
	}
	if len(tc.targets) == 0 {
		for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
			if got := s.Productions(
				manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "old"},
				nil,
			); len(
				got,
			) != 0 {
				t.Fatal("lookalike produced a target")
			}
		}
	}
	testEmitRuntimeProducerTargetsCase1Tail(
		t,
		&testEmitRuntimeProducerTargetsCase1TailInputs{
			tc:        &tc,
			doc:       &doc,
			publish:   &publish,
			supporter: &supporter,
			root:      &root,
			s:         &s,
			c:         &c,
		},
	)
}

func testEmitRuntimeProducerTargetsCase1Part1(
	t *testing.T,
	s **store.Store,
	target *manifest.NamedResource,
	supporter **manifest.Kustomization,
) {
	t.Helper()
	if got := (*s).Productions((*target), nil); len(got) != 1 || got[0].Owner != (*supporter).Named() {
		t.Fatalf("target rename changed independent contribution: %v", got)
	}
	target.
		Name = "next"
	assert.FatalIf(t, len((*s).Productions((*target), nil)) != 1, "%s", "target rename lost new contribution")

	data := values.NewStoreProvider((*s)).Data((*target))
	assert.FatalIf(t, data != nil, "%s", "renamed declaration fabricated data")
}

func TestEmit_RuntimeProducerDataPrecedence(t *testing.T) {
	t.Parallel()

	for _, pending := range []string{"none", "renderer", "authored"} {
		for _, actual := range []bool{false, true} {
			t.Run(fmt.Sprintf("pending=%s/actual=%t", pending, actual), func(t *testing.T) {
				t.Parallel()
				testEmitRuntimeProducerDataPrecedenceCase1(t, &pending, &actual)
			})
		}
	}
}

func testEmitRuntimeProducerDataPrecedenceCase1(t *testing.T, pending *string, actual *bool) {
	t.Helper()

	s := store.New()
	c := New(s, nil)
	root := &manifest.Kustomization{Name: "p", Namespace: "apps"}
	materializer := &manifest.Kustomization{Name: "materializer", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{root, materializer})
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "credentials", Namespace: "apps"}
	if (*pending) != "none" {
		proof := manifest.Production{Source: manifest.BootstrapSourceID}
		if (*pending) == "renderer" {
			proof.Owner = materializer.Named()
		} else {
			proof.Authored = target
			proof.Path = "source/credentials.yaml"
			proof.File = "credentials.yaml"
		}
		s.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					target: {Productions: []manifest.Production{proof}},
				},
			},
		)
	}
	declaration := &manifest.RawObject{
		APIVersion: "external-secrets.io/v1",
		Kind:       "ExternalSecret",
		Name:       "declaration",
		Namespace:  "apps",
		Spec: map[string]any{
			"target": map[string]any{
				"name":     target.Name,
				"template": map[string]any{"data": map[string]any{"X": "remote"}},
			},
		},
	}
	c.emit(
		t.Context(),
		root.Named(),
		&store.Emission{Children: []store.Child{{Obj: declaration}}, Complete: true},
		nil,
	)
	if *actual {
		s.AddObject(
			&manifest.Secret{
				Name:       target.Name,
				Namespace:  target.Namespace,
				StringData: map[string]any{"X": "actual"},
			},
		)
	}
	data := values.NewStoreProvider(s).Data(target)
	if !(*actual) {
		assert.FatalIf(t, data != nil, "%s", "declaration fabricated data")

		return
	}
	assert.FatalIf(t, data == nil, "%s", "usable data missing")

	result4, err := data.Value("X")
	got := result4.Value
	want := "actual"
	assert.FatalIf(t, err != nil || got != want,
		"value=%q want %q error=%v", got, want, err)
}

type testEmitRuntimeProducerTargetsCase1TailInputs struct {
	tc **struct {
		name string

		group string

		kind string

		targets []manifest.
			NamedResource
	}

	doc *map[string]any

	publish *func(
		owner manifest.
			NamedResource,

		doc map[string]any)

	supporter **manifest.
			Kustomization

	root **manifest.
		Kustomization

	s **store.
		Store

	c **Controller
}

func testEmitRuntimeProducerTargetsCase1Tail(t *testing.T, inputs *testEmitRuntimeProducerTargetsCase1TailInputs) {
	t.Helper()

	if len((*inputs.tc).targets) > 0 {
		independent := manifest.DeepCopyMap((*inputs.doc))
		if (*inputs.tc).name != "bucket" {
			independent["metadata"].(map[string]any)["name"] = "independent"
		}
		(*inputs.publish)((*inputs.supporter).Named(), independent)
	}
	renamed := manifest.DeepCopyMap((*inputs.doc))
	rspec := renamed["spec"].(map[string]any)
	rspec["target"].(map[string]any)["name"] = "next"
	rspec["target"].(map[string]any)["template"].(map[string]any)["data"] = map[string]any{"Y": "remote"}
	rspec["template"].(map[string]any)["metadata"].(map[string]any)["name"] = "next"
	rspec["encryptedData"] = map[string]any{"Y": "ciphertext"}
	if (*inputs.tc).name == "bucket" {
		renamed["metadata"].(map[string]any)["name"] = "next"
	}
	(*inputs.publish)((*inputs.root).Named(), renamed)
	for _, target := range (*inputs.tc).targets {
		testEmitRuntimeProducerTargetsCase1Part1(t, inputs.s, &target, inputs.supporter)
	}
	(*inputs.c).
		emit(t.Context(), (*inputs.root).Named(), &store.Emission{Complete: true}, nil)
	for _, target := range (*inputs.tc).targets {
		if got := (*inputs.s).Productions(target, nil); len(got) != 1 || got[0].Owner != (*inputs.supporter).Named() {
			t.Fatalf("omission withdrew independent support: %v", got)
		}
		target.Name = "next"
		assert.FatalIf(t, len((*inputs.s).Productions(target, nil)) != 0, "%s", "omission retained cluster-only target")
	}
	(*inputs.c).
		emit(t.Context(), (*inputs.supporter).Named(), &store.Emission{Complete: true}, nil)
	for _, target := range (*inputs.tc).targets {
		assert.FatalIf(
			t,
			len((*inputs.s).Productions(target, nil)) != 0 || values.NewStoreProvider((*inputs.s)).Data(target) != nil,
			"%s",
			"last supporter withdrawal retained target data",
		)
	}
}
