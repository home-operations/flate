package base

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"weak"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/admissiontest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize" // Standalone controller tests MUST install the process scope owner before parsing.
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestEmit_MetadataRefreshFingerprintReuse(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	childID := testutil.KustomizationID("apps", "child")
	publish := func(label, path string) {
		t.Helper()
		doc := map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata": map[string]any{
				"name":      "child",
				"namespace": "apps",
				"labels":    map[string]any{"kustomize.toolkit.fluxcd.io/name": label},
			},
			"spec": map[string]any{"path": path},
		}
		parseChildrenForTestResult1 := parseChildrenForTest(
			parent.Named(),
			[]map[string]any{doc},
			manifest.ParseDocOptions{},
		)
		children, docs := parseChildrenForTestResult1.Children, parseChildrenForTestResult1.Documents
		_, _, next := s.GetEmission(parent.Named())
		defer next.Finish()
		next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
		c.emit(t.Context(), parent.Named(), &next, nil)
	}
	publish("first", "app")
	_, _, next := s.GetEmission(childID)
	fingerprintDedupResult2, fingerprintDedupResult2Err := newExecution(
		c,
		childID,
	).FingerprintDedup(t.Context(), childID, "effective-owner-input", &store.Emission{})
	fingerprint, _, _ := fingerprintDedupResult2.Fingerprint, fingerprintDedupResult2.Reused, fingerprintDedupResult2Err
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
	{
		fingerprintDedupResult3, fingerprintDedupResult3Err := newExecution(
			c,
			childID,
		).FingerprintDedup(t.Context(), childID, "effective-owner-input", &store.Emission{})
		_, handled, err := fingerprintDedupResult3.Fingerprint, fingerprintDedupResult3.Reused, fingerprintDedupResult3Err
		assert.FatalIf(
			t,
			!handled || err != nil || s.GetArtifact(childID, store.ArtifactContent).(*store.Emission).Fingerprint != before.(*store.Emission).Fingerprint || arrivals != 0 ||
				statuses != 0,
			"metadata refresh discarded current artifact or replayed work: handled=%t error=%v arrivals=%d statuses=%d",
			handled,
			err,
			arrivals,
			statuses,
		)
	}
	publish("third", "different")
	assert.FatalIf(
		t,
		s.GetArtifact(childID, store.ArtifactContent) != nil,
		"%v",
		"changed effective input exposed stale current artifact",
	)
	obj, _, accepted := s.GetEmission(childID)
	_, replayable := accepted.Replay(fingerprint)
	accepted.Finish()
	assert.FatalIf(t, obj == nil || !replayable, "%v", "failed successor lost accepted render identity")
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
			index := &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{obj.Named(): {File: "reader.yaml"}},
			}
			admissionLoader1 := admissiontest.New(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{"reader.yaml"}),
					Index:     index,
					Inventory: s,
					Root:      "",
				},
			)
			admissiontest.Commit(t, admissionLoader1, nil)
			c.Configure(Options{Filter: admissionLoader1.Options.Filter, Index: index, Loader: admissionLoader1})
			first := newExecution(c, obj.Named())
			first.Reads.GetObject(obj.Named())
			warning := manifest.Warning{
				Resource: obj.Named(),
				Category: manifest.WarnStaleValues,
				Message:  "unused value",
			}
			first.Reads.AddWarning(warning)
			first.Reads.RecordSuppliedOverride(
				store.SuppliedOverride{Resource: obj.Named(), Keys: []string{"previous"}},
			)
			_, _, next := first.Reads.GetEmission(obj.Named())
			next.Children = []store.Child{{Obj: child}}
			next.Fingerprint, next.Complete = manifest.Fingerprint("equal projection"), true
			assert.NoError(t, first.Emit(t.Context(), obj.Named(), &next, nil))
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
			current := store.SuppliedOverride{Resource: obj.Named(), Keys: []string{"current"}}
			second.Reads.RecordSuppliedOverride(current)
			{
				fingerprintDedupResult4, fingerprintDedupResult4Err := second.FingerprintDedup(
					t.Context(),
					obj.Named(),
					"equal projection",
					&store.Emission{},
				)
				_, handled, err := fingerprintDedupResult4.Fingerprint, fingerprintDedupResult4.Reused, fingerprintDedupResult4Err
				assert.FatalIf(t, !handled ||
					err != nil, "projection replay: handled=%t error=%v", handled, err)
			}
			second.Reads.Accept()
			defer second.Close()
			warning.Count = 1
			warning.Subject = manifest.DiagnosticFor(obj.Named(), obj)
			assert.Diff(t, s.Warnings(), []manifest.Warning{warning})
			assert.Diff(t, s.AdvisoryEvidence(), store.AdvisoryEvidence{Overrides: []store.SuppliedOverride{current}})
			assert.Equal(t, second.Valid(), true)
			s.SetIncluded(child.Named(), false)
			assert.FatalIf(t, !second.Valid(), "%v", "replayed output became a consumed input of its own producer")
		})
	}
}

func TestEmit_PartialRetainsUnvisitedDocuments(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil)
	root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	s.AddObject(root)
	invalid := map[string]any{
		"apiVersion": manifest.FluxKustomizeDomain + "/v1",
		"kind":       manifest.KindKustomization,
		"metadata":   map[string]any{"name": "unvisited", "namespace": "apps"},
		"spec":       map[string]any{"path": "unvisited"},
	}
	secret := map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindSecret,
		"metadata":   map[string]any{"name": "secret", "namespace": "apps"},
		"stringData": map[string]any{"X": "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"},
	}
	valid := manifest.DeepCopyMap(invalid)
	valid["metadata"].(map[string]any)["name"] = "updated"
	valid["spec"] = map[string]any{"path": "original"}
	parseChildrenForTestResult5 := parseChildrenForTest(
		root.Named(),
		[]map[string]any{secret, valid, invalid},
		manifest.ParseDocOptions{WipeSecrets: true},
	)
	children, docs := parseChildrenForTestResult5.Children, parseChildrenForTestResult5.Documents
	assert.FatalIf(t, len(children) != 3 || len(docs) != 3, "%v", "invalid predecessor fixture")
	_, _, next := s.GetEmission(root.Named())
	next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
	c.emit(t.Context(), root.Named(), &next, nil)
	next.Finish()
	_, _, inherited := s.GetEmission(root.Named())
	inherited.Children = nil
	c.emit(t.Context(), root.Named(), &inherited, nil)
	_, _, retained := s.GetEmission(root.Named())
	{
		diff := cmp.Diff(docs, testutil.DocumentMaps(retained.Manifests))
		assert.FatalIf(t, diff != "", "inherited partial changed order or bytes (-want +got): %s", diff)
	}
	inherited.Finish()
	fresh := map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindConfigMap,
		"metadata":   map[string]any{"name": "fresh", "namespace": "apps"},
		"data":       map[string]any{"value": "first"},
	}
	parseChildrenForTestResult6 := parseChildrenForTest(
		root.Named(),
		[]map[string]any{fresh},
		manifest.ParseDocOptions{},
	)
	incoming, incomingDocs := parseChildrenForTestResult6.Children, parseChildrenForTestResult6.Documents
	_, _, partial := s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.emit(t.Context(), root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want := append([]map[string]any{fresh}, docs...)
	{
		diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests))
		assert.FatalIf(t, diff != "", "replacement-doc partial discarded predecessor (-want +got): %s", diff)
	}
	fresh = manifest.DeepCopyMap(fresh)
	fresh["data"].(map[string]any)["value"] = "second"
	parseChildrenForTestResult7 := parseChildrenForTest(
		root.Named(),
		[]map[string]any{fresh},
		manifest.ParseDocOptions{},
	)
	incoming, incomingDocs = parseChildrenForTestResult7.Children, parseChildrenForTestResult7.Documents
	_, _, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.emit(t.Context(), root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want = append([]map[string]any{fresh}, docs...)
	{
		diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests))
		assert.FatalIf(
			t,
			diff != "",
			"visited actual child did not replace only its documents (-want +got): %s",
			diff,
		)
	}
	valid = manifest.DeepCopyMap(valid)
	valid["spec"].(map[string]any)["path"] = "updated"
	parseChildrenForTestResult8 := parseChildrenForTest(
		root.Named(),
		[]map[string]any{valid},
		manifest.ParseDocOptions{},
	)
	incoming, incomingDocs = parseChildrenForTestResult8.Children, parseChildrenForTestResult8.Documents
	_, _, partial = s.GetEmission(root.Named())
	partial.Children, partial.Manifests = incoming, testutil.DocumentOccurrences(incomingDocs)
	c.emit(t.Context(), root.Named(), &partial, nil)
	partial.Finish()
	_, _, retained = s.GetEmission(root.Named())
	want = []map[string]any{valid, fresh, docs[0], invalid}
	{
		diff := cmp.Diff(want, testutil.DocumentMaps(retained.Manifests))
		assert.FatalIf(
			t,
			diff != "",
			"same-identity invalid output was mistaken for admitted ownership (-want +got): %s",
			diff,
		)
	}
	c.emit(t.Context(), root.Named(), &store.Emission{Complete: true}, nil)
	_, _, retained = s.GetEmission(root.Named())
	assert.FatalIf(
		t,
		len(retained.Manifests) != 0 || len(retained.Children) != 0,
		"%v",
		"complete empty retained unbound invalid output",
	)
}

func TestEmit_DefaultsNamespace(t *testing.T) {
	t.Parallel()
	for _, kind := range []manifest.KindInfo{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease}} {
		for _, fallback := range []string{"", "apps"} {
			t.Run(kind.Kind+"/default="+fallback, func(t *testing.T) {
				t.Parallel()
				testEmitDefaultsNamespaceCase(t, testEmitDefaultsNamespaceCaseInputs{kind: &kind, fallback: &fallback})
			},
			)
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
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
		},
	}
	release := &manifest.HelmRelease{
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind: manifest.KindHelmChart,
			Name: "chart",
		},
		Name:      "release",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Namespace: "apps",
				Name:      "chart",
			},
		},
	}
	for _, obj := range []manifest.BaseManifest{producer, chart, consumer, release} {
		s.AddObject(obj)
		s.UpdateStatus(
			obj.Named(),
			store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"},
		)
	}
	foreignProducer := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       manifest.KindKustomization,
		Name:       producer.Name,
		Namespace:  producer.Namespace,
	}
	s.AddObject(foreignProducer)
	s.UpdateStatus(
		foreignProducer.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "foreign unchanged"},
	)
	foreignSettings := settings
	foreignSettings.Group = "example.test"
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			parent: {File: "parent.yaml"},
			settings: {
				Productions: []manifest.Production{
					{
						Owner:    producer.Named(),
						Authored: settings,
						Path:     "/fixtures/settings.yaml",
						File:     "settings.yaml",
						Source:   manifest.BootstrapSourceID,
					},
				},
			},
			foreignSettings: {
				Productions: []manifest.Production{
					{
						Owner:    foreignProducer.Named(),
						Authored: foreignSettings,
						Path:     "/fixtures/foreign-settings.yaml",
						File:     "foreign-settings.yaml",
						Source:   manifest.BootstrapSourceID,
					},
				},
			},
		},
	}
	admissionLoader1 := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{"parent.yaml"}), Index: index, Inventory: s, Root: ""},
	)
	admissiontest.Commit(t, admissionLoader1, nil)
	c.Configure(Options{Filter: admissionLoader1.Options.Filter, Index: index, Loader: admissionLoader1})

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

	c.KeepEmitted(t.Context(), parent, store.Child{Obj: consumer}, nil)
	c.KeepEmitted(t.Context(), parent, store.Child{Obj: release}, nil)

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
		want := store.StatusReady
		if tc.included {
			want = store.StatusPending
		}
		if info, _ := s.GetStatus(tc.obj.Named()); info.Status != want {
			t.Errorf("%s status = %+v, want %s", tc.obj.Named(), info, want)
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
		&manifest.HelmRelease{ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"}, Name: "child"},
		&manifest.ResourceSet{Name: "child"},
		&manifest.ResourceSetInputProvider{Name: "child"},
		&manifest.ConfigMap{Name: "child"},
		&manifest.Secret{Name: "child"},
	} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			testKeepEmittedInvalidatesCapturedExclusionCase2(
				t,
				testKeepEmittedInvalidatesCapturedExclusionCase2Inputs{parent: &parent, obj: &obj},
			)
		},
		)
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
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindConfigMap,
		"metadata":   map[string]any{"name": "output", "namespace": "apps"},
		"data":       map[string]any{"value": "one"},
	}
	parseChildrenForTestResult9 := parseChildrenForTest(
		parent.Named(),
		[]map[string]any{doc},
		manifest.ParseDocOptions{},
	)
	children, retained := parseChildrenForTestResult9.Children, parseChildrenForTestResult9.Documents
	initial.Children, initial.Manifests = children, testutil.DocumentOccurrences(retained)
	initial.Fingerprint, initial.Complete = manifest.Fingerprint("rendered input"), true
	first.Emit(t.Context(), parent.Named(), &initial, nil)
	initial.Finish()
	assert.FatalIf(
		t,
		!s.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission).OutputReads.Valid(),
		"%v",
		"initial output is not valid",
	)
	changed := manifest.DeepCopyMap(doc)
	changed["data"].(map[string]any)["value"] = "two"
	_, _, next := s.GetEmission(parent.Named())
	parseChildrenForTestResult10 := parseChildrenForTest(
		parent.Named(),
		[]map[string]any{changed},
		manifest.ParseDocOptions{},
	)
	children, retained = parseChildrenForTestResult10.Children, parseChildrenForTestResult10.Documents
	next.Children, next.Manifests = children, testutil.DocumentOccurrences(retained)
	next.Complete = true
	c.emit(t.Context(), parent.Named(), &next, nil)
	next.Finish()
	accepted := s.GetArtifact(parent.Named(), store.ArtifactContent).(*store.Emission)
	if accepted.Fingerprint != "" {
		t.Errorf("changed complete output inherited key %q", accepted.Fingerprint)
	}
	{
		_, found := accepted.Replay(initial.Fingerprint)
		assert.ErrorIf(t, found, "%v", "changed complete output retained a false render mapping")
	}
	{
		diff := cmp.Diff([]map[string]any{changed}, testutil.DocumentMaps(accepted.Manifests))
		assert.FatalIf(t, diff != "", "changed payload was not published (-want +got): %s", diff)
	}
	second := newExecution(c, parent.Named())
	defer second.Close()
	{
		fingerprintDedupResult11, fingerprintDedupResult11Err := second.FingerprintDedup(
			t.Context(),
			parent.Named(),
			"rendered input",
			&store.Emission{},
		)
		_, handled, err := fingerprintDedupResult11.Fingerprint, fingerprintDedupResult11.Reused, fingerprintDedupResult11Err
		assert.FatalIf(t, handled ||
			err != nil, "old render input reused changed bytes: handled=%t error=%v", handled, err)
	}
}

func TestExecution_ReplacementReclaimsBorrowedOutput(t *testing.T) {
	t.Parallel()
	for _, replacement := range []bool{false, true} {
		t.Run(strconv.FormatBool(replacement), func(t *testing.T) {
			t.Parallel()
			testExecutionReplacementReclaimsBorrowedOutputCase(
				t,
				&replacement,
			)
		},
		)
	}
}

type testEmitDefaultsNamespaceCaseInputs struct {
	kind     *manifest.KindInfo
	fallback *string
}

func testEmitDefaultsNamespaceCase(t *testing.T, testCaseInput testEmitDefaultsNamespaceCaseInputs) {
	t.Helper()

	s := store.New()
	c := New(s, nil)
	parent := manifest.NamedResource{
		Group:     testCaseInput.kind.Group,
		Kind:      testCaseInput.kind.Kind,
		Namespace: "parent",
		Name:      "infra",
	}
	docs := []map[string]any{
		{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "OCIRepository",
			"metadata":   map[string]any{"name": "chart"},
			"spec":       map[string]any{"url": "oci://example.test/chart"},
		},
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cm", "namespace": "release"},
		},
		{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "app"}},
		{
			"apiVersion": "rbac.authorization.k8s.io/v1",
			"kind":       "ClusterRole",
			"metadata":   map[string]any{"name": "role"},
		},
		{
			"apiVersion": "foreign.example/v1",
			"kind":       "ClusterRole",
			"metadata":   map[string]any{"name": "foreign"},
		},
	}
	before := make([]map[string]any, len(docs))
	for i, doc := range docs {
		before[i] = manifest.DeepCopyMap(doc)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: (*testCaseInput.fallback)}
	s.AddObject(
		&manifest.RawObject{
			APIVersion: parent.Group + "/v1",
			Kind:       parent.Kind,
			Name:       parent.Name,
			Namespace:  parent.Namespace,
		},
	)
	parseChildrenForTestResult12 := parseChildrenForTest(parent, docs, opts)
	children, retained := parseChildrenForTestResult12.Children, parseChildrenForTestResult12.Documents
	c.emit(
		t.Context(),
		parent,
		&store.Emission{
			Children:  children,
			Manifests: testutil.DocumentOccurrences(retained),
			Complete:  true,
		},
		nil,
	)
	assert.Equal(t, len(children), len(docs))
	for i, want := range []string{(*testCaseInput.fallback), "release", (*testCaseInput.fallback), "", (*testCaseInput.fallback)} {
		assert.Equal(t, children[i].Obj.Named().Namespace, want)
		assert.ErrorIf(t, s.GetObject(children[i].Obj.Named()) == nil, "child %d was not published", i)
	}
	assert.Diff(t, docs, before)
	if (*testCaseInput.fallback) != "" {
		for _, i := range []int{0, 2, 4} {
			before[i]["metadata"].(map[string]any)["namespace"] = (*testCaseInput.fallback)
		}
	}
	assert.Diff(t, retained, before)
	parseChildrenForTestResult13 := parseChildrenForTest(parent, retained, opts)
	replayed, replay := parseChildrenForTestResult13.Children, parseChildrenForTestResult13.Documents
	c.emit(
		t.Context(),
		parent,
		&store.Emission{
			Children:  replayed,
			Manifests: testutil.DocumentOccurrences(replay),
			Complete:  true,
		},
		nil,
	)
	for i := range children {
		assert.Equal(t, replayed[i].Obj.Named(), children[i].Obj.Named())
	}
	assert.Diff(t, replay, before)
}

type testKeepEmittedInvalidatesCapturedExclusionCaseInputs struct {
	parent *manifest.NamedResource
	obj    *manifest.BaseManifest
}

func testKeepEmittedInvalidatesCapturedExclusionCase(
	t *testing.T,
	testCaseInput testKeepEmittedInvalidatesCapturedExclusionCaseInputs,
) {
	t.Helper()

	s := store.New()
	c := New(s, nil)
	obj := (*testCaseInput.obj).WithNamespace("apps")
	s.AddObject(obj)
	s.UpdateStatus(
		obj.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"},
	)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{(*testCaseInput.parent): {File: "parent.yaml"}},
	}
	admissionLoader2 := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"parent.yaml"}),
			Index:     index,
			Inventory: s,
			Root:      "",
		},
	)
	admissiontest.Commit(t, admissionLoader2, nil)
	c.Configure(Options{Filter: admissionLoader2.Options.Filter, Index: index, Loader: admissionLoader2})
	s.SetIncluded(obj.Named(), false)
	execution := newExecution(c, obj.Named())
	t.Cleanup(execution.Close)
	assert.FatalIf(t, execution.Reads.Admitted(), "%v", "child admitted before publication")
	c.KeepEmitted(t.Context(), (*testCaseInput.parent), store.Child{Obj: obj}, nil)
	assert.Equal(t, execution.Reads.Valid(), false)
	next := newExecution(c, obj.Named())
	t.Cleanup(next.Close)
	assert.Equal(t, next.Reads.Admitted(), true)
	info, _ := s.GetStatus(obj.Named())
	assert.Equal(t, info.Status, store.StatusPending)
	assert.Equal(t, info.Outcome(), store.OutcomePending)
	assert.Equal(t, info.Message, "")
}

func testExecutionReplacementReclaimsBorrowedOutputCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	declaration := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "old"}
	st.AddObject(parent)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	publish := func(obj *manifest.Kustomization) {
		st.RecordEmitted(
			parent.Named(),
			&store.Emission{Children: []store.Child{{Obj: obj}}, Complete: true},
			nil,
		)
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
		attempt.Children = []store.Child{
			{Obj: &manifest.ConfigMap{Name: "output", Namespace: "apps", Data: map[string]any{"value": "old"}}},
		}
		attempt.Complete = true
		first.Emit(t.Context(), declaration.Named(), &attempt, nil)
		attempt.Finish()
		first.Reads.Accept()
		first.Retire()
		if output, ok := st.GetArtifact(declaration.Named(), store.ArtifactContent).(*store.Emission); !ok ||
			output.OutputReads != &first.Reads {
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
		if *testCaseInput {
			name = "replacement"
		}
		attempt.Children = []store.Child{
			{Obj: &manifest.ConfigMap{Name: name, Namespace: "apps", Data: map[string]any{"value": "new"}}},
		}
		attempt.Complete = true
		current.Emit(t.Context(), declaration.Named(), &attempt, nil)
		attempt.Finish()
		current.Reads.Accept()
	})
	tasks.BlockTillDone()
	assert.FatalIf(t, tasks.Failures() != 0 || current == nil, "%v", "replacement did not complete")
	defer current.Close()
	for range st.Sweep() {
	}
	runtime.GC()
	assert.FatalIf(
		t,
		previous.Value() != nil,
		"%v",
		"accepted replacement retained predecessor registration storage",
	)
	assert.FatalIf(t, !current.Valid(), "%v", "replacement retired current output provenance")
	runtime.KeepAlive(st)
	runtime.KeepAlive(current)
}

type testKeepEmittedInvalidatesCapturedExclusionCase2Inputs struct {
	parent *manifest.NamedResource
	obj    *manifest.BaseManifest
}

func testKeepEmittedInvalidatesCapturedExclusionCase2(
	t *testing.T,
	testCaseInput testKeepEmittedInvalidatesCapturedExclusionCase2Inputs,
) {
	t.Helper()

	testKeepEmittedInvalidatesCapturedExclusionCase(
		t,
		testKeepEmittedInvalidatesCapturedExclusionCaseInputs(testCaseInput),
	)
}
