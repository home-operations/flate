package base

import (
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"

	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize" // Standalone controller tests MUST install the process scope owner before parsing.
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestEmit_InvalidReads(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "A"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "A"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}},
	}
	filterLoader, _ := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{"parent.yaml"}), Index: index, Inventory: s, Root: ""},
	)
	admissiontest.Commit(t, filterLoader.Options.Session, nil)
	filter := filterLoader.Options.Session
	c.Configure(Options{Session: filter, Index: index, Loader: filterLoader})
	run := newExecution(c, parent.Named())
	t.Cleanup(run.Close)
	_, _, next := run.Reads.GetEmission(parent.Named())
	defer next.Finish()
	next.Children, next.Complete = []store.Child{{Obj: child}}, true
	run.Reads.Bind(&next)
	replacement := parent.Clone()
	replacement.Path = "B"
	s.AddObject(replacement)
	assert.FatalIf(t, run.Reads.Valid(), "%v", "changed parent left captured reads valid")
	assert.FatalIf(t, !filter.Snapshot().Reconcile(parent.Named()) || filter.Snapshot().Reconcile(child.Named()) ||
		s.GetObject(child.Named()) != nil ||
		s.HasInventory(parent.Named()), "%v", "invalid emission fixture already selected or published the child")
	(&Execution{Controller: c}).emit(t.Context(), parent.Named(), &next, nil)
	assert.ErrorIf(t, filter.Snapshot().Reconcile(child.Named()), "%v", "invalid reads extended changed-only selection")
	assert.ErrorIf(
		t,
		s.GetObject(child.Named()) != nil || s.GetDeclaration(child.Named()) != nil ||
			s.HasInventory(parent.Named()),
		"%v",
		"invalid reads published a child or parent inventory",
	)
}

func TestEmit_InputReplacementSelection(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := New(s, nil)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "A"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "A"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}},
	}
	filterLoader, _ := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{"parent.yaml"}), Index: index, Inventory: s, Root: ""},
	)
	admissiontest.Commit(t, filterLoader.Options.Session, nil)
	filter := filterLoader.Options.Session
	c.Configure(Options{Session: filter, Index: index, Loader: filterLoader})
	run := newExecution(c, parent.Named())
	_, _, captured := run.Reads.GetEmission(parent.Named())
	next := &captured
	next.Children, next.Complete = []store.Child{{Obj: child}}, true
	replacement := parent.Clone()
	replacement.Path = "B"
	s.AddObject(replacement)
	run.Emit(t.Context(), parent.Named(), next, nil)
	assert.FatalIf(t, run.Reads.Valid(), "%v", "stale input succeeded")
	assert.FatalIf(
		t,
		filter.Snapshot().Reconcile(child.Named()) || s.GetObject(child.Named()) != nil,
		"%v",
		"stale input extended selection",
	)
	accepted := child.Clone()
	accepted.Path = "B"
	fresh := captureEmission(
		s,
		parent.Named(),
		&store.Emission{Children: []store.Child{{Obj: accepted}}, Complete: true},
	)
	newExecution(c, parent.Named()).Emit(t.Context(), parent.Named(), fresh, nil)
	assert.FatalIf(
		t,
		!filter.Snapshot().Reconcile(accepted.Named()) || s.GetDeclaration(accepted.Named()) != accepted,
		"%v",
		"ordinary replacement did not publish and extend selection",
	)
}

func TestFingerprintDedup_AcceptedReceipt(t *testing.T) {
	t.Parallel()
	for _, branch := range []string{"current", "borrowed"} {
		for _, receipt := range []string{"previous", "current", "invalid_input", "aba_input", "retired_input"} {
			t.Run(branch+"/"+receipt, func(t *testing.T) {
				t.Parallel()
				testFingerprintDedupAcceptedReceiptCase(
					t,
					testFingerprintDedupAcceptedReceiptCaseInputs{branch: &branch, receipt: &receipt},
				)
			},
			)
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
			next := store.Emission{
				Complete: true,
				Render:   manifest.NewRenderReceipt("accepted", func(a, b any) bool { return a == b }),
			}
			run.Emit(t.Context(), owner.Named(), &next, nil)
			assert.FatalIf(t, next.Footprint() == nil, "%v", "fixture did not accept complete-empty output")
			before := st.GetArtifact(owner.Named(), store.ArtifactContent)
			st.AddObject(
				&manifest.ConfigMap{
					Name:      dependency.Name,
					Namespace: dependency.Namespace,
					Data:      map[string]any{"value": "changed"},
				},
			)
			if caller == "store" {
				st.RecordEmitted(owner.Named(), &next, nil)
			} else {
				(&Execution{Controller: controller}).emit(t.Context(), owner.Named(), &next, nil)
			}
			assert.FatalIf(
				t,
				next.Footprint() != nil || st.GetArtifact(owner.Named(), store.ArtifactContent) != before,
				"%v",
				"early rejection retained caller acceptance or replaced inventory",
			)
		})
	}
}

func TestEmit_FailedAuthorityReplacesSuccess(t *testing.T) {
	t.Parallel()
	for _, branch := range []string{"current", "borrowed"} {
		t.Run(branch, func(t *testing.T) {
			t.Parallel()
			testEmitFailedAuthorityReplacesSuccessCase(
				t,
				&branch,
			)
		},
		)
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
			sibling := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "healthy", "namespace": "apps"},
			}
			failure := manifest.NewError(manifest.ErrInput, nil, "normalize helm list")
			c := Controller{SkipKinds: tc.skipKinds}
			parseChildrenResult1 := (&Execution{Controller: &c}).ParseChildren(
				owner,
				[]manifest.DocOccurrence{
					{Document: invalid, Ordinal: 2, Err: failure},
					{Document: sibling, Ordinal: 3},
				},
				manifest.ParseDocOptions{},
			)
			children, docs, failures := parseChildrenResult1.Children, parseChildrenResult1.Visible, parseChildrenResult1.Failures
			assert.FatalIf(
				t,
				len(children) != 1 || len(docs) != tc.wantDocs || len(failures) != 1 || failures[0].Ordinal != 2 ||
					!errors.Is(
						failures[0].Err,
						failure,
					),
				"renderer failure was dropped: children=%v docs=%v failures=%v",
				children,
				docs,
				failures,
			)
			assert.Equal(t, children[0].Obj.Named().Name, "healthy")
			assert.Diff(t, docs[len(docs)-1].Document, sibling)
			if tc.wantDocs == 2 {
				assert.Diff(t, docs[0].Document, invalid)
			}
		})
	}
}

type testFingerprintDedupAcceptedReceiptCaseInputs struct {
	branch  *string
	receipt *string
}

func testFingerprintDedupAcceptedReceiptCase(
	t *testing.T,
	testCaseInput testFingerprintDedupAcceptedReceiptCaseInputs,
) {
	t.Helper()

	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	dependency := &manifest.ConfigMap{
		Name:      "input",
		Namespace: "apps",
		Data:      map[string]any{"value": "first"},
	}
	st.AddObjects([]manifest.BaseManifest{owner, dependency})
	controller := New(st, nil)
	first := newExecution(controller, owner.Named())
	defer first.Retire()
	first.Reads.GetObject(dependency.Named())
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "output", "namespace": "apps"},
	}
	parsed := (&Execution{Controller: controller}).ParseChildren(
		owner,
		testutil.DocumentOccurrences([]map[string]any{doc}),
		manifest.ParseDocOptions{},
	)
	if failure := manifest.DocumentFailure(parsed.Failures); failure != nil {
		panic(failure)
	}
	children, docs := parsed.Children, testutil.DocumentMaps(parsed.Visible)
	next := store.Emission{
		Children:    children,
		Manifests:   testutil.DocumentOccurrences(docs),
		Fingerprint: manifest.Fingerprint("content"),
		Complete:    true,
		Render:      manifest.NewRenderReceipt("previous", func(a, b any) bool { return a == b }),
	}
	first.Emit(t.Context(), owner.Named(), &next, nil)
	if (*testCaseInput.branch) == "borrowed" {
		progress := newExecution(controller, owner.Named())
		defer progress.Retire()
		partialDoc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "progress", "namespace": "apps"},
		}
		parseChildrenResult2 := (&Execution{Controller: controller}).ParseChildren(
			owner,
			testutil.DocumentOccurrences([]map[string]any{partialDoc}),
			manifest.ParseDocOptions{},
		)
		partialChildren, partialDocs, partialFailures := parseChildrenResult2.Children, parseChildrenResult2.Visible, parseChildrenResult2.Failures
		partial := store.Emission{
			Children:  partialChildren,
			Manifests: partialDocs,
			Failures:  partialFailures,
		}
		progress.Emit(t.Context(), owner.Named(), &partial, nil)
		assert.FatalIf(
			t,
			st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission).Complete,
			"%v",
			"fixture did not preserve a borrowed complete predecessor",
		)
	}
	if (*testCaseInput.receipt) == "retired_input" {
		first.Reads.Retire()
		for range st.Sweep() {
		}
	}
	if (*testCaseInput.receipt) == "invalid_input" || (*testCaseInput.receipt) == "aba_input" ||
		(*testCaseInput.receipt) == "retired_input" {
		st.AddObject(
			&manifest.ConfigMap{
				Name:      dependency.Name,
				Namespace: dependency.Namespace,
				Data:      map[string]any{"value": "changed"},
			},
		)
	}
	if (*testCaseInput.receipt) == "aba_input" {
		st.AddObject(dependency)
	}
	assert.FatalIf(
		t, (*testCaseInput.receipt) ==
			"retired_input" && !first.Reads.Valid(), "%v",
		"fixture did not retain an immutable projection without active subscriptions",
	)
	second := newExecution(controller, owner.Named())
	defer second.Retire()
	attempt := store.Emission{}
	want := "previous"
	switch *testCaseInput.receipt {
	case "current":
		attempt.Render = manifest.NewRenderReceipt("current", func(a, b any) bool { return a == b })
		want = "current"
	case "invalid_input", "aba_input", "retired_input":
		want = ""
	}
	fingerprintDedupResult3, fingerprintDedupResult3Err := second.FingerprintDedup(
		t.Context(),
		owner.Named(),
		"content",
		&attempt,
	)
	_, handled, err := fingerprintDedupResult3.Fingerprint, fingerprintDedupResult3.Reused, fingerprintDedupResult3Err
	assert.NoError(t, err)
	assert.Equal(t, handled, true)
	var expected *manifest.RenderReceipt
	if want != "" {
		expected = manifest.NewRenderReceipt(want, func(a, b any) bool { return a == b })
	}
	assert.Diff(t, attempt.Render, expected)
	assert.Equal(t, attempt.Reads, &second.Reads)
	assert.Diff(t, st.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission).Render, expected)
	assert.FatalIf(
		t,
		attempt.Footprint() == nil || !attempt.Complete,
		"%v",
		"replay did not hand back exact complete acceptance",
	)
	ids := []manifest.NamedResource{}
	for id := range attempt.Footprint() {
		ids = append(ids, id)
	}
	assert.Diff(t, ids, []manifest.NamedResource{children[0].Obj.Named()})
}

func testEmitFailedAuthorityReplacesSuccessCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	c := New(s, task.NewBounded(2))
	valid := map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2",
		"kind":       "HelmRelease",
		"metadata":   map[string]any{"name": "child", "namespace": "apps"},
		"spec": map[string]any{
			"interval": "10m",
			"chart": map[string]any{
				"spec": map[string]any{
					"chart":     "demo",
					"sourceRef": map[string]any{"kind": "HelmRepository", "name": "charts"},
				},
			},
		},
	}
	first := newExecution(c, owner.Named())
	defer first.Close()
	parseChildrenResult4 := (&Execution{Controller: c}).ParseChildren(
		owner,
		testutil.DocumentOccurrences([]map[string]any{valid}),
		manifest.ParseDocOptions{},
	)
	children, docs, failures := parseChildrenResult4.Children, parseChildrenResult4.Visible, parseChildrenResult4.Failures
	attempt := store.Emission{
		Children:    children,
		Manifests:   docs,
		Failures:    failures,
		Complete:    true,
		Fingerprint: manifest.Fingerprint("content"),
	}
	{
		err := first.Emit(t.Context(), owner.Named(), &attempt, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	first.finish(t.Context(), nil)
	childID := manifest.DocIdentity(valid)
	assert.FatalIf(t, s.GetObject(childID) == nil, "%v", "valid predecessor missing")
	if (*testCaseInput) == "borrowed" {
		progress := newExecution(c, owner.Named())
		defer progress.Close()
		{
			err := progress.Emit(t.Context(), owner.Named(), &store.Emission{}, nil)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	invalid := manifest.DeepCopyMap(valid)
	invalid["spec"].(map[string]any)["interval"] = []any{}
	sibling := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "healthy", "namespace": "apps"},
	}
	var diagnostic string
	closeRuns := make([]func(), 0, 2)
	defer func() {
		for _, closeRun := range slices.Backward(closeRuns) {
			closeRun()
		}
	}()
	for _, phase := range []string{"failure", "repeat"} {
		run := newExecution(c, owner.Named())
		closeRuns = append(closeRuns, run.Close)
		outcomes := []manifest.DocOccurrence{{Document: invalid, Ordinal: 3}, {Document: sibling, Ordinal: 4}}
		if phase == "repeat" {
			data, err := manifest.EncodeRender(outcomes)
			assert.FatalIf(t, err != nil, "%v", err)
			outcomes, err = manifest.DecodeRender(data)
			assert.FatalIf(t, err != nil, "%v", err)
		}
		parseChildrenResult5 := (&Execution{Controller: c}).ParseChildren(owner, outcomes, manifest.ParseDocOptions{})
		children, docs, failures = parseChildrenResult5.Children, parseChildrenResult5.Visible, parseChildrenResult5.Failures
		attempt = store.Emission{
			Children:    children,
			Manifests:   docs,
			Failures:    failures,
			Complete:    true,
			Fingerprint: manifest.Fingerprint("content"),
		}
		err := run.Emit(t.Context(), owner.Named(), &attempt, nil)
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "%s accepted invalid output: %v", phase, err)
		run.finish(t.Context(), err)
		if diagnostic == "" {
			diagnostic = err.Error()
		}
		assert.Equal(t, err.Error(), diagnostic)
		current := s.GetArtifact(owner.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(t, current.Complete || attempt.Footprint() != nil || len(current.Failures) != 1 ||
			len(current.Manifests) != 2, "failed inventory: %+v", current)
		{
			_, hit := current.Replay(manifest.Fingerprint("content"))
			assert.FatalIf(t, hit, "%v", "failure replayed predecessor success")
		}
		assert.FatalIf(
			t,
			s.GetObject(childID) != nil || s.GetObject(manifest.DocIdentity(sibling)) == nil,
			"%v",
			"failed authority restored old child or lost valid sibling",
		)
		info, _ := s.GetStatus(owner.Named())
		assert.Equal(t, info.Status, store.StatusFailed)
	}
	corrected := newExecution(c, owner.Named())
	defer corrected.Close()
	var next store.Emission
	fingerprintDedupResult6, fingerprintDedupResult6Err := corrected.FingerprintDedup(
		t.Context(),
		owner.Named(),
		"content",
		&next,
	)
	_, hit, err := fingerprintDedupResult6.Fingerprint, fingerprintDedupResult6.Reused, fingerprintDedupResult6Err
	assert.FatalIf(t, hit || err != nil, "failed fingerprint bypassed admission: %t %v", hit, err)
	parseChildrenResult7 := (&Execution{Controller: c}).ParseChildren(
		owner,
		testutil.DocumentOccurrences([]map[string]any{valid}),
		manifest.ParseDocOptions{},
	)
	next.Children, next.Manifests, next.Failures = parseChildrenResult7.Children, parseChildrenResult7.Visible, parseChildrenResult7.Failures
	next.Complete, next.Fingerprint = true, manifest.Fingerprint("content")
	{
		err := corrected.Emit(t.Context(), owner.Named(), &next, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	corrected.finish(t.Context(), nil)
	info, _ := s.GetStatus(owner.Named())
	assert.FatalIf(
		t,
		info.Status != store.StatusReady || s.GetObject(childID) == nil || next.Footprint() == nil,
		"correction did not recover: %+v",
		info,
	)
}
