package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSelect_ErrorAggregationRetainsSharedTeardown(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("shared cache close")
	inputErr := manifest.NewError(manifest.ErrInput, nil, "independent input")
	domainErr := manifest.NewError(manifest.ErrFlux, nil, "independent domain")
	failed := selectedRun(t,
		&Result{
			Failed: map[manifest.NamedResource]store.StatusInfo{
				testutil.KustomizationID("flux-system", "failed"): {
					Status:  store.StatusFailed,
					Message: "resource failed",
				},
			},
		},
	)
	failed.Err = errors.Join(&selection.FailuresError{Message: "raw failures"}, context.Canceled)
	t.Run("head_only", func(t *testing.T) {
		t.Parallel()
		testSelectErrorAggregationRetainsSharedTeardownHeadOnly(
			t,
			testSelectErrorAggregationRetainsSharedTeardownHeadOnlyInputs{
				closeErr: &closeErr,
				inputErr: &inputErr,
				failed:   &failed,
			},
		)
	},
	)
	for _, tc := range []struct {
		name                  string
		orig, current, shared error
		want                  string
	}{
		{name: "clean"},
		{name: "orig", orig: failed.Err, want: "orig snapshot:"},
		{name: "current", current: failed.Err, want: "current snapshot:"},
		{name: "both", orig: failed.Err, current: failed.Err, want: "both snapshots"},
		{name: "close", shared: closeErr},
		{name: "joined close", shared: errors.Join(errors.Join(nil, nil), closeErr)},
		{name: "composed", orig: failed.Err, shared: closeErr, want: "orig snapshot:"},
		{name: "orig input sibling", orig: errors.Join(failed.Err, inputErr), want: "orig snapshot:"},
		{name: "current domain sibling", current: errors.Join(failed.Err, domainErr), want: "current snapshot:"},
		{name: "shared domain siblings", orig: failed.Err, shared: errors.Join(inputErr, domainErr, closeErr), want: "orig snapshot:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectErrorAggregationRetainsSharedTeardownCase(
				t,
				testSelectErrorAggregationRetainsSharedTeardownCaseInputs{
					closeErr:  &closeErr,
					inputErr:  &inputErr,
					domainErr: &domainErr,
					failed:    &failed,
					tc:        &tc,
				},
			)
		},
		)
	}
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested tree reversed=%t", reversed), func(t *testing.T) {
			t.Parallel()
			testSelectErrorAggregationRetainsSharedTeardownCase2(
				t,
				testSelectErrorAggregationRetainsSharedTeardownCase2Inputs{failed: &failed, reversed: &reversed},
			)
		},
		)
	}
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			t.Parallel()
			testSelectErrorAggregationRetainsSharedTeardownCase3(
				t,
				testSelectErrorAggregationRetainsSharedTeardownCase3Inputs{closeErr: &closeErr, order: &order},
			)
		},
		)
	}
}

func TestSelect_UnavailabilityUsesLocalCanonicalEvidence(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("flux-system", "parent")
	child := testutil.HelmReleaseID("apps", "child")
	for _, tc := range []struct {
		disposition store.StatusInfo
		unavailable bool
	}{
		{disposition: store.StatusInfo{Status: store.StatusFailed, Message: "parent failed"}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "absent source"}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"}},
		{disposition: store.StatusInfo{Status: store.StatusReady}},
		{disposition: store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Message: "skipped: absent source"}},
		{disposition: store.StatusInfo{Status: store.StatusReady, Message: "suspended"}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: "fetching"}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: "skipped: absent source"}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: "suspended"}},
	} {
		disposition := tc.disposition
		for _, reverse := range []bool{false, true} {
			t.Run(
				fmt.Sprintf("%s_%s_reverse_%t", disposition.Status, disposition.String(), reverse),
				func(t *testing.T) {
					t.Parallel()
					testSelectUnavailabilityUsesLocalCanonicalEvidenceCase(
						t,
						testSelectUnavailabilityUsesLocalCanonicalEvidenceCaseInputs{
							parent:      &parent,
							child:       &child,
							tc:          &tc,
							disposition: &disposition,
							reverse:     &reverse,
						},
					)
				},
			)
		}
	}
	t.Run("entry correspondence", testEntryCorrespondence)
}

func TestSelect_RequiredFailureOutsideOutputNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		required, paired bool
		namespace        string
		full             bool
	}{
		{name: "excluded"},
		{name: "required", required: true},
		{name: "required with other side", required: true, paired: true},
		{name: "namespace excluded", namespace: "apps"},
		{name: "namespace required", namespace: "apps", required: true},
		{name: "namespace required with other side", namespace: "apps", required: true, paired: true},
		{name: "namespace required full render", namespace: "apps", required: true, full: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectRequiredFailureOutsideOutputNamespaceCase(
				t,
				&tc,
			)
		},
		)
	}
	t.Run("unscoped required descendant", func(t *testing.T) {
		t.Parallel()
		testSelectRequiredFailureOutsideOutputNamespaceUnscopedRequiredDescendant(t)
	},
	)
}

func TestSelect_ChangedParentExcludesUnselectedFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		selected bool
		named    bool
	}{
		{name: "needed child"},
		{name: "selected child", selected: true},
		{name: "named parent", named: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
			run := selectedRun(t, &Result{Failed: map[manifest.NamedResource]store.StatusInfo{
				child.Named(): {Status: store.StatusFailed, Message: "child failed"},
			}})
			run.store.AddObject(parent)
			run.store.AddObject(child)
			publishFixture(t, run.store, parent.Named(), []manifest.NamedResource{child.Named()})
			selected := []manifest.NamedResource{parent.Named()}
			if tc.selected {
				selected = append(selected, child.Named())
			}
			if !tc.selected && !tc.named {
				run.index.Records[parent.Named()] = manifest.DiscoveryRecord{File: "parent.yaml"}
				run.index.Records[child.Named()] = manifest.DiscoveryRecord{File: "child.yaml", Declaration: child}
				run.cfg.ExternalChanges = change.NewSet([]string{"parent.yaml"})
			}
			retry := prepareSelectionForTest(t, &run, []manifest.NamedResource{child.Named()}, selected)
			assert.Equal(t, retry, false)
			name := ""
			if tc.named {
				name = parent.Name
			}
			run.Err = &selection.FailuresError{Message: "child failed"}
			projectedResults0, projectionErrors0 := Select(
				[]Rendered{run},
				run.Err,
				[]manifest.NamedResource{testutil.KustomizationID("", name), testutil.HelmReleaseID("", name)},
				nil,
			)
			result, err, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
			assert.Equal(t, err != nil, tc.selected || tc.named)
			assert.NoError(t, remainder)
			_, failed := result[0].Failed[child.Named()]
			assert.Equal(t, failed, tc.selected || tc.named)
		})
	}
	t.Run("namespace selected source descendant", func(t *testing.T) {
		t.Parallel()
		parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
		child := &manifest.GitRepository{Name: "source", Namespace: "apps"}
		failure := store.StatusInfo{Status: store.StatusFailed, Message: "source failed"}
		run := selectedRun(t, &Result{Failed: map[manifest.NamedResource]store.StatusInfo{child.Named(): failure}})
		run.store.AddObjects([]manifest.BaseManifest{parent, child})
		publishFixture(t, run.store, parent.Named(), []manifest.NamedResource{child.Named()})
		retry := prepareSelectionForTest(t, &run, nil, []manifest.NamedResource{parent.Named(), child.Named()})
		assert.Equal(t, retry, false)
		roots := []manifest.NamedResource{testutil.KustomizationID("apps", "")}
		assert.Equal(t, (selection.Request{Roots: roots}).Matches(child.Named()), false)
		membershipResult1 := run.selection.Snapshot()
		selectedChild := membershipResult1.Output(child.Named())
		assert.Equal(t, selectedChild, true)
		run.Err = &selection.FailuresError{Message: failure.Message}
		projectedResults1, projectionErrors1 := Select([]Rendered{run}, run.Err, roots, nil)
		result, err, remainder := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
		assert.Equal(t, err != nil, true)
		assert.NoError(t, remainder)
		assert.Equal(t, len(result[0].Failed), 1)
		assert.Equal(t, result[0].Failed[child.Named()].String(), failure.Message)
		assert.Diff(t, result[0].Causes, map[manifest.NamedResource][]manifest.NamedResource{child.Named(): nil})
	})
}

type testSelectErrorAggregationRetainsSharedTeardownHeadOnlyInputs struct {
	closeErr *error
	inputErr **manifest.Error
	failed   *Rendered
}

func testSelectErrorAggregationRetainsSharedTeardownHeadOnly(
	t *testing.T,
	testCaseInput testSelectErrorAggregationRetainsSharedTeardownHeadOnlyInputs,
) {
	t.Helper()

	raw := errors.Join(errors.Join((*testCaseInput.inputErr), testCaseInput.failed.Err), (*testCaseInput.closeErr))
	projectedResults2, projectionErrors2 := Select(
		[]Rendered{(*testCaseInput.failed)},
		raw,
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	selected, err, remainder := projectedResults2, projectionErrors2.Selected, projectionErrors2.Remaining
	assert.FatalIf(
		t,
		len(selected) != 1 || len(selected[0].Failed) != 1,
		"head-only selection lost partial failures: %+v %v",
		selected,
		err,
	)
	for _, got := range []error{err, remainder} {
		for _, cause := range []error{(*testCaseInput.inputErr), manifest.ErrInput, context.Canceled, (*testCaseInput.closeErr)} {
			assert.FatalIf(t, !errors.Is(got, cause) ||
				!errors.Is(cause, manifest.ErrInput) &&
					strings.Count(
						got.Error(),
						cause.Error(),
					) != 1, "head-only projection lost or duplicated %v: %v", cause, got)
		}
	}
	{
		_, ok := errors.AsType[*selection.FailuresError](err)
		assert.FatalIf(t, !ok, "%v", "selected failure identity lost")
	}
	{
		_, ok := errors.AsType[*selection.FailuresError](remainder)
		assert.FatalIf(t, ok, "resource failure entered remainder: %v", remainder)
	}
}

type testSelectErrorAggregationRetainsSharedTeardownCaseInputs struct {
	closeErr  *error
	inputErr  **manifest.Error
	domainErr **manifest.Error
	failed    *Rendered
	tc        *struct {
		name string

		orig    error
		current error
		shared  error
		want    string
	}
}

func testSelectErrorAggregationRetainsSharedTeardownCase(
	t *testing.T,
	testCaseInput testSelectErrorAggregationRetainsSharedTeardownCaseInputs,
) {
	t.Helper()

	runs := []Rendered{selectedRun(t, &Result{}), selectedRun(t, &Result{})}
	for i, err := range []error{testCaseInput.tc.orig, testCaseInput.tc.current} {
		if err != nil {
			runs[i] = (*testCaseInput.failed)
		}
		runs[i].Err = err
	}
	projectedResults3, projectionErrors3 := Select(
		runs,
		errors.Join(errors.Join(testCaseInput.tc.orig, testCaseInput.tc.current), testCaseInput.tc.shared),
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	_, err, remainder := projectedResults3, projectionErrors3.Selected, projectionErrors3.Remaining
	assert.FatalIf(
		t,
		(err != nil) != (testCaseInput.tc.orig != nil || testCaseInput.tc.current != nil || testCaseInput.tc.shared != nil) ||
			(testCaseInput.tc.want != "" && !strings.Contains(err.Error(), testCaseInput.tc.want)),
		"snapshot attribution changed: %v",
		err,
	)
	for _, cause := range []error{context.Canceled, (*testCaseInput.closeErr), (*testCaseInput.inputErr), (*testCaseInput.domainErr), manifest.ErrInput} {
		want := errors.Is(testCaseInput.tc.orig, cause) || errors.Is(testCaseInput.tc.current, cause) ||
			errors.Is(testCaseInput.tc.shared, cause)
		assert.FatalIf(
			t,
			errors.Is(err, cause) != want || errors.Is(remainder, cause) != want,
			"error identity lost: selected=%v remainder=%v",
			err,
			remainder,
		)
	}
	wantDomainRemainder := errors.Is(testCaseInput.tc.orig, (*testCaseInput.inputErr)) ||
		errors.Is(testCaseInput.tc.current, (*testCaseInput.domainErr)) ||
		errors.Is(testCaseInput.tc.shared, manifest.ErrFlux)
	assert.FatalIf(
		t,
		errors.Is(remainder, manifest.ErrFlux) != wantDomainRemainder,
		"incidental domain identity changed: %v",
		remainder,
	)
	{
		_, ok := errors.AsType[*selection.FailuresError](remainder)
		assert.FatalIf(t, ok, "resource failures entered remainder: %v", remainder)
	}
}

type testSelectErrorAggregationRetainsSharedTeardownCase2Inputs struct {
	failed   *Rendered
	reversed *bool
}

func testSelectErrorAggregationRetainsSharedTeardownCase2(
	t *testing.T,
	testCaseInput testSelectErrorAggregationRetainsSharedTeardownCase2Inputs,
) {
	t.Helper()

	teardownA, teardownB := errors.New("teardown A"), errors.New("teardown B")
	baseFailure := &selection.FailuresError{Message: "raw base failure"}
	runs := []Rendered{(*testCaseInput.failed), selectedRun(t, &Result{})}
	runs[0].Err, runs[1].Err = baseFailure, context.Canceled
	nested := fmt.Errorf("outer: %w", errors.Join(baseFailure, context.Canceled))
	children := []error{teardownA, nested, teardownB}
	if *testCaseInput.reversed {
		slices.Reverse(children)
	}
	projectedResults4, projectionErrors4 := Select(
		runs,
		errors.Join(children...),
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	selected, err, remainder := projectedResults4, projectionErrors4.Selected, projectionErrors4.Remaining
	want := "both snapshots had reconcile failures\norig snapshot: reconcile completed with 1 failure(s):\n  " + testutil.KustomizationID("flux-system", "failed").
		String() +
		": resource failed\ncurrent snapshot: context canceled"
	wantRemainder := "current snapshot: context canceled"
	if *testCaseInput.reversed {
		want = "teardown B\n" + want + "\nteardown A"
		wantRemainder = "teardown B\n" + wantRemainder + "\nteardown A"
	} else {
		want = "teardown A\n" + want + "\nteardown B"
		wantRemainder = "teardown A\n" + wantRemainder + "\nteardown B"
	}
	assert.FatalIf(
		t,
		len(selected) != 2 || err == nil || remainder == nil,
		"lost nested projection: selected=%v err=%v remainder=%v",
		selected,
		err,
		remainder,
	)
	assert.Equal(t, err.Error(), want)
	assert.Equal(t, remainder.Error(), wantRemainder)
	for _, got := range []error{err, remainder} {
		for _, cause := range []error{teardownA, teardownB, context.Canceled} {
			assert.FatalIf(t, !errors.Is(got, cause), "lost nested cause %v: %v", cause, got)
		}
		assert.FatalIf(t, errors.Is(got, baseFailure), "raw base failure entered projection: %v", got)
	}
	{
		_, ok := errors.AsType[*selection.FailuresError](err)
		assert.FatalIf(t, !ok, "%v", "selected failure identity lost")
	}
	{
		_, ok := errors.AsType[*selection.FailuresError](remainder)
		assert.FatalIf(t, ok, "%v", "resource failure entered remainder")
	}
}

type testSelectErrorAggregationRetainsSharedTeardownCase3Inputs struct {
	closeErr *error
	order    *[2]int
}

func testSelectErrorAggregationRetainsSharedTeardownCase3(
	t *testing.T,
	testCaseInput testSelectErrorAggregationRetainsSharedTeardownCase3Inputs,
) {
	t.Helper()

	causes := [2]error{errors.New("base operation"), errors.New("head operation")}
	runs := []Rendered{selectedRun(t, &Result{}), selectedRun(t, &Result{})}
	gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	finished := make(chan int)
	for i := range runs {
		go func() {
			<-gates[i]
			runs[i].Err = causes[i]
			finished <- i
		}()
	}
	for _, i := range *testCaseInput.order {
		close(gates[i])
		{
			got := <-finished
			assert.FatalIf(t, got != i, "completion = %d, want %d", got, i)
		}
	}
	raw := errors.Join(errors.Join(runs[0].Err, runs[1].Err), (*testCaseInput.closeErr))
	projectedResults5, projectionErrors5 := Select(
		runs,
		raw,
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	selected, err, remainder := projectedResults5, projectionErrors5.Selected, projectionErrors5.Remaining
	assert.FatalIf(t, len(selected) != 2, "lost partial sides: %v", selected)
	for _, got := range []error{raw, err, remainder} {
		previous := -1
		for _, cause := range []error{causes[0], causes[1], (*testCaseInput.closeErr)} {
			position := strings.Index(got.Error(), cause.Error())
			assert.FatalIf(t, !errors.Is(got, cause) ||
				!errors.Is(cause, manifest.ErrInput) && strings.Count(got.Error(), cause.Error()) != 1 ||
				position <= previous, "base/head/backing ordering or identity lost: %v", got)
			previous = position
		}
	}
}

type testSelectUnavailabilityUsesLocalCanonicalEvidenceCaseInputs struct {
	parent *manifest.NamedResource
	child  *manifest.NamedResource
	tc     *struct {
		disposition store.StatusInfo
		unavailable bool
	}
	disposition *store.StatusInfo
	reverse     *bool
}

func testSelectUnavailabilityUsesLocalCanonicalEvidenceCase(
	t *testing.T,
	testCaseInput testSelectUnavailabilityUsesLocalCanonicalEvidenceCaseInputs,
) {
	t.Helper()

	orig := selectedRun(t,
		&Result{
			Manifests: []manifest.DocumentBatch{
				{
					Parent: &manifest.HelmRelease{
						Name:      testCaseInput.child.Name,
						Namespace: testCaseInput.child.Namespace,
					},
					Documents: []map[string]any{{"kind": "Deployment"}},
				},
			},
		},
	)
	current := selectedRun(t, &Result{})
	orig.store.AddObject(
		&manifest.HelmRelease{Name: testCaseInput.child.Name, Namespace: testCaseInput.child.Namespace},
	)
	publishFixture(t, orig.store, (*testCaseInput.parent), []manifest.NamedResource{(*testCaseInput.child)})
	current.store.AddObject(
		&manifest.Kustomization{Name: testCaseInput.parent.Name, Namespace: testCaseInput.parent.Namespace},
	)
	current.store.UpdateStatus((*testCaseInput.parent), (*testCaseInput.disposition))
	if testCaseInput.disposition.Status == store.StatusFailed {
		canonical := (*testCaseInput.disposition)
		canonical.Message = "canonical parent failure"
		current.Result.Failed = map[manifest.NamedResource]store.StatusInfo{(*testCaseInput.parent): canonical}
	}
	runs := []Rendered{orig, current}
	if *testCaseInput.reverse {
		slices.Reverse(runs)
	}
	projectedResults6, projectionErrors6 := Select(
		runs,
		&selection.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{
			{Group: testCaseInput.child.Group, Kind: testCaseInput.child.Kind, Namespace: "apps"},
		},
		nil,
	)
	results, err, _ := projectedResults6, projectionErrors6.Selected, projectionErrors6.Remaining
	assert.Equal(t, err != nil, testCaseInput.disposition.Status == store.StatusFailed)
	index := 1
	if *testCaseInput.reverse {
		index = 0
	}
	want := testCaseInput.tc.unavailable
	if _, unavailable := results[index].Unavailable[(*testCaseInput.child)]; unavailable != want {
		t.Fatalf(
			"local ancestor availability = %+v, want unavailable=%t",
			results[index].Unavailable,
			want,
		)
	}
	if want {
		message := testCaseInput.disposition.String()
		if testCaseInput.disposition.Status == store.StatusFailed {
			message = "canonical parent failure"
		}
		assert.Equal(t, results[index].Unavailable[(*testCaseInput.child)].String(), message)
	}
	assert.FatalIf(
		t,
		len(results[1-index].Unavailable) != 0,
		"%v",
		"counterpart failure treated as local evidence",
	)
}

func testSelectRequiredFailureOutsideOutputNamespaceCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		required  bool
		paired    bool
		namespace string
		full      bool
	},
) {
	t.Helper()

	parent := testutil.KustomizationID("flux-system", "parent")
	child := testutil.HelmReleaseID("apps", "child")
	sibling := testutil.HelmReleaseID("monitoring", "excluded")
	run := selectedRun(t, &Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		parent: {Status: store.StatusFailed, Message: "parent failed"},
	}})
	run.store.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	run.store.AddObject(&manifest.HelmRelease{Name: child.Name, Namespace: child.Namespace})
	run.store.AddObject(&manifest.HelmRelease{Name: sibling.Name, Namespace: sibling.Namespace})
	var needed []manifest.NamedResource
	if testCaseInput.required {
		needed = append(needed, parent)
		run.index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			child: {Parent: parent},
		}}
		if testCaseInput.namespace != "" {
			run.index.Records[sibling] = manifest.DiscoveryRecord{Parent: child}
			run.Result.Failed[sibling] = store.StatusInfo{
				Status:  store.StatusFailed,
				Message: "independent child failed",
			}
		}
	}
	if !testCaseInput.full {
		retry := prepareSelectionForTest(t, &run, needed, []manifest.NamedResource{child})
		assert.Equal(t, retry, false)
	}
	run.Err = &selection.FailuresError{Message: "parent failed"}
	runs := []Rendered{run}
	if testCaseInput.paired {
		other := selectedRun(t, &Result{})
		retry := prepareSelectionForTest(
			t,
			&other,
			nil,
			[]manifest.NamedResource{testutil.HelmReleaseID(parent.Namespace, "other")},
		)
		assert.Equal(t, retry, false)
		runs = append(runs, other)
	}
	projectedResults7, projectionErrors7 := Select(
		runs,
		run.Err,
		[]manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Namespace: testCaseInput.namespace}},
		nil,
	)
	selected, err, remainder := projectedResults7, projectionErrors7.Selected, projectionErrors7.Remaining
	assert.Equal(t, err != nil, testCaseInput.required)
	assert.Equal(t, remainder == nil, true)
	_, visible := selected[0].Failed[parent]
	assert.Equal(t, visible, testCaseInput.required)
	_, visible = selected[0].Failed[sibling]
	assert.Equal(t, visible, false)
	_, visible = selected[0].Causes[parent]
	assert.Equal(t, visible, testCaseInput.required)
	_, visible = selected[0].Causes[sibling]
	assert.Equal(t, visible, false)
}

func testSelectRequiredFailureOutsideOutputNamespaceUnscopedRequiredDescendant(t *testing.T) {
	t.Helper()

	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "monitoring"}
	run := selectedRun(t,
		&Result{
			Failed: map[manifest.NamedResource]store.StatusInfo{
				child.Named(): {Status: store.StatusFailed, Message: "required descendant failed"},
			},
		},
	)
	run.store.AddObjects([]manifest.BaseManifest{parent, child})
	setParentsForTest(
		run.Orchestrator,
		map[manifest.NamedResource]manifest.NamedResource{child.Named(): parent.Named()},
	)
	var reads store.ReadSet
	reads.Init(run.store, parent.Named(), nil)
	reads.GetObject(parent.Named())
	reads.GetObject(child.Named())
	reads.RequireData(child.Named())
	reads.Accept(store.AssessmentReconciled)
	reads.Retire()
	_, required := maps.Collect(run.store.RequiredInputs(parent.Named()))[child.Named()]
	assert.Equal(t, required, true)
	retry := prepareSelectionForTest(
		t,
		&run,
		[]manifest.NamedResource{parent.Named(), child.Named()},
		[]manifest.NamedResource{parent.Named()},
	)
	assert.Equal(t, retry, false)
	membershipResult2 := run.selection.Snapshot()
	selectedChild := membershipResult2.Output(child.Named())
	assert.Equal(t, selectedChild, false)
	run.Err = &selection.FailuresError{Message: "required descendant failed"}
	projectedResults8, projectionErrors8 := Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	selected, err, remainder := projectedResults8, projectionErrors8.Selected, projectionErrors8.Remaining
	assert.Equal(t, err != nil, true)
	assert.NoError(t, remainder)
	_, retained := selected[0].Failed[child.Named()]
	assert.Equal(t, retained, true)
}
