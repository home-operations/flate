package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func selectedRun(res *Result) Rendered {
	return Rendered{Orchestrator: &Orchestrator{store: store.New(), index: indexForTest(nil)}, Result: res}
}

func TestSelect_RenderWarningScope(t *testing.T) {
	t.Parallel()
	warnings := []manifest.Warning{
		{Category: manifest.WarnUnresolvedSubstitution, Message: "shared Secret", Count: 2},
		{
			Category: manifest.WarnStaleValues,
			Resource: testutil.HelmReleaseID("apps", "app"),
			Message:  "stale",
			Count:    1,
		},
		{
			Category: manifest.WarnStaleValues,
			Resource: testutil.HelmReleaseID("other", "app"),
			Message:  "foreign",
			Count:    1,
		},
		{
			Category: manifest.WarnOmittedValues,
			Resource: testutil.HelmReleaseID("apps", "app"),
			Message:  "selected",
			Count:    1,
		},
		{
			Category: manifest.WarnOmittedValues,
			Resource: testutil.HelmReleaseID("apps", "sibling"),
			Message:  "other name",
			Count:    1,
		},
		{
			Category: manifest.WarnOmittedValues,
			Resource: testutil.KustomizationID("apps", "app"),
			Message:  "other kind",
			Count:    1,
		},
	}
	for _, tc := range []struct {
		name  string
		sides int
		want  []manifest.Warning
	}{
		{name: "single", sides: 1, want: slices.Concat(warnings[:2], warnings[3:])},
		{name: "comparison", sides: 2, want: slices.Concat(warnings[:2], warnings[3:4])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runs := make([]Rendered, tc.sides)
			for i := range runs {
				runs[i] = selectedRun(&Result{Warnings: slices.Clone(warnings)})
			}
			selected, err, remainder := Select(
				runs,
				nil,
				[]manifest.NamedResource{testutil.HelmReleaseID("apps", "app")},
				nil,
			)
			assert.NoError(t, err)
			assert.NoError(t, remainder)
			assert.Equal(t, len(selected), tc.sides)
			for _, res := range selected {
				assert.Diff(t, res.Warnings, tc.want)
			}
		})
	}
}

func TestMatchesLabels_Matches(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Labels: map[string]string{"env": "prod"},
	}
	cases := []struct {
		name   string
		obj    manifest.BaseManifest
		labels map[string]string
		want   bool
	}{
		{name: "empty matches all", obj: ks, want: true},
		{name: "empty map matches all", obj: ks, labels: map[string]string{}, want: true},
		{name: "label match", obj: ks, labels: map[string]string{"env": "prod"}, want: true},
		{name: "label mismatch", obj: ks, labels: map[string]string{"env": "dev"}},
		{name: "nil"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				got := matchesLabels(tc.obj, tc.labels)
				assert.ErrorIf(t, got != tc.want, "got %v want %v", got, tc.want)
			}
		})
	}
}

func TestSelect_CountsBeforeLabelsAndRequestedOrder(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{Name: "same", Namespace: "apps"}
	ks := &manifest.Kustomization{Name: "same", Namespace: "apps"}
	ks.Labels = map[string]string{"env": ""}
	run := selectedRun(
		&Result{Manifests: []manifest.DocumentBatch{{Parent: ks, Documents: []map[string]any{{"kind": "ConfigMap"}}}}},
	)
	run.store.AddObject(hr)
	run.store.AddObject(ks)
	run.store.AddObject(&manifest.Kustomization{Name: "other", Namespace: "apps", Labels: ks.Labels})
	run.store.UpdateStatus(
		hr.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"},
	)
	raw := outputByParent(run.Result.Manifests)
	roots := []manifest.NamedResource{testutil.HelmReleaseID("", "same"), testutil.KustomizationID("", "same")}
	selected, err, _ := Select([]Rendered{run}, nil, roots, map[string]string{"env": ""})
	assert.FatalIf(t, err != nil, "%v", err)
	res := selected[0]
	assert.Diff(t, res.Counts, map[string]int{manifest.KindHelmRelease: 1, manifest.KindKustomization: 1})
	assert.FatalIf(
		t,
		len(res.Objects) != 1 || res.Objects[0] != ks || len(res.Manifests) != 1,
		"label or raw snapshot changed: selected=%+v raw=%+v",
		res,
		run.Result,
	)
	assert.Diff(t, outputByParent(run.Result.Manifests), raw)
	selected, err, _ = Select([]Rendered{run}, nil, roots, nil)
	assert.FatalIf(
		t,
		err != nil || len(selected[0].Objects) != 2 || selected[0].Objects[0] != hr || selected[0].Objects[1] != ks,
		"requested grouping changed: %+v %v",
		selected,
		err,
	)
	for i := range roots {
		roots[i].Name = "missing"
	}
	selected, err, _ = Select([]Rendered{run}, nil, roots, nil)
	assert.FatalIf(
		t,
		err != nil || len(selected[0].Objects) != 0 || len(selected[0].Counts) != 0,
		"name mismatch selected objects or counted roots: %+v %v",
		selected,
		err,
	)
}

func TestSelect_InferredNamespacesAreNotIdentityFilter(t *testing.T) {
	t.Parallel()
	a, sibling, other := testutil.KustomizationID(
		"flux-system",
		"a",
	), testutil.KustomizationID(
		"flux-system",
		"sibling",
	), testutil.KustomizationID(
		"flux-system",
		"other",
	)
	other.Namespace = "other"
	run := selectedRun(&Result{})
	for _, id := range []manifest.NamedResource{a, sibling, other} {
		run.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	set := change.NewSet([]string{"a.yaml"})
	run.index = indexForTest(
		map[manifest.NamedResource]string{a: "a.yaml", sibling: "sibling.yaml", other: "other.yaml"},
	)
	run.loader = admissiontest.New(
		t,
		admissiontest.Config{Changes: set, Index: run.index, Inventory: run.store, Root: ""},
	)
	admissiontest.Commit(t, run.loader, nil)
	run.filter = run.loader.Options.Filter
	selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	assert.FatalIf(
		t,
		err != nil || selected[0].Counts[manifest.KindKustomization] != 2,
		"inferred scope excluded healthy sibling or admitted foreign namespace: %+v %v",
		selected,
		err,
	)
}

func TestSelect_IncompleteInputCoverageScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace, requested string
		helm, visible, empty       bool
		filters                    [][]bool
	}{
		{name: "inferred unrelated namespace", visible: true},
		{name: "explicit affected namespace", namespace: "apps", visible: true},
		{name: "explicit other namespace", namespace: "infra"},
		{name: "explicit affected name", requested: "affected", visible: true},
		{name: "explicit other name", requested: "selected"},
		{name: "explicit other kind", helm: true},
		{name: "disabled", visible: true, filters: [][]bool{{false}, {false, false}}},
		{name: "orig enabled", visible: true, filters: [][]bool{{true, false}}},
		{name: "current enabled", visible: true, filters: [][]bool{{false, true}}},
		{name: "enabled empty changes", visible: true, empty: true, filters: [][]bool{{true}}},
	} {
		filters := tc.filters
		if filters == nil {
			filters = [][]bool{{true}, {true, true}}
		}
		for _, enabled := range filters {
			t.Run(fmt.Sprintf("%s/%d", tc.name, len(enabled)), func(t *testing.T) {
				t.Parallel()
				testSelectIncompleteInputCoverageScopeCase(
					t,
					testSelectIncompleteInputCoverageScopeCaseInputs{tc: &tc, enabled: &enabled},
				)
			},
			)
		}
	}
}

func TestSelect_FatalScopeAndRunErrorIdentities(t *testing.T) {
	t.Parallel()
	visible := testutil.KustomizationID("media", "plex")
	hidden := testutil.KustomizationID("default", "other")
	for _, sides := range []int{1, 2} {
		t.Run(fmt.Sprintf("sides_%d", sides), func(t *testing.T) {
			t.Parallel()
			testSelectFatalScopeAndRunErrorIdentitiesCase(
				t,
				testSelectFatalScopeAndRunErrorIdentitiesCaseInputs{visible: &visible, hidden: &hidden, sides: &sides},
			)
		},
		)
	}
}

func TestSelect_MissingAndCycles(t *testing.T) {
	t.Parallel()
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("apps", name)
	}
	selected, missing, a, b, consumer := id("selected"), id("missing"), id("a"), id("b"), id("consumer")
	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprintf("named_%t", named), func(t *testing.T) {
			t.Parallel()
			testSelectMissingAndCyclesCase(
				t,
				testSelectMissingAndCyclesCaseInputs{
					selected: &selected,
					missing:  &missing,
					a:        &a,
					b:        &b,
					consumer: &consumer,
					named:    &named,
				},
			)
		},
		)
	}
}

func TestSelect_SharedRootsAndAdvisoryCycles(t *testing.T) {
	t.Parallel()
	selected, root, other, a, b := testutil.KustomizationID(
		"flux-system",
		"selected",
	), testutil.KustomizationID(
		"flux-system",
		"root",
	), testutil.KustomizationID(
		"flux-system",
		"other",
	), testutil.KustomizationID(
		"flux-system",
		"a",
	), testutil.KustomizationID(
		"flux-system",
		"b",
	)
	run := selectedRun(
		&Result{
			Failed: make(map[manifest.NamedResource]store.StatusInfo),
			Blocked: map[manifest.NamedResource][]manifest.NamedResource{
				selected: {root},
				other:    {root},
				a:        {b},
				b:        {a},
			},
		},
	)
	for _, id := range []manifest.NamedResource{selected, root, other, a, b} {
		run.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		run.store.UpdateStatus(
			id,
			store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: id.Name + " failure"},
		)
		run.Result.Failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " failure"}
	}
	for id, deps := range run.Result.Blocked {
		acceptInputsForTest(t, run.store, id, deps)
	}
	run.Err = &FailuresError{Message: "raw summary"}
	panicErr := errors.New("unattributed panic")
	results, err, _ := Select(
		[]Rendered{run},
		errors.Join(run.Err, context.Canceled, panicErr),
		[]manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}},
		nil,
	)
	if len(results[0].Failed) != 2 || len(results[0].Unrelated) != 3 || !errors.Is(err, context.Canceled) ||
		!errors.Is(err, panicErr) {
		t.Fatalf("shared/cyclic projection lost: %+v %v", results[0], err)
	}
	for _, warning := range results[0].Unrelated {
		assert.FatalIf(t, warning.Resource == root || warning.Category != manifest.WarnUnrelatedFailure ||
			strings.Contains(
				warning.Message,
				"dependency cycle",
			), "fatal/advisory policies conflated: %+v", warning)
	}
	got, _, _ := Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}},
		nil,
	)
	assert.Diff(t, got[0].Unrelated, results[0].Unrelated)
	missing := testutil.KustomizationID("flux-system", "missing")
	run.Result.Blocked = map[manifest.NamedResource][]manifest.NamedResource{selected: {missing}, other: {missing}}
	run.Result.Failed = map[manifest.NamedResource]store.StatusInfo{
		selected: {Status: store.StatusFailed, Message: "selected blocked"},
		other:    {Status: store.StatusFailed, Message: "other blocked"},
	}
	for id, deps := range run.Result.Blocked {
		acceptInputsForTest(t, run.store, id, deps)
	}
	results, _, _ = Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}},
		nil,
	)
	assert.Diff(t, results[0].Causes, map[manifest.NamedResource][]manifest.NamedResource{missing: {selected}})
	assert.Diff(
		t,
		results[0].Unrelated,
		[]manifest.Warning{
			{
				Resource: other,
				Subject:  manifest.DiagnosticFor(other, nil),
				Related:  []manifest.NamedResource{missing},
				Category: manifest.WarnUnrelatedFailure,
				Message:  "other blocked",
				Detail:   []string{"blocked by " + missing.String()},
			},
		},
	)
	selected.Name = "healthy"
	run.store.AddObject(&manifest.Kustomization{Name: selected.Name, Namespace: selected.Namespace})
	c := testutil.KustomizationID("flux-system", "c")
	run.Result = &Result{
		Failed: map[manifest.NamedResource]store.StatusInfo{
			a: {Message: "cycle a"},
			b: {Message: "cycle b"},
			c: {Message: "cycle c"},
		},
		Blocked: map[manifest.NamedResource][]manifest.NamedResource{a: {c, b, c}, b: {a}, c: {a}},
	}
	results, err, _ = Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}},
		nil,
	)
	assert.Equal(t, err, nil)
	assert.Diff(t, results[0].Unrelated[0].Detail, []string{"blocked by " + b.String(), "blocked by " + c.String()})
}

type testSelectIncompleteInputCoverageScopeCaseInputs struct {
	tc *struct {
		name string

		namespace string
		requested string
		helm      bool
		visible   bool
		empty     bool
		filters   [][]bool
	}
	enabled *[]bool
}

func testSelectIncompleteInputCoverageScopeCase(
	t *testing.T,
	testCaseInput testSelectIncompleteInputCoverageScopeCaseInputs,
) {
	t.Helper()

	affected := testutil.KustomizationID("apps", "affected")
	selected := testutil.KustomizationID("infra", "selected")
	warning := manifest.Warning{
		Resource: affected,
		Category: manifest.WarnIncompleteInputCoverage,
		Message:  "incomplete",
	}
	run := selectedRun(
		&Result{
			Warnings: []manifest.Warning{warning, {Resource: affected, Category: manifest.WarnStaleValues}},
		},
	)
	run.store.AddObject(&manifest.Kustomization{Name: affected.Name, Namespace: affected.Namespace})
	run.store.AddObject(&manifest.Kustomization{Name: selected.Name, Namespace: selected.Namespace})
	run.index = indexForTest(
		map[manifest.NamedResource]string{affected: "affected.yaml", selected: "selected.yaml"},
	)
	paths := []string{"selected.yaml"}
	if testCaseInput.tc.empty {
		paths = nil
	}
	runs := make([]Rendered, len((*testCaseInput.enabled)))
	for i, active := range *testCaseInput.enabled {
		runs[i] = Rendered{
			Orchestrator: &Orchestrator{store: run.store, index: run.index},
			Result:       run.Result,
		}
		if active {
			runs[i].loader = admissiontest.New(
				t,
				admissiontest.Config{
					Changes:   change.NewSet(paths),
					Index:     run.index,
					Inventory: run.store,
					Root:      "",
				},
			)
			admissiontest.Commit(t, runs[i].loader, nil)
			runs[i].filter = runs[i].loader.Options.Filter
		}
	}
	root := testutil.KustomizationID(testCaseInput.tc.namespace, testCaseInput.tc.requested)
	if testCaseInput.tc.helm {
		root = testutil.HelmReleaseID(testCaseInput.tc.namespace, testCaseInput.tc.requested)
	}
	results, err, _ := Select(runs, nil, []manifest.NamedResource{root}, nil)
	assert.FatalIf(t, err != nil, "%v", err)
	assert.Diff(
		t,
		run.Result.Warnings,
		[]manifest.Warning{warning, {Resource: affected, Category: manifest.WarnStaleValues}},
	)
	for i, result := range results {
		visible := slices.ContainsFunc(
			result.Warnings,
			func(w manifest.Warning) bool { return w.Category == warning.Category && w.Resource == affected },
		)
		assert.Equal(t, visible, (*testCaseInput.enabled)[i] && testCaseInput.tc.visible)
		if testCaseInput.tc.namespace == "" && testCaseInput.tc.requested == "" && !testCaseInput.tc.helm {
			var warnings []manifest.Warning
			if (*testCaseInput.enabled)[i] {
				warnings = append(warnings, warning)
			}
			if (*testCaseInput.enabled)[i] && !testCaseInput.tc.empty {
				assert.Equal(t, len(result.Objects), 1)
				assert.Equal(t, result.Objects[0].Named(), selected)
			} else {
				assert.Equal(t, len(result.Objects), 2)
				warnings = append(warnings, run.Result.Warnings[1])
			}
			assert.Diff(t, result.Warnings, warnings)
		}
	}
}

type testSelectFatalScopeAndRunErrorIdentitiesCaseInputs struct {
	visible *manifest.NamedResource
	hidden  *manifest.NamedResource
	sides   *int
}

func testSelectFatalScopeAndRunErrorIdentitiesCase(
	t *testing.T,
	testCaseInput testSelectFatalScopeAndRunErrorIdentitiesCaseInputs,
) {
	t.Helper()

	for _, tc := range []struct {
		name    string
		visible bool
		wrapped bool
		joined  bool
		extra   error
	}{
		{name: "visible", visible: true},
		{name: "outside raw"},
		{name: "outside wrapped", wrapped: true},
		{name: "outside joined", joined: true},
		{name: "unattributed", extra: errors.New("unattributed panic")},
		{name: "cancellation", extra: context.Canceled},
		{name: "independent input", wrapped: true, extra: manifest.NewError(manifest.ErrInput, nil, "independent input")},
		{name: "independent domain", extra: manifest.NewError(manifest.ErrFlux, nil, "independent domain")},
		{name: "visible and unattributed", visible: true, extra: errors.New("unattributed panic")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectFatalScopeAndRunErrorIdentitiesCaseCase(
				t,
				testSelectFatalScopeAndRunErrorIdentitiesCaseCaseInputs{testCaseInput: &testCaseInput, tc: &tc},
			)
		},
		)
	}
}

type testSelectMissingAndCyclesCaseInputs struct {
	selected *manifest.NamedResource
	missing  *manifest.NamedResource
	a        *manifest.NamedResource
	b        *manifest.NamedResource
	consumer *manifest.NamedResource
	named    *bool
}

func testSelectMissingAndCyclesCase(t *testing.T, testCaseInput testSelectMissingAndCyclesCaseInputs) {
	t.Helper()

	blocked := map[manifest.NamedResource][]manifest.NamedResource{
		(*testCaseInput.selected): {(*testCaseInput.missing)},
		(*testCaseInput.a):        {(*testCaseInput.b)},
		(*testCaseInput.b):        {(*testCaseInput.a)},
		(*testCaseInput.consumer): {(*testCaseInput.a)},
	}
	run := selectedRun(&Result{Blocked: blocked, Failed: make(map[manifest.NamedResource]store.StatusInfo)})
	for _, resource := range []manifest.NamedResource{(*testCaseInput.selected), (*testCaseInput.a), (*testCaseInput.b), (*testCaseInput.consumer)} {
		run.store.AddObject(&manifest.Kustomization{Name: resource.Name, Namespace: resource.Namespace})
		run.store.UpdateFailure(
			resource,
			&manifest.DependencyFailedError{Parent: resource, Failed: blocked[resource]},
		)
		info, _ := run.store.GetStatus(resource)
		run.Result.Failed[resource] = info
	}
	for resource, deps := range blocked {
		acceptInputsForTest(t, run.store, resource, deps)
	}
	setParentsForTest(
		run.Orchestrator,
		map[manifest.NamedResource]manifest.NamedResource{
			(*testCaseInput.a):        (*testCaseInput.selected),
			(*testCaseInput.b):        (*testCaseInput.selected),
			(*testCaseInput.consumer): (*testCaseInput.selected),
		},
	)
	run.Err = &FailuresError{Message: "raw failure summary"}
	root := testutil.KustomizationID("", "")
	if *testCaseInput.named {
		root.Name = testCaseInput.selected.Name
	}
	results, err, _ := Select(
		[]Rendered{run, run},
		errors.Join(run.Err, run.Err),
		[]manifest.NamedResource{root},
		nil,
	)
	res := results[0]
	assert.Diff(
		t,
		res.Causes,
		map[manifest.NamedResource][]manifest.NamedResource{
			(*testCaseInput.missing): {(*testCaseInput.selected)},
			(*testCaseInput.a):       {(*testCaseInput.consumer)},
			(*testCaseInput.b):       nil,
		},
	)
	assert.Diff(t, res.Blocked, blocked)
	assert.Equal(
		t,
		selectedFailures(res).Error(),
		"reconcile completed with 3 failure(s):\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/a: dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/apps/b\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/b: dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/apps/a\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/missing: not found\n  (+2 blocked by failed/missing dependencies)",
	)
	assert.FatalIf(
		t,
		err == nil || !strings.Contains(err.Error(), "both snapshots had reconcile failures"),
		"cycle error missing: %v",
		err,
	)
	for _, resource := range []manifest.NamedResource{(*testCaseInput.a), (*testCaseInput.b)} {
		raw := run.Result.Failed[resource]
		if res.Failed[resource].String() == raw.String() || !res.Failed[resource].IsDependencyFailure() {
			t.Fatalf(
				"cycle message or typed evidence lost: raw=%+v selected=%+v unavailable=%+v",
				raw,
				res.Failed[resource],
				res.Unavailable[resource],
			)
		}
		assert.Diff(t, res.Failed[resource].BlockedBy(), blocked[resource])
		if !(*testCaseInput.named) {
			assert.Equal(t, res.Unavailable[resource].String(), raw.String())
		}
	}
	if !(*testCaseInput.named) {
		assert.Equal(t, res.Statuses[(*testCaseInput.a)].String(), "blocked by apps/b")
		assert.Equal(t, res.Statuses[(*testCaseInput.consumer)].String(), "blocked by apps/a")
	} else {
		assert.Equal(t, res.Statuses[(*testCaseInput.selected)].String(), "blocked by apps/missing (not found)")
	}
}

type testSelectFatalScopeAndRunErrorIdentitiesCaseCaseInputs struct {
	testCaseInput *testSelectFatalScopeAndRunErrorIdentitiesCaseInputs
	tc            *struct {
		name    string
		visible bool
		wrapped bool
		joined  bool
		extra   error
	}
}

func testSelectFatalScopeAndRunErrorIdentitiesCaseCase(
	t *testing.T,
	testCaseInput testSelectFatalScopeAndRunErrorIdentitiesCaseCaseInputs,
) {
	t.Helper()

	failed := map[manifest.NamedResource]store.StatusInfo{
		(*testCaseInput.testCaseInput.hidden): {Status: store.StatusFailed, Message: "default failed"},
	}
	wantRaw := "reconcile completed with 1 failure(s):\n  " + testCaseInput.testCaseInput.hidden.String() + ": default failed"
	if testCaseInput.tc.visible {
		failed[(*testCaseInput.testCaseInput.visible)] = store.StatusInfo{
			Status:  store.StatusFailed,
			Message: "media failed",
		}
		wantRaw = "reconcile completed with 2 failure(s):\n  " + testCaseInput.testCaseInput.hidden.String() + ": default failed\n  " + testCaseInput.testCaseInput.visible.String() + ": media failed"
	}
	run := selectedRun(&Result{Failed: failed})
	raw := run.aggregateFailures(failed)
	run.Err = raw
	if testCaseInput.tc.wrapped {
		run.Err = fmt.Errorf("wrapped: %w", run.Err)
	}
	if testCaseInput.tc.joined || testCaseInput.tc.extra != nil {
		run.Err = errors.Join(run.Err, testCaseInput.tc.extra)
	}
	for _, aggregate := range []error{raw, run.Err} {
		concrete, ok := errors.AsType[*FailuresError](aggregate)
		assert.FatalIf(
			t,
			!ok || !errors.Is(concrete, raw) || !errors.Is(aggregate, manifest.ErrFlux),
			"aggregate identity lost: %v",
			aggregate,
		)
		assert.Equal(t, concrete.Message, wantRaw)
	}
	assert.Equal(t, raw.Error(), wantRaw)
	assert.FatalIf(
		t,
		errors.Is(raw, manifest.ErrInput),
		"%v",
		"aggregate classified heterogeneous failures as input errors",
	)
	runs, runErr := []Rendered{run}, run.Err
	if (*testCaseInput.testCaseInput.sides) == 2 {
		runs, runErr = []Rendered{run, run}, errors.Join(run.Err, run.Err)
	}
	selected, err, remainder := Select(
		runs,
		runErr,
		[]manifest.NamedResource{testutil.KustomizationID("media", "")},
		nil,
	)
	assert.FatalIf(
		t,
		(err != nil) != (testCaseInput.tc.visible || testCaseInput.tc.extra != nil) ||
			len(selected) != (*testCaseInput.testCaseInput.sides),
		"unexpected scoped failure: %+v %v",
		selected,
		err,
	)
	for _, res := range selected {
		if (len(res.Failed) != 0) != testCaseInput.tc.visible {
			t.Fatalf("unexpected selected failures: %+v", res.Failed)
		}
	}
	assert.FatalIf(
		t,
		(remainder != nil) != (testCaseInput.tc.extra != nil),
		"unexpected incidental error: %v",
		remainder,
	)
	for _, got := range []error{err, remainder} {
		assert.FatalIf(
			t,
			testCaseInput.tc.extra != nil && !errors.Is(got, testCaseInput.tc.extra),
			"non-resource error identity lost: %v",
			got,
		)
		assert.FatalIf(
			t,
			errors.Is(got, manifest.ErrInput) != errors.Is(testCaseInput.tc.extra, manifest.ErrInput),
			"input identity changed: %v",
			got,
		)
		assert.FatalIf(t, errors.Is(got, raw), "raw aggregate entered projection: %v", got)
	}
	assert.FatalIf(
		t,
		errors.Is(err, manifest.ErrFlux) != (testCaseInput.tc.visible || errors.Is(testCaseInput.tc.extra, manifest.ErrFlux)) ||
			errors.Is(
				remainder,
				manifest.ErrFlux,
			) != errors.Is(testCaseInput.tc.extra, manifest.ErrFlux),
		"domain identity changed: selected=%v remainder=%v",
		err,
		remainder,
	)
	concrete, ok := errors.AsType[*FailuresError](err)
	assert.FatalIf(t, ok != testCaseInput.tc.visible, "selected aggregate identity changed: %v", err)
	if testCaseInput.tc.visible {
		assert.Equal(
			t,
			concrete.Message,
			"reconcile completed with 1 failure(s):\n  "+testCaseInput.testCaseInput.visible.String()+": media failed",
		)
	}
	{
		_, ok := errors.AsType[*FailuresError](remainder)
		assert.FatalIf(t, ok, "aggregate entered incidental error: %v", remainder)
	}
	assert.FatalIf(
		t,
		err != nil && strings.Contains(err.Error(), "default failed"),
		"hidden raw failure leaked: %v",
		err,
	)
}
