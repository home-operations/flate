package orchestrator

import (
	"context"
	"errors"
	"fmt"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"maps"
	"slices"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

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
		{Category: manifest.WarnStaleValues, Resource: testutil.HelmReleaseID("apps", "app"), Message: "stale", Count: 1},
		{Category: manifest.WarnStaleValues, Resource: testutil.HelmReleaseID("other", "app"), Message: "foreign", Count: 1},
		{Category: manifest.WarnOmittedValues, Resource: testutil.HelmReleaseID("apps", "app"), Message: "selected", Count: 1},
		{Category: manifest.WarnOmittedValues, Resource: testutil.HelmReleaseID("apps", "sibling"), Message: "other name", Count: 1},
		{Category: manifest.WarnOmittedValues, Resource: testutil.KustomizationID("apps", "app"), Message: "other kind", Count: 1},
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
			selected, err, remainder := Select(runs, nil, []manifest.NamedResource{testutil.HelmReleaseID("apps", "app")}, nil)
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
			if got := matchesLabels(tc.obj, tc.labels); got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestSelect_CountsBeforeLabelsAndRequestedOrder(t *testing.T) {
	t.Parallel()
	hr := &manifest.HelmRelease{Name: "same", Namespace: "apps"}
	ks := &manifest.Kustomization{Name: "same", Namespace: "apps"}
	ks.Labels = map[string]string{"env": ""}
	run := selectedRun(&Result{Manifests: map[manifest.NamedResource][]map[string]any{ks.Named(): {{"kind": "ConfigMap"}}}})
	run.store.AddObject(hr)
	run.store.AddObject(ks)
	run.store.AddObject(&manifest.Kustomization{Name: "other", Namespace: "apps", Labels: ks.Labels})
	run.store.UpdateStatus(hr.Named(), store.StatusReady, store.MsgSuspended)
	roots := []manifest.NamedResource{testutil.HelmReleaseID("", "same"), testutil.KustomizationID("", "same")}
	selected, err, _ := Select([]Rendered{run}, nil, roots, map[string]string{"env": ""})
	if err != nil {
		t.Fatal(err)
	}
	res := selected[0]
	assert.Diff(t, res.Counts, map[string]int{manifest.KindHelmRelease: 1, manifest.KindKustomization: 1})
	if len(res.Objects) != 1 || res.Objects[0] != ks || len(res.Manifests) != 1 || run.Result.Counts != nil {
		t.Fatalf("label or raw snapshot changed: selected=%+v raw=%+v", res, run.Result)
	}
	selected, err, _ = Select([]Rendered{run}, nil, roots, nil)
	if err != nil || len(selected[0].Objects) != 2 || selected[0].Objects[0] != hr || selected[0].Objects[1] != ks {
		t.Fatalf("requested grouping changed: %+v %v", selected, err)
	}
	for i := range roots {
		roots[i].Name = "missing"
	}
	selected, err, _ = Select([]Rendered{run}, nil, roots, nil)
	if err != nil || len(selected[0].Objects) != 0 || len(selected[0].Counts) != 0 {
		t.Fatalf("name mismatch selected objects or counted roots: %+v %v", selected, err)
	}
}

func TestSelect_InferredNamespacesAreNotIdentityFilter(t *testing.T) {
	t.Parallel()
	a, sibling, other := testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "sibling"), testutil.KustomizationID("flux-system", "other")
	other.Namespace = "other"
	run := selectedRun(&Result{})
	for _, id := range []manifest.NamedResource{a, sibling, other} {
		run.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	set := change.NewSet([]string{"a.yaml"})
	run.index = indexForTest(map[manifest.NamedResource]string{a: "a.yaml", sibling: "sibling.yaml", other: "other.yaml"})
	run.filter = change.NewFilter(set, run.index, run.store)
	selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	if err != nil || selected[0].Counts[manifest.KindKustomization] != 2 {
		t.Fatalf("inferred scope excluded healthy sibling or admitted foreign namespace: %+v %v", selected, err)
	}
}

func TestSelect_FatalScopeAndRunErrorIdentities(t *testing.T) {
	t.Parallel()
	visible := testutil.KustomizationID("media", "plex")
	hidden := testutil.KustomizationID("default", "other")
	for _, tc := range []struct {
		name    string
		visible bool
		extra   error
	}{
		{name: "visible", visible: true},
		{name: "outside only"},
		{name: "unattributed", extra: errors.New("unattributed panic")},
		{name: "cancellation", extra: context.Canceled},
		{name: "visible and unattributed", visible: true, extra: errors.New("unattributed panic")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			failed := map[manifest.NamedResource]store.StatusInfo{hidden: {Status: store.StatusFailed, Message: "default failed"}}
			if tc.visible {
				failed[visible] = store.StatusInfo{Status: store.StatusFailed, Message: "media failed"}
			}
			run := selectedRun(&Result{Failed: failed})
			run.Err = errors.Join(&FailuresError{Message: "raw resource failures"}, tc.extra)
			selected, err, _ := Select([]Rendered{run}, run.Err, []manifest.NamedResource{testutil.KustomizationID("media", "")}, nil)
			if (err != nil) != (tc.visible || tc.extra != nil) || (len(selected[0].Failed) != 0) != tc.visible {
				t.Fatalf("unexpected scoped failure: %+v %v", selected[0], err)
			}
			if tc.extra != nil && !errors.Is(err, tc.extra) {
				t.Fatalf("non-resource error identity lost: %v", err)
			}
			if err != nil && (strings.Contains(err.Error(), "default failed") || strings.Contains(err.Error(), "raw resource failures")) {
				t.Fatalf("hidden raw failure leaked: %v", err)
			}
		})
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
			blocked := map[manifest.NamedResource][]manifest.NamedResource{selected: {missing}, a: {b}, b: {a}, consumer: {a}}
			run := selectedRun(&Result{Blocked: blocked, Failed: make(map[manifest.NamedResource]store.StatusInfo)})
			for _, resource := range []manifest.NamedResource{selected, a, b, consumer} {
				run.store.AddObject(&manifest.Kustomization{Name: resource.Name, Namespace: resource.Namespace})
				run.store.UpdateFailure(resource, &manifest.DependencyFailedError{Parent: resource, Failed: blocked[resource]})
				info, _ := run.store.GetStatus(resource)
				run.Result.Failed[resource] = info
			}
			setParentsForTest(run.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{a: selected, b: selected, consumer: selected})
			run.Err = &FailuresError{Message: "raw failure summary"}
			root := testutil.KustomizationID("", "")
			if named {
				root.Name = selected.Name
			}
			results, err, _ := Select([]Rendered{run, run}, joinTreeErrors(run.Err, run.Err), []manifest.NamedResource{root}, nil)
			res := results[0]
			assert.Diff(t, res.Causes, map[manifest.NamedResource][]manifest.NamedResource{missing: {selected}, a: {consumer}, b: nil})
			assert.Diff(t, res.Blocked, blocked)
			assert.Equal(t, selectedFailures(res).Error(), "reconcile completed with 3 failure(s):\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/a: dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/apps/b\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/b: dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/apps/a\n  Kustomization.kustomize.toolkit.fluxcd.io/apps/missing: not found\n  (+2 blocked by failed/missing dependencies)")
			if err == nil || !strings.Contains(err.Error(), "both snapshots had reconcile failures") {
				t.Fatalf("cycle error missing: %v", err)
			}
			for _, resource := range []manifest.NamedResource{a, b} {
				raw := run.Result.Failed[resource]
				if res.Failed[resource].Message == raw.Message || !res.Failed[resource].IsDependencyFailure() {
					t.Fatalf("cycle message or typed evidence lost: raw=%+v selected=%+v unavailable=%+v", raw, res.Failed[resource], res.Unavailable[resource])
				}
				assert.Diff(t, res.Failed[resource].BlockedBy(), blocked[resource])
				if !named {
					assert.Equal(t, res.Unavailable[resource].Message, raw.Message)
				}
			}
			if !named {
				assert.Equal(t, res.Statuses[a].Message, "blocked by apps/b")
				assert.Equal(t, res.Statuses[consumer].Message, "blocked by apps/a")
			} else {
				assert.Equal(t, res.Statuses[selected].Message, "blocked by apps/missing (not found)")
			}
		})
	}
}

func TestSelect_SharedRootsAndAdvisoryCycles(t *testing.T) {
	t.Parallel()
	selected, root, other, a, b := testutil.KustomizationID("flux-system", "selected"), testutil.KustomizationID("flux-system", "root"), testutil.KustomizationID("flux-system", "other"), testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "b")
	run := selectedRun(&Result{Failed: make(map[manifest.NamedResource]store.StatusInfo), Blocked: map[manifest.NamedResource][]manifest.NamedResource{selected: {root}, other: {root}, a: {b}, b: {a}}})
	for _, id := range []manifest.NamedResource{selected, root, other, a, b} {
		run.Result.Failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " failure"}
	}
	run.Err = &FailuresError{Message: "raw summary"}
	panicErr := errors.New("unattributed panic")
	results, err, _ := Select([]Rendered{run}, errors.Join(run.Err, context.Canceled, panicErr), []manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}}, nil)
	if len(results[0].Failed) != 2 || len(results[0].Unrelated) != 3 || !errors.Is(err, context.Canceled) || !errors.Is(err, panicErr) {
		t.Fatalf("shared/cyclic projection lost: %+v %v", results[0], err)
	}
	for _, warning := range results[0].Unrelated {
		if warning.Resource == root || warning.Category != "" || strings.Contains(warning.Message, "dependency cycle") {
			t.Fatalf("fatal/advisory policies conflated: %+v", warning)
		}
	}
	got, _, _ := Select([]Rendered{run}, run.Err, []manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}}, nil)
	assert.Diff(t, got[0].Unrelated, results[0].Unrelated)
	missing := testutil.KustomizationID("flux-system", "missing")
	run.Result.Blocked = map[manifest.NamedResource][]manifest.NamedResource{selected: {missing}, other: {missing}}
	run.Result.Failed = map[manifest.NamedResource]store.StatusInfo{selected: {Status: store.StatusFailed, Message: "selected blocked"}, other: {Status: store.StatusFailed, Message: "other blocked"}}
	results, _, _ = Select([]Rendered{run}, run.Err, []manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}}, nil)
	assert.Diff(t, results[0].Causes, map[manifest.NamedResource][]manifest.NamedResource{missing: {selected}})
	assert.Diff(t, results[0].Unrelated, []manifest.Warning{{Resource: other, Message: "other blocked", Detail: []string{"blocked by " + missing.String()}}})
	selected.Name = "healthy"
	run.store.AddObject(&manifest.Kustomization{Name: selected.Name, Namespace: selected.Namespace})
	c := testutil.KustomizationID("flux-system", "c")
	run.Result = &Result{Failed: map[manifest.NamedResource]store.StatusInfo{a: {Message: "cycle a"}, b: {Message: "cycle b"}, c: {Message: "cycle c"}}, Blocked: map[manifest.NamedResource][]manifest.NamedResource{a: {c, b, c}, b: {a}, c: {a}}}
	results, err, _ = Select([]Rendered{run}, run.Err, []manifest.NamedResource{{Group: selected.Group, Kind: selected.Kind, Name: selected.Name}}, nil)
	assert.Equal(t, err, nil)
	assert.Diff(t, results[0].Unrelated[0].Detail, []string{"blocked by " + b.String(), "blocked by " + c.String()})
}

func TestSelect_ErrorAggregationRetainsSharedTeardown(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("shared cache close")
	failed := selectedRun(&Result{Failed: map[manifest.NamedResource]store.StatusInfo{testutil.KustomizationID("flux-system", "failed"): {Status: store.StatusFailed, Message: "resource failed"}}})
	failed.Err = errors.Join(&FailuresError{Message: "raw failures"}, context.Canceled)
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
		{name: "joined close", shared: errors.Join(joinTreeErrors(nil, nil), closeErr)},
		{name: "composed", orig: failed.Err, shared: closeErr, want: "orig snapshot:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runs := []Rendered{selectedRun(&Result{}), selectedRun(&Result{})}
			for i, err := range []error{tc.orig, tc.current} {
				if err != nil {
					runs[i] = failed
				}
				runs[i].Err = err
			}
			_, err, remainder := Select(runs, errors.Join(joinTreeErrors(tc.orig, tc.current), tc.shared), []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
			if (err != nil) != (tc.orig != nil || tc.current != nil || tc.shared != nil) || (tc.want != "" && !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("snapshot attribution changed: %v", err)
			}
			for _, cause := range []error{context.Canceled, closeErr} {
				want := errors.Is(cause, context.Canceled) && (tc.orig != nil || tc.current != nil) || errors.Is(cause, closeErr) && tc.shared != nil
				if errors.Is(err, cause) != want || errors.Is(remainder, cause) != want {
					t.Fatalf("error identity lost: selected=%v remainder=%v", err, remainder)
				}
			}
			if _, ok := errors.AsType[*FailuresError](remainder); ok {
				t.Fatalf("resource failures entered remainder: %v", remainder)
			}
		})
	}
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("nested tree reversed=%t", reversed), func(t *testing.T) {
			t.Parallel()
			teardownA, teardownB := errors.New("teardown A"), errors.New("teardown B")
			baseFailure := &FailuresError{Message: "raw base failure"}
			runs := []Rendered{failed, selectedRun(&Result{})}
			runs[0].Err, runs[1].Err = baseFailure, context.Canceled
			nested := fmt.Errorf("outer: %w", joinTreeErrors(baseFailure, context.Canceled))
			children := []error{teardownA, nested, teardownB}
			if reversed {
				slices.Reverse(children)
			}
			selected, err, remainder := Select(runs, errors.Join(children...), []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
			want := "both snapshots had reconcile failures\norig snapshot: reconcile completed with 1 failure(s):\n  " + testutil.KustomizationID("flux-system", "failed").String() + ": resource failed\ncurrent snapshot: context canceled"
			wantRemainder := "current snapshot: context canceled"
			if reversed {
				want = "teardown B\n" + want + "\nteardown A"
				wantRemainder = "teardown B\n" + wantRemainder + "\nteardown A"
			} else {
				want = "teardown A\n" + want + "\nteardown B"
				wantRemainder = "teardown A\n" + wantRemainder + "\nteardown B"
			}
			if len(selected) != 2 || err == nil || remainder == nil {
				t.Fatalf("lost nested projection: selected=%v err=%v remainder=%v", selected, err, remainder)
			}
			assert.Equal(t, err.Error(), want)
			assert.Equal(t, remainder.Error(), wantRemainder)
			for _, got := range []error{err, remainder} {
				for _, cause := range []error{teardownA, teardownB, context.Canceled} {
					if !errors.Is(got, cause) {
						t.Fatalf("lost nested cause %v: %v", cause, got)
					}
				}
				if errors.Is(got, baseFailure) {
					t.Fatalf("raw base failure entered projection: %v", got)
				}
			}
			if _, ok := errors.AsType[*FailuresError](err); !ok {
				t.Fatal("selected failure identity lost")
			}
			if _, ok := errors.AsType[*FailuresError](remainder); ok {
				t.Fatal("resource failure entered remainder")
			}
		})
	}
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			t.Parallel()
			causes := [2]error{errors.New("base operation"), errors.New("head operation")}
			runs := []Rendered{selectedRun(&Result{}), selectedRun(&Result{})}
			gates := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			finished := make(chan int)
			for i := range runs {
				go func() {
					<-gates[i]
					runs[i].Err = causes[i]
					finished <- i
				}()
			}
			for _, i := range order {
				close(gates[i])
				if got := <-finished; got != i {
					t.Fatalf("completion = %d, want %d", got, i)
				}
			}
			raw := errors.Join(joinTreeErrors(runs[0].Err, runs[1].Err), closeErr)
			selected, err, remainder := Select(runs, raw, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
			if len(selected) != 2 {
				t.Fatalf("lost partial sides: %v", selected)
			}
			for _, got := range []error{raw, err, remainder} {
				previous := -1
				for _, cause := range []error{causes[0], causes[1], closeErr} {
					position := strings.Index(got.Error(), cause.Error())
					if !errors.Is(got, cause) || strings.Count(got.Error(), cause.Error()) != 1 || position <= previous {
						t.Fatalf("base/head/backing ordering or identity lost: %v", got)
					}
					previous = position
				}
			}
		})
	}
	if joinTreeErrors(errors.New("base"), errors.New("head")).Error() != "both trees had reconcile failures\nbase tree: base\nhead tree: head" {
		t.Fatal("raw tree aggregate wording changed")
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
		{disposition: store.StatusInfo{Status: store.StatusReady, Message: "skipped: absent source"}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Message: store.MsgSuspended}, unavailable: true},
		{disposition: store.StatusInfo{Status: store.StatusReady, Message: store.MsgUnchanged}},
		{disposition: store.StatusInfo{Status: store.StatusReady}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: "fetching"}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: "skipped: absent source"}},
		{disposition: store.StatusInfo{Status: store.StatusPending, Message: store.MsgSuspended}},
	} {
		disposition := tc.disposition
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_%s_reverse_%t", disposition.Status, disposition.Message, reverse), func(t *testing.T) {
				t.Parallel()
				orig := selectedRun(&Result{Manifests: map[manifest.NamedResource][]map[string]any{child: {{"kind": "Deployment"}}}})
				current := selectedRun(&Result{})
				orig.store.AddObject(&manifest.HelmRelease{Name: child.Name, Namespace: child.Namespace})
				setParentsForTest(orig.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{child: parent})
				current.store.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
				current.store.UpdateStatus(parent, disposition.Status, disposition.Message)
				if disposition.Status == store.StatusFailed {
					canonical := disposition
					canonical.Message = "canonical parent failure"
					current.Result.Failed = map[manifest.NamedResource]store.StatusInfo{parent: canonical}
				}
				runs := []Rendered{orig, current}
				if reverse {
					slices.Reverse(runs)
				}
				results, err, _ := Select(runs, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Namespace: "apps"}}, nil)
				if err != nil {
					t.Fatalf("out-of-namespace owner made unnamed query fatal: %v", err)
				}
				index := 1
				if reverse {
					index = 0
				}
				want := tc.unavailable
				if _, unavailable := results[index].Unavailable[child]; unavailable != want {
					t.Fatalf("local ancestor availability = %+v, want unavailable=%t", results[index].Unavailable, want)
				}
				if want {
					message := disposition.Message
					if disposition.Status == store.StatusFailed {
						message = "canonical parent failure"
					}
					assert.Equal(t, results[index].Unavailable[child].Message, message)
				}
				if len(results[1-index].Unavailable) != 0 {
					t.Fatal("counterpart failure treated as local evidence")
				}
			})
		}
	}
}

func TestSelect_PreparedStatusesExcludeOffRosterPrerequisites(t *testing.T) {
	t.Parallel()
	victim := testutil.KustomizationID("flux-system", "victim")
	chart := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "flux-system", Name: victim.Name}
	extra := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "victim"}
	run := selectedRun(&Result{})
	run.store.AddObject(&manifest.Kustomization{Name: victim.Name, Namespace: victim.Namespace})
	run.store.AddObject(&manifest.HelmChartSource{Name: chart.Name, Namespace: chart.Namespace})
	run.store.AddObject(&manifest.ResourceSet{Name: extra.Name, Namespace: extra.Namespace})
	run.store.UpdateFailure(chart, errors.New("chart failed"))
	run.store.UpdateFailure(extra, errors.New("generator failed"))
	run.store.UpdateFailure(victim, &manifest.DependencyFailedError{Parent: victim, Failed: []manifest.NamedResource{chart}})
	run.Result.Failed = run.store.FailedResources()
	run.Result.Blocked = map[manifest.NamedResource][]manifest.NamedResource{victim: {chart}}
	run.Err = &FailuresError{Message: "raw failures"}
	results, _, _ := Select([]Rendered{run}, run.Err, []manifest.NamedResource{{Group: victim.Group, Kind: victim.Kind, Name: victim.Name}}, nil)
	res := results[0]
	assert.Diff(t, slices.SortedFunc(maps.Keys(res.Statuses), manifest.NamedResource.Compare), []manifest.NamedResource{victim, extra})
	assert.Equal(t, res.Statuses[victim].Message, "blocked by "+chart.NamespacedName()+": chart failed")
	assert.Equal(t, res.Statuses[victim].IsDependencyFailure(), true)
	assert.Diff(t, res.Statuses[victim].BlockedBy(), []manifest.NamedResource{chart})
	if _, exists := res.Statuses[chart]; exists || res.Counts[victim.Kind] != 1 {
		t.Fatal("prerequisite entered diagnostic roster or roots count")
	}

	bootstrap := &manifest.GitRepository{Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace}
	ordinary := &manifest.GitRepository{Name: "ordinary", Namespace: "flux-system"}
	otherNamespace := &manifest.GitRepository{Name: bootstrap.Name, Namespace: "other"}
	foreignChart := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: chart.Kind, Namespace: chart.Namespace, Name: chart.Name}
	statusless := &manifest.Kustomization{Name: "statusless", Namespace: victim.Namespace}
	for _, obj := range []manifest.BaseManifest{bootstrap, ordinary, otherNamespace, foreignChart, statusless} {
		run.store.AddObject(obj)
	}
	run.store.UpdateStatus(bootstrap.Named(), store.StatusFailed, "bootstrap failed")
	run.store.UpdateStatus(ordinary.Named(), store.StatusReady, "")
	run.store.UpdateStatus(otherNamespace.Named(), store.StatusReady, "")
	run.store.UpdateStatus(foreignChart.Named(), store.StatusFailed, "foreign chart failed")
	run.Result.Failed = run.store.FailedResources()
	run.Result.Manifests = make(map[manifest.NamedResource][]map[string]any)
	for _, obj := range run.store.ListObjects("", "") {
		run.Result.Manifests[obj.Named()] = []map[string]any{{"kind": "ConfigMap", "metadata": map[string]any{"name": obj.Named().Name}}}
	}
	for _, tc := range []struct {
		name     string
		root     manifest.NamedResource
		objects  []manifest.BaseManifest
		statuses []manifest.NamedResource
	}{
		{
			name: "render roster with missing status", root: manifest.NamedResource{Group: victim.Group, Kind: victim.Kind},
			objects:  []manifest.BaseManifest{statusless, run.store.GetObject(victim)},
			statuses: []manifest.NamedResource{victim, statusless.Named(), extra, foreignChart.Named()},
		},
		{
			name: "explicit chart", root: manifest.NamedResource{Group: chart.Group, Kind: chart.Kind},
			objects:  []manifest.BaseManifest{run.store.GetObject(chart)},
			statuses: []manifest.NamedResource{chart, victim, extra, foreignChart.Named()},
		},
		{
			name: "explicit source", root: manifest.NamedResource{Group: bootstrap.Named().Group, Kind: bootstrap.Named().Kind},
			objects:  []manifest.BaseManifest{bootstrap, ordinary, otherNamespace},
			statuses: []manifest.NamedResource{ordinary.Named(), otherNamespace.Named(), victim, extra, foreignChart.Named()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			results, _, _ := Select([]Rendered{run}, run.Err, []manifest.NamedResource{tc.root}, nil)
			res := results[0]
			assert.Diff(t, res.Objects, tc.objects)
			assert.Diff(t, res.Counts, map[string]int{tc.root.Kind: len(tc.objects)})
			wantDocs := make(map[manifest.NamedResource][]map[string]any)
			for _, obj := range tc.objects {
				wantDocs[obj.Named()] = run.Result.Manifests[obj.Named()]
			}
			assert.Diff(t, res.Manifests, wantDocs)
			slices.SortFunc(tc.statuses, manifest.NamedResource.Compare)
			assert.Diff(t, slices.SortedFunc(maps.Keys(res.Statuses), manifest.NamedResource.Compare), tc.statuses)
			assert.Equal(t, maps.Equal(res.Failed, run.Result.Failed), true)
			if info, ok := res.Statuses[statusless.Named()]; ok {
				assert.Equal(t, info.Status, store.StatusFailed)
				assert.Equal(t, info.Message, "no status reported")
			}
			_, exists := run.store.GetStatus(statusless.Named())
			assert.Equal(t, exists, false)
			assert.Equal(t, maps.Equal(run.store.FailedResources(), run.Result.Failed), true)
		})
	}
	t.Run("extra failure query filters", func(t *testing.T) {
		t.Parallel()
		run := selectedRun(&Result{})
		obj := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
		run.store.AddObject(obj)
		run.store.UpdateStatus(obj.Named(), store.StatusReady, "")
		eligible := &manifest.ResourceSet{Name: obj.Name, Namespace: obj.Namespace}
		for _, extra := range []*manifest.ResourceSet{
			eligible,
			{Name: "other", Namespace: obj.Namespace},
			{Name: obj.Name, Namespace: "other"},
		} {
			run.store.AddObject(extra)
			run.store.UpdateStatus(extra.Named(), store.StatusFailed, "extra failure")
		}
		run.Result.Failed = run.store.FailedResources()
		selected, _, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{obj.Named()}, nil)
		assert.Diff(t, slices.SortedFunc(maps.Keys(selected[0].Statuses), manifest.NamedResource.Compare), []manifest.NamedResource{obj.Named(), eligible.Named()})
		assert.Diff(t, selected[0].Objects, []manifest.BaseManifest{obj})
		assert.Equal(t, len(run.store.FailedResources()), 3)
	})

}

func TestSelect_CascadesAndSharedVictims(t *testing.T) {
	t.Parallel()
	root, mid, leaf, missing, dual := testutil.KustomizationID("flux-system", "root"), testutil.KustomizationID("flux-system", "mid"), testutil.KustomizationID("flux-system", "leaf"), testutil.KustomizationID("flux-system", "missing"), testutil.KustomizationID("flux-system", "dual")
	for _, tc := range []struct {
		name    string
		blocked map[manifest.NamedResource][]manifest.NamedResource
		want    map[manifest.NamedResource][]manifest.NamedResource
	}{
		{name: "transitive", blocked: map[manifest.NamedResource][]manifest.NamedResource{mid: {root}, leaf: {mid}}, want: map[manifest.NamedResource][]manifest.NamedResource{root: {leaf, mid}}},
		{name: "shared", blocked: map[manifest.NamedResource][]manifest.NamedResource{dual: {root, missing, root}}, want: map[manifest.NamedResource][]manifest.NamedResource{root: {dual}, missing: {dual}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{Failed: make(map[manifest.NamedResource]store.StatusInfo), Blocked: tc.blocked})
			run.Result.Failed[root] = store.StatusInfo{Status: store.StatusFailed, Message: "root boom"}
			for id := range tc.blocked {
				run.Result.Failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: "nested dependencies failed"}
			}
			run.Err = &FailuresError{Message: "raw failures"}
			selected, err, _ := Select([]Rendered{run}, run.Err, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
			assert.Diff(t, selected[0].Causes, tc.want)
			if err == nil || strings.Contains(err.Error(), "nested dependencies failed") || !strings.Contains(err.Error(), fmt.Sprintf("(+%d blocked", len(tc.blocked))) {
				t.Fatalf("cascade error did not count distinct victims: %v", err)
			}
			if _, ok := errors.AsType[*FailuresError](err); !ok {
				t.Fatalf("aggregate lost failure type: %T", err)
			}
		})
	}
}

func TestSelect_InvalidQueryAndInitialization(t *testing.T) {
	t.Parallel()
	run := selectedRun(&Result{})
	valid := []manifest.NamedResource{testutil.KustomizationID("", "")}
	for _, roots := range [][]manifest.NamedResource{
		nil,
		{{}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "different"}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "different"}},
	} {
		if _, err, _ := Select([]Rendered{run}, nil, roots, nil); !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("invalid query lacks domain identity: %+v %v", roots, err)
		}
	}
	initErr := errors.New("initialization failed")
	if selected, err, _ := Select([]Rendered{{Err: initErr}}, initErr, valid, nil); len(selected) != 0 || !errors.Is(err, initErr) {
		t.Fatalf("initialization error changed: %+v %v", selected, err)
	}
	if _, err, _ := Select([]Rendered{{}}, nil, valid, nil); !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("incomplete run accepted: %v", err)
	}
}

func TestSelect_AncestryCyclesAndOwnEvidence(t *testing.T) {
	t.Parallel()
	child, parent, unrelated := testutil.KustomizationID("flux-system", "child"), testutil.KustomizationID("flux-system", "parent"), testutil.KustomizationID("flux-system", "unrelated")
	orig := selectedRun(&Result{})
	current := selectedRun(&Result{Failed: map[manifest.NamedResource]store.StatusInfo{unrelated: {Status: store.StatusFailed, Message: "unrelated failed"}}})
	for _, run := range []Rendered{orig, current} {
		run.store.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
		run.store.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
		setParentsForTest(run.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{child: parent, parent: child})
	}
	current.store.UpdateStatus(child, store.StatusReady, "skipped: own skip")
	current.store.UpdateStatus(parent, store.StatusFailed, "ancestor failed")
	results, _, _ := Select([]Rendered{orig, current}, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}}, nil)
	assert.Equal(t, results[1].Unavailable[child].Message, "skipped: own skip")
	assert.Equal(t, len(results[0].Unavailable), 0)
	current.store.UpdateStatus(child, store.StatusReady, store.MsgUnchanged)
	results, _, _ = Select([]Rendered{orig, current}, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}}, nil)
	assert.Equal(t, results[1].Unavailable[child].Message, "ancestor failed")
	current.store.UpdateStatus(parent, store.StatusReady, "")
	results, _, _ = Select([]Rendered{orig, current}, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}}, nil)
	assert.Equal(t, len(results[1].Unavailable), 0)
}

func TestSelect_NamespaceSnapshots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		namespace string
		changes   []string
		disabled  bool
		want      []string
	}{
		{name: "explicit", namespace: "media", changes: []string{"networking.yaml"}, want: []string{"cluster", "media"}},
		{name: "inferred", changes: []string{"media.yaml", "networking.yaml"}, want: []string{"cluster", "media", "networking"}},
		{name: "disabled", disabled: true, want: []string{"cluster", "default", "media", "networking"}},
		{name: "empty active", want: []string{"cluster", "default", "media", "networking"}},
	} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s_%d_side", tc.name, count), func(t *testing.T) {
				t.Parallel()
				run := selectedRun(&Result{Warnings: []manifest.Warning{{Message: "global"}}})
				files := make(map[manifest.NamedResource]string)
				for _, namespace := range []string{"", "default", "media", "networking"} {
					name := namespace
					if name == "" {
						name = "cluster"
					}
					obj := &manifest.Kustomization{Name: name, Namespace: namespace}
					run.store.AddObject(obj)
					files[obj.Named()] = namespace + ".yaml"
				}
				run.index = indexForTest(files)
				if !tc.disabled {
					run.filter = change.NewFilter(change.NewSet(tc.changes), run.index, run.store)
				}
				runs := []Rendered{run}
				if count == 2 {
					runs = append(runs, run)
				}
				selected, err, _ := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID(tc.namespace, "")}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range selected {
					var names []string
					for _, obj := range result.Objects {
						names = append(names, obj.Named().Name)
					}
					slices.Sort(names)
					assert.Diff(t, names, tc.want)
					assert.Equal(t, result.Counts[manifest.KindKustomization], len(tc.want))
				}
			})
		}
	}
}

func TestSelect_AuthoredPrerequisites(t *testing.T) {
	t.Parallel()
	dep := testutil.HelmReleaseID("other", "dep")
	provider := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSetInputProvider, Namespace: "apps", Name: "provider"}
	rs := &manifest.ResourceSet{Name: "selected", Namespace: "apps"}
	rs.DependsOn = []fluxopv1.Dependency{{APIVersion: dep.Group + "/v2", Kind: dep.Kind, Name: dep.Name, Namespace: dep.Namespace}}
	manifest.KindOf(&manifest.RawObject{APIVersion: dep.Group + "/v2", Kind: dep.Kind}).Namespaced()
	rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: provider.Name}, {}}
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
		deps   []manifest.NamedResource
	}{
		{"ResourceSet", rs, []manifest.NamedResource{dep, provider}},
		{"DependsOn", &manifest.Kustomization{Name: "selected", Namespace: "apps"}, []manifest.NamedResource{dep}},
		{"Source", &manifest.Kustomization{Name: "selected", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{Failed: make(map[manifest.NamedResource]store.StatusInfo)})
			run.store.AddObject(tc.object)
			for _, id := range tc.deps {
				run.Result.Failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: "dependency failed"}
			}
			if tc.name == "DependsOn" {
				run.Result.DependsOn = map[manifest.NamedResource][]manifest.NamedResource{tc.object.Named(): tc.deps}
			}
			selected, err, _ := Select([]Rendered{run}, &FailuresError{Message: "raw failures"}, []manifest.NamedResource{tc.object.Named()}, nil)
			if err == nil || len(selected[0].Failed) != len(tc.deps) || len(selected[0].Unrelated) != 0 {
				t.Fatalf("authored prerequisites omitted: %+v %v", selected[0], err)
			}
		})
	}
}

func TestSelect_UnavailabilitySeparatesAncestryOwners(t *testing.T) {
	t.Parallel()
	present, absent := testutil.KustomizationID("flux-system", "a-present"), testutil.KustomizationID("flux-system", "z-absent")
	middle, oldRoot, newRoot := testutil.KustomizationID("flux-system", "middle"), testutil.KustomizationID("flux-system", "old-root"), testutil.KustomizationID("flux-system", "new-root")
	orig, current := selectedRun(&Result{}), selectedRun(&Result{})
	for _, id := range []manifest.NamedResource{present, absent} {
		orig.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	current.store.AddObject(&manifest.Kustomization{Name: present.Name, Namespace: present.Namespace})
	current.store.UpdateStatus(oldRoot, store.StatusFailed, "old ancestor failed")
	setParentsForTest(orig.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{present: middle, absent: middle, middle: oldRoot})
	setParentsForTest(current.Orchestrator, map[manifest.NamedResource]manifest.NamedResource{present: middle, middle: newRoot})
	selected, err, _ := Select([]Rendered{orig, current}, nil, []manifest.NamedResource{{Group: present.Group, Kind: present.Kind}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selected[1].Unavailable[present]; ok {
		t.Fatal("moved producer inherited old ancestry")
	}
	assert.Equal(t, selected[1].Unavailable[absent].Message, "old ancestor failed")
}

func TestSelect_SingleRunOmitsExternalDisclosures(t *testing.T) {
	t.Parallel()
	obj := &manifest.Kustomization{Name: "external", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "source"}}
	run := selectedRun(&Result{})
	run.store.AddObject(obj)
	run.store.UpdateStatus(obj.Named(), store.StatusReady, "skipped: external source")
	selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{{Group: obj.Named().Group, Kind: obj.Named().Kind}}, nil)
	if err != nil || selected[0].External != nil {
		t.Fatalf("single-run external disclosures: %+v %v", selected[0].External, err)
	}
}

func TestSelect_RawFluxLookalikeExcludedFromStatuses(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindBucket, manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{})
			obj := &manifest.RawObject{Kind: kind, APIVersion: "seaweed.seaweedfs.com/v1", Name: "foreign", Namespace: "apps"}
			run.store.AddObject(obj)
			run.store.UpdateStatus(obj.Named(), store.StatusReady, "")
			selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{{Group: obj.Named().Group, Kind: obj.Kind}}, nil)
			if err != nil || len(selected[0].Statuses) != 0 {
				t.Fatalf("foreign Flux lookalike entered roster: %+v %v", selected[0].Statuses, err)
			}
		})
	}
}

func TestSelect_ExplicitChartVersion(t *testing.T) {
	t.Parallel()
	for _, ns := range []struct{ namespace, source string }{{"", "apps"}, {"foreign", "foreign"}} {
		for _, tc := range []struct {
			name, kind, want string
			ref              *manifest.OCIRepositoryRef
			absent           bool
		}{
			{name: "digest", ref: &manifest.OCIRepositoryRef{Digest: "sha256:abc", SemVer: ">=1.0.0", Tag: "1.2.3"}, want: "sha256:abc"},
			{name: "semver", ref: &manifest.OCIRepositoryRef{SemVer: ">=1.0.0", Tag: "1.2.3"}, want: ">=1.0.0"},
			{name: "tag", ref: &manifest.OCIRepositoryRef{Tag: "1.2.3"}, want: "1.2.3"},
			{name: "empty reference", ref: &manifest.OCIRepositoryRef{}},
			{name: "nil reference"},
			{name: "missing source", absent: true},
			{name: "helm chart", kind: manifest.KindHelmChart, want: "2.3.4"},
			{name: "missing helm chart", kind: manifest.KindHelmChart, absent: true},
			{name: "nonapplicable source", kind: manifest.KindHelmRepository},
		} {
			t.Run(tc.name+"/source_"+ns.namespace, func(t *testing.T) {
				t.Parallel()
				run := selectedRun(&Result{})
				kind := tc.kind
				if kind == "" {
					kind = manifest.KindOCIRepository
				}
				hr := &manifest.HelmRelease{Name: "app", Namespace: "authored", Chart: manifest.HelmChart{Version: "9.9.9", RepoGroup: manifest.SourceDomain, RepoKind: kind, RepoNamespace: ns.namespace, RepoName: "chart"}}
				hr = hr.WithNamespace("apps").(*manifest.HelmRelease)
				run.store.AddObject(hr)
				if !tc.absent {
					switch kind {
					case manifest.KindHelmChart:
						source := &manifest.HelmChartSource{Name: "chart", Namespace: ns.source}
						source.Version = tc.want
						run.store.AddObject(source)
					case manifest.KindHelmRepository:
						run.store.AddObject(&manifest.HelmRepository{Name: "chart", Namespace: ns.source})
					default:
						source := &manifest.OCIRepository{Name: "chart", Namespace: ns.source}
						source.Reference = (*sourcev1.OCIRepositoryRef)(tc.ref)
						run.store.AddObject(source)
					}
				}
				selected, err, _ := Select([]Rendered{run}, nil, []manifest.NamedResource{testutil.HelmReleaseID("", "")}, nil)
				if err != nil || selected[0].ChartVersions != nil || hr.Chart.Version != "9.9.9" {
					t.Fatalf("explicit version replaced: %+v %v", selected[0].ChartVersions, err)
				}
				updated := hr.Clone()
				updated.Chart.Version = ""
				run.store.AddObject(updated)
				selected, err, _ = Select([]Rendered{run}, nil, []manifest.NamedResource{testutil.HelmReleaseID("", "")}, nil)
				if err != nil || selected[0].ChartVersions[hr.Named()] != tc.want {
					t.Fatalf("effective source version: %+v %v, want %q", selected[0].ChartVersions, err, tc.want)
				}
			})
		}
	}
}
