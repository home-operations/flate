package base_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/depwait"
	"github.com/home-operations/flate/internal/schedule"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_CycleSettlementOrder(t *testing.T) {
	t.Parallel()
	const message = "dependency cycle detected: a → b → a"
	ids := []manifest.NamedResource{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b")}
	want := fmt.Sprintf("%s: %s\n%s: %s\n", ids[0], message, ids[1], message)
	for first := range ids {
		t.Run(ids[first].Name+"_first", func(t *testing.T) {
			t.Parallel()
			st := store.New()
			for _, id := range ids {
				st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
				st.SetPreflight(id, message)
				st.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending})
			}
			c := base.New(st, nil)
			for i, id := range ids {
				e := controllertest.NewExecution(c, id)
				w := depwait.Waiter{Store: &e.Reads, Parent: id}
				cl := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: ids[1-i]}, manifest.DrainNone)
				assert.Equal(t, errors.Is(cl.Err, manifest.ErrInput), true)
				e.Close()
			}
			release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			done := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			for i, id := range ids {
				go func() {
					defer close(done[i])
					<-release[i]
					e := controllertest.NewExecution(c, id)
					defer e.Close()
					e.Reads.Require(manifest.DependencyRef{NamedResource: ids[1-i]})
					e.Settle(t.Context(), manifest.DrainForce)
				}()
			}
			close(release[first])
			<-done[first]
			close(release[1-first])
			<-done[1-first]
			var output strings.Builder
			for _, id := range ids {
				info, _ := st.GetStatus(id)
				assert.Equal(t, info.Outcome(), store.OutcomeFailed)
				fmt.Fprintf(&output, "%s: %s\n", id, info.String())
			}
			assert.Equal(t, output.String(), want)
		})
	}
}

func TestRequire_Classifications(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"ready", "blocked", "failed", "mixed", "force"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRequireClassificationsCase(t, &mode)
		},
		)
	}
}

func TestExecution_BlockedSuccessorRetainsAssessment(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"value": "old"}}
	gate := testutil.KustomizationID("apps", "gate")
	st.AddObjects([]manifest.BaseManifest{obj, input})
	st.UpdateStatus(gate, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
	c := base.New(st, nil)
	first := controllertest.NewExecution(c, obj.Named())
	first.Reads.GetObject(input.Named())
	_, _, output := first.Reads.GetEmission(obj.Named())
	output.Complete = true
	first.Emit(t.Context(), obj.Named(), &output, nil)
	output.Finish()
	defer first.Close()
	st.AddObject(
		&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{"value": "new"}},
	)
	assert.FatalIf(t, first.Valid(), "%v", "changed input left the predecessor current")
	second := controllertest.NewExecution(c, obj.Named())
	defer second.Close()
	type state struct {
		run *base.Execution
		obj *manifest.Kustomization
	}
	calls := 0
	retained := func() weak.Pointer[state] {
		s := &state{run: second, obj: obj}
		ref := weak.Make(s)
		second.Dispatch(
			t.Context(),
			manifest.DrainNone,
			s,
			func(s *state, ctx context.Context, got *manifest.Kustomization, level manifest.DrainLevel) error {
				assert.FatalIf(t, got != s.obj, "%v", "resumption lost the captured declaration")
				calls++
				return s.run.Require(ctx, []manifest.DependencyRef{{NamedResource: gate}}, level)
			},
		)
		return ref
	}()
	runtime.GC()
	assert.FatalIf(t, retained.Value() == nil, "%v", "blocked successor released its state")
	assert.FatalIf(
		t,
		second.Outcome() != schedule.Blocked || !second.Valid(),
		"%v",
		"current blocked assessment was invalidated by stale predecessor output",
	)
	second.Settle(t.Context(), manifest.DrainForce)
	assert.FatalIf(
		t,
		calls != 2 || second.Outcome() != schedule.DependencyFailed || !second.Valid(),
		"%v",
		"successor did not settle with its captured state",
	)
	runtime.GC()
	assert.FatalIf(t, retained.Value() != nil, "%v", "terminal failure retained successor state")
	runtime.KeepAlive(second)
	st.UpdateStatus(gate, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	assert.FatalIf(t, second.Valid(), "%v", "changed readiness did not invalidate the blocked assessment")
}

func TestRecordEmitted_PhantomSupersession(t *testing.T) {
	t.Parallel()
	for _, invalidated := range []bool{false, true} {
		t.Run(map[bool]string{false: "settled", true: "invalidated"}[invalidated], func(t *testing.T) {
			t.Parallel()
			st := store.New()
			obj := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			input := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "old"}}
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{obj, input})
			c := base.New(st, nil)
			first := controllertest.NewExecution(c, obj.Named())
			defer first.Close()
			first.Dispatch(
				t.Context(),
				manifest.DrainNone,
				first,
				func(first *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
					first.Reads.GetObject(input.Named())
					_, _, output := first.Reads.GetEmission(obj.Named())
					output.Children, output.Complete = []store.Child{{Obj: child, Doc: child.Snapshot()}}, true
					first.Emit(t.Context(), obj.Named(), &output, nil)
					output.Finish()
					return nil
				},
			)
			assert.FatalIf(t, !first.Valid(), "%v", "initial inventory was never settled")
			if invalidated {
				st.AddObject(
					&manifest.ConfigMap{
						Name:      input.Name,
						Namespace: input.Namespace,
						Data:      map[string]any{"value": "new"},
					},
				)
			}
			second := controllertest.NewExecution(c, obj.Named())
			defer second.Close()
			second.Dispatch(
				t.Context(),
				manifest.DrainNone,
				second,
				func(_ *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
					return errors.New("successor failed")
				},
			)
			if !second.Valid() || (st.GetObject(child.Named()) == nil) != invalidated {
				t.Fatalf(
					"failed successor retained invalid output or discarded settled output: valid=%t output=%v",
					second.Valid(),
					st.GetObject(child.Named()),
				)
			}
		})
	}
}

func TestExecution_CanonicalManifest(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"active", "missing", "suspended"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			found := mode != "missing"
			st := store.New()
			obj := &manifest.HelmRelease{
				ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
				Name:      "consumer",
				Namespace: "apps",
				Values:    map[string]any{"value": "old"},
			}
			fresh := obj.Clone()
			fresh.Values["value"] = "fresh"
			fresh.Suspend = mode == "suspended"
			var want *manifest.HelmRelease
			if found {
				st.AddObject(fresh)
				want = fresh
			}
			execution := controllertest.NewExecution(base.New(st, nil), obj.Named())
			defer execution.Close()
			got, ok := execution.Reads.Get[*manifest.HelmRelease](obj.Named())
			if ok != found || got != want || obj.Values["value"] != "old" {
				t.Fatalf("captured = %v, %v; want canonical %v, original=%v", got, ok, want, obj.Values)
			}
			called := false
			execution.Dispatch(
				t.Context(),
				manifest.DrainNone,
				execution,
				func(_ *base.Execution, _ context.Context, got *manifest.HelmRelease, _ manifest.DrainLevel) error {
					called = true
					assert.FatalIf(t, got != want, "dispatched declaration = %v, want captured %v", got, want)
					return nil
				},
			)
			assert.FatalIf(t, called != (mode == "active"), "reconcile called=%v for %s", called, mode)
			info, has := st.GetStatus(obj.Named())
			if mode == "suspended" {
				assert.FatalIf(
					t,
					!has || info.Outcome() != store.OutcomeSuspended,
					"suspended refresh status: %+v",
					info,
				)
			} else {
				assert.FatalIf(t, mode == "missing" && has, "missing declaration changed status: %+v", info)
			}
		})
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRequire_ReadyAllocation(t *testing.T) {
	st := store.New()
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	st.AddObject(dependency)
	st.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	execution := controllertest.NewExecution(base.New(st, nil), testutil.HelmReleaseID("apps", "consumer"))
	defer execution.Close()
	deps := []manifest.DependencyRef{{NamedResource: dependency.Named()}}
	ctx := t.Context()
	{
		allocations := testing.AllocsPerRun(100, func() {
			if err := execution.Require(ctx, deps, manifest.DrainNone); err != nil {
				t.Fatal(err)
			}
		})
		assert.FatalIf(t, allocations != 0, "ready classification allocations=%v, want 0", allocations)
	}
}

func TestRequire_MultiplePredicates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		expressions []string
		status      store.Status
		level       manifest.DrainLevel
		want        schedule.Kind
	}{
		{name: "authored then builtin", expressions: []string{"false", ""}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "builtin then authored", expressions: []string{"", "false"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "true then false", expressions: []string{"true", "false"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "false then true", expressions: []string{"false", "true"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "multiple failed", expressions: []string{"false", "1 == 2", "not valid CEL"}, status: store.StatusReady, level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "multiple blocked", expressions: []string{"false", "1 == 2"}, status: store.StatusReady, want: schedule.Blocked},
		{name: "expression replaces own builtin", expressions: []string{"true"}, status: store.StatusPending, want: schedule.Terminal},
		{name: "separate builtin still required", expressions: []string{"true", ""}, status: store.StatusPending, want: schedule.Blocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRequireMultiplePredicatesCase(t, &tc)
		},
		)
	}
}

type countingSettlerError struct {
	error

	calls int
}

func (s *countingSettlerError) Unwrap() error { return s.error }

func (s *countingSettlerError) Settle(context.Context, manifest.DrainLevel) error {
	s.calls++
	return nil
}

func TestExecution_SettleInvalidReads(t *testing.T) {
	t.Parallel()
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprintf("stale=%t", stale), func(t *testing.T) {
			t.Parallel()
			testExecutionSettleInvalidReadsCase(t, &stale)
		},
		)
	}
}

func TestExecution_ResumedPredicates(t *testing.T) {
	t.Parallel()
	st := store.New()
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{consumer, dependency})
	st.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
	defer execution.Close()
	deps := []manifest.DependencyRef{
		{NamedResource: dependency.Named(), ReadyExpr: new("false")},
		{NamedResource: dependency.Named()},
	}
	calls := 0
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		execution,
		func(execution *base.Execution, ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
			calls++
			assert.FatalIf(t, obj != consumer, "%v", "resumption replaced the captured declaration")
			return execution.Require(ctx, deps, level)
		},
	)
	assert.FatalIf(t, execution.Outcome() != schedule.Blocked ||
		!slices.Equal(
			execution.Blocked,
			[]manifest.NamedResource{dependency.Named()},
		), "%v", "false predicate did not park the execution")
	execution.Settle(t.Context(), manifest.DrainForce)
	assert.FatalIf(t, calls != 2, "reconcile calls=%d, want initial and retained callback", calls)
	info, _ := st.GetStatus(consumer.Named())
	assert.FatalIf(t, execution.Outcome() != schedule.DependencyFailed || !info.IsDependencyFailure() ||
		!slices.Equal(
			info.BlockedBy(),
			[]manifest.NamedResource{dependency.Named()},
		), "resumed execution lost its false predicate: %+v", info)
	want := make([]store.Gate, len(deps))
	for i, dep := range deps {
		want[i].DependencyRef = dep
	}
	{
		got := slices.Collect(execution.Reads.Gates())
		assert.FatalIf(t, !slices.Equal(got, want), "resumption duplicated or lost predicates: %v", got)
	}
}

func TestRequire_RetainsArtifactFactsThroughSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ reverse, dataWait bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("reverse=%t/data=%t", tc.reverse, tc.dataWait), func(t *testing.T) {
			t.Parallel()
			testRequireRetainsArtifactFactsThroughSettlementCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestDispatch_ExactMaterializers(t *testing.T) {
	t.Parallel()
	for _, state := range []store.Status{store.StatusPending, store.StatusFailed, store.StatusReady} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			id := testutil.HelmReleaseID("apps", "missing")
			owner := &manifest.ResourceSet{Name: "a-owner", Namespace: "apps"}
			peer := &manifest.ResourceSet{Name: "z-failed", Namespace: "apps"}
			unrelated := &manifest.ResourceSet{Name: "unrelated", Namespace: "other"}
			st.AddObjects([]manifest.BaseManifest{owner, peer, unrelated})
			st.UpdateStatus(owner.Named(), store.StatusInfo{Status: state, Reason: "", Message: "owner state"})
			st.UpdateStatus(
				peer.Named(),
				store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "peer failed"},
			)
			st.UpdateStatus(
				unrelated.Named(),
				store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "unrelated"},
			)
			a := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "a"}
			z := a
			z.Name = "z"
			other := id
			other.Namespace = "other"
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {
					Productions: []manifest.Production{
						{Owner: owner.Named(), Source: z},
						{Owner: peer.Named(), Source: a},
						{Owner: owner.Named(), Source: a},
						{},
						{Owner: id},
					},
				},
				other: {Productions: []manifest.Production{{Owner: unrelated.Named(), Source: a}}},
			}})
			run := controllertest.NewExecution(base.New(st, nil), id)
			run.Dispatch(
				t.Context(),
				manifest.DrainNone,
				struct{}{},
				func(_ struct{}, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
					t.Error("missing declaration reached body")
					return nil
				},
			)
			info, _ := st.GetStatus(id)
			if state == store.StatusPending {
				blocked, isBlocked := run.Blocked, run.Outcome() == schedule.Blocked
				assert.FatalIf(
					t,
					!isBlocked || !slices.Equal(blocked, []manifest.NamedResource{owner.Named()}),
					"blocked=%v error=%v, want exact pending owner once",
					blocked,
					info,
				)
			} else {
				failed, isFailed := info.BlockedBy(), info.IsDependencyFailure()
				assert.FatalIf(
					t,
					!isFailed || !slices.Equal(failed, []manifest.NamedResource{owner.Named(), peer.Named()}),
					"failure=%v, want sorted exact owners once",
					info,
				)
				assert.FatalIf(
					t,
					state == store.StatusReady && !strings.Contains(info.Message, "declaration not emitted"),
					"ready owner reason = %q",
					info.Message,
				)
			}
		})
	}
}

func testRequireClassificationsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	id := testutil.HelmReleaseID("apps", "consumer")
	ready := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "ready")}
	failed := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "failed")}
	pending := manifest.DependencyRef{NamedResource: testutil.KustomizationID("apps", "pending")}
	st.UpdateStatus(ready.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	st.UpdateStatus(
		failed.NamedResource,
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "render failed"},
	)
	st.UpdateStatus(
		pending.NamedResource,
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"},
	)
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "informative"})
	st.AddObject(
		&manifest.HelmRelease{
			ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
			Name:      id.Name,
			Namespace: id.Namespace,
			Suspend:   (*testCaseInput) != "ready",
		},
	)
	deps := []manifest.DependencyRef{ready}
	level := manifest.DrainNone
	switch *testCaseInput {
	case "blocked":
		deps = append(deps, pending)
	case "failed":
		deps = append(deps, failed)
	case "mixed", "force":
		deps = append(deps, pending, failed)
		if (*testCaseInput) == "force" {
			level = manifest.DrainForce
		}
	}
	err := controllertest.NewExecution(base.New(st, nil), id).Require(t.Context(), deps, level)
	info, _ := st.GetStatus(id)
	assert.FatalIf(
		t,
		info.Status != store.StatusReady || info.Message != "informative",
		"gate changed caller status: %+v",
		info,
	)
	switch *testCaseInput {
	case "ready":
		assert.FatalIf(t, err != nil, "%v", err)
	case "blocked", "mixed":
		blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
		assert.FatalIf(
			t,
			!ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{pending.NamedResource}) ||
				errors.Is(err, manifest.ErrFlux),
			"blocked gate = %v, want only pending dependency",
			err,
		)
	case "failed", "force":
		failure, ok := errors.AsType[*manifest.DependencyFailedError](err)
		want := []manifest.NamedResource{failed.NamedResource}
		if (*testCaseInput) == "force" {
			want = append(want, pending.NamedResource)
		}
		assert.FatalIf(t, !ok || failure.Parent != id || !slices.Equal(failure.Failed, want) ||
			!errors.Is(err, manifest.ErrInput) ||
			!errors.Is(err, manifest.ErrFlux), "failed gate = %#v, want sorted dependencies %v", err, want)
		if failure.Reasons[failed.NamedResource].Message != "render failed" || (*testCaseInput) ==
			"force" && failure.Reasons[pending.NamedResource].Message != "not ready" {
			t.Fatalf("failure reasons = %v", failure.Reasons)
		}
	}
}

func testRequireMultiplePredicatesCase(t *testing.T, testCaseInput *struct {
	name string

	expressions []string
	status      store.Status
	level       manifest.DrainLevel
	want        schedule.Kind
},
) {
	t.Helper()

	st := store.New()
	obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	consumer := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "consumer",
		Namespace: "apps",
	}
	st.AddObjects([]manifest.BaseManifest{obj, consumer})
	st.UpdateStatus(obj.Named(), store.StatusInfo{Status: testCaseInput.status, Reason: "", Message: ""})
	var deps []manifest.DependencyRef
	if len(testCaseInput.expressions) > 0 {
		deps = make([]manifest.DependencyRef, 0, len(testCaseInput.expressions))
	}
	for _, expr := range testCaseInput.expressions {
		deps = append(deps, manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new(expr)})
	}
	var reason string
	closeExecutions := make([]func(), 0, 2)
	defer func() {
		for _, closeExecution := range slices.Backward(closeExecutions) {
			closeExecution()
		}
	}()
	for range 2 {
		execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
		closeExecutions = append(closeExecutions, execution.Close)
		err := execution.Require(t.Context(), deps, testCaseInput.level)
		switch testCaseInput.want {
		case schedule.DependencyFailed:
			failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok || !slices.Equal(failed.Failed, []manifest.NamedResource{obj.Named()}) ||
				len(failed.Reasons) != 1, "failed identities were lost or duplicated: %v", err)
			if reason != "" && reason != failed.Reasons[obj.Named()].Message {
				t.Fatalf(
					"registration order changed diagnostic: %q vs %q",
					reason,
					failed.Reasons[obj.Named()].Message,
				)
			}
			reason = failed.Reasons[obj.Named()].Message
		case schedule.Blocked:
			blocked, ok := errors.AsType[*depwait.ErrBlocked](err)
			assert.FatalIf(
				t,
				!ok || !slices.Equal(blocked.Deps, []manifest.NamedResource{obj.Named()}),
				"blocked identities were lost or duplicated: %v",
				err,
			)
		case schedule.Terminal:
			assert.FatalIf(t, err != nil, "%v", err)
		default:
			assert.FatalIf(t, err != nil, "%v", err)
		}
		_ = execution.Require(t.Context(), deps, testCaseInput.level)
		{
			got := slices.Collect(execution.Reads.Gates())
			assert.FatalIf(
				t,
				len(got) != len(testCaseInput.expressions),
				"repeated registration changed predicates: %v",
				got,
			)
		}
		slices.Reverse(deps)
	}
}

func testExecutionSettleInvalidReadsCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	st := store.New()
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	input := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "old"}}
	st.AddObjects([]manifest.BaseManifest{consumer, input})
	st.UpdateStatus(
		consumer.Named(),
		store.StatusInfo{Status: store.StatusPending, Message: "waiting for input"},
	)
	execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
	t.Cleanup(execution.Close)
	pending := &countingSettlerError{error: &depwait.ErrBlocked{Deps: []manifest.NamedResource{input.Named()}}}
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		struct{}{},
		func(_ struct{}, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
			execution.Reads.GetObject(input.Named())
			return pending
		},
	)
	assert.FatalIf(t, execution.Outcome() != schedule.Blocked ||
		!slices.Equal(execution.Blocked, []manifest.NamedResource{input.Named()}) ||
		pending.calls != 0, "%v", "dispatch did not retain the blocked settler")
	before, _ := st.GetStatus(consumer.Named())
	if *testCaseInput {
		replacement := *input
		replacement.Data = map[string]any{"value": "new"}
		st.AddObject(&replacement)
	}
	assert.FatalIf(
		t,
		execution.Reads.Valid() == (*testCaseInput),
		"%v",
		"captured input validity does not match replacement",
	)
	execution.Settle(t.Context(), manifest.DrainForce)
	after, _ := st.GetStatus(consumer.Named())
	if *testCaseInput {
		if pending.calls != 0 {
			t.Fatalf("stale execution called retained settler %d times", pending.calls)
		}
		assert.FatalIf(t, execution.Outcome() != schedule.Blocked ||
			!slices.Equal(execution.Blocked, []manifest.NamedResource{input.Named()}) ||
			after != before, "stale settlement changed retained assessment or status: %+v", after)
	} else if pending.calls != 1 || execution.Outcome() != schedule.Terminal || after.Status != store.StatusReady {
		t.Fatalf(
			"valid settlement did not run retained work: calls=%d outcome=%v status=%+v",
			pending.calls,
			execution.Outcome(),
			after,
		)
	}
}

func testRequireRetainsArtifactFactsThroughSettlementCase(
	t *testing.T,
	testCaseInput *struct {
		reverse bool

		dataWait bool
	},
) {
	t.Helper()

	st := store.New()
	consumer := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "consumer",
		Namespace: "apps",
	}
	source := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{consumer, source})
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	st.SetArtifact(source.Named(), &store.SourceArtifact{})
	execution := controllertest.NewExecution(base.New(st, nil), consumer.Named())
	defer execution.Close()
	deps := []manifest.DependencyRef{
		{NamedResource: source.Named(), ReadyExpr: new("true")},
		{NamedResource: source.Named(), ReadyExpr: new("true"), Artifact: true, ChartSource: true},
	}
	if testCaseInput.reverse {
		slices.Reverse(deps)
	}
	rendered := false
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		execution,
		func(execution *base.Execution, ctx context.Context, _ *manifest.HelmRelease, level manifest.DrainLevel) error {
			err := execution.Require(ctx, deps, level)
			if !testCaseInput.dataWait {
				return err
			}
			return execution.AwaitData(
				ctx,
				base.DataWait{
					Resume: func(context.Context, manifest.DrainLevel, []manifest.NamedResource) error {
						rendered = true
						return nil
					},
				},
				level,
			)
		},
	)
	if execution.Outcome() != schedule.Blocked ||
		!slices.Equal(execution.Blocked, []manifest.NamedResource{source.Named()}) {
		t.Fatalf("missing content did not block: %v %v", execution.Outcome(), execution.Blocked)
	}
	execution.Settle(t.Context(), manifest.DrainCascade)
	info, _ := st.GetStatus(consumer.Named())
	if rendered || execution.Outcome() != schedule.Terminal || info.Status != store.StatusFailed ||
		!strings.Contains(
			info.String(),
			"chart source "+source.Named().String()+" artifact content unavailable",
		) {
		t.Fatalf("settlement lost artifact facts: outcome=%v status=%+v", execution.Outcome(), info)
	}
}
