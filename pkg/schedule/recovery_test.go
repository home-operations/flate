package schedule_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type registeredDispatcher struct {
	controller *base.Controller
	reconcile  func(context.Context, *base.Execution, schedule.NodeID, manifest.DrainLevel) error
}

func TestScheduler_BoundedDAG(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			runScaleDAG(t, workers, 160)
		})
	}
}

func TestScheduler_ScaleDAG(t *testing.T) {
	t.Parallel()
	if os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip("set FLATE_E8_SLOW=1 for 1000 producers and 10000 publications")
	}
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			runScaleDAG(t, workers, 1000)
		})
	}
}

func runScaleDAG(t *testing.T, workers, producers int) {
	t.Helper()
	objects := make([]manifest.BaseManifest, producers)
	deps := make(map[manifest.NamedResource][]manifest.DependencyRef, producers)
	ids := make([]manifest.NamedResource, producers)
	for n := range producers {
		obj := &manifest.Kustomization{Name: fmt.Sprintf("producer-%04d", n), Namespace: "apps"}
		objects[n], ids[n] = obj, obj.Named()
	}
	for n := range producers {
		switch {
		case n > 0 && n < 64:
			deps[ids[n]] = []manifest.DependencyRef{{NamedResource: ids[n-1]}}
		case n >= 64 && n < 128:
			deps[ids[n]] = []manifest.DependencyRef{{NamedResource: ids[0]}}
		case n == 128:
			for _, dep := range ids[64:128] {
				deps[ids[n]] = append(deps[ids[n]], manifest.DependencyRef{NamedResource: dep})
			}
		}
	}
	runScaleDAGCompletion(
		t,
		&runScaleDAGCompletionState{
			workers:   &workers,
			producers: &producers,
			objects:   &objects,
			deps:      &deps,
			ids:       &ids,
		},
	)
}

func (d *registeredDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
) schedule.Result {
	e := controllertest.NewExecution(d.controller, id)
	e.Dispatch[*manifest.Kustomization](
		ctx,
		level,
		e,
		func(e *base.Execution, ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
			return d.reconcile(ctx, e, obj.Named(), level)
		},
	)
	return schedule.Result{Assessment: e}
}

func (d *registeredDispatcher) Sweep(context.Context, bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

func registeredScheduler(
	t *testing.T,
	workers int,
	objects []manifest.BaseManifest,
	reconcile func(context.Context, *base.Execution, schedule.NodeID, manifest.DrainLevel) error,
) (*schedule.Scheduler, *store.Store) {
	t.Helper()
	st, tasks := store.New(), task.NewBounded(workers)
	st.AddObjects(objects)
	c := base.New(st, tasks)
	c.StartLifecycle()
	s := schedule.New(tasks, &registeredDispatcher{controller: c, reconcile: reconcile})
	t.Cleanup(
		st.AddListener(
			store.EventObjectAdded,
			func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) },
			true,
		),
	)
	for _, event := range []store.EventKind{store.EventArtifactUpdated, store.EventStatusUpdated} {
		t.Cleanup(st.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() }, true))
	}
	return s, st
}

func TestScheduler_RegisteredDependencyGraphs(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			testSchedulerRegisteredDependencyGraphsCase(
				t,
				&workers,
			)
		},
		)
	}
}

func TestDispatchNode_SelfStatusSettlement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, expression string
		wantAssessments  int64
		feedback         bool
	}{
		{name: "bookkeeping", wantAssessments: 1},
		{name: "lazy self", expression: `dep.metadata.name == "dependency"`, wantAssessments: 1},
		{name: "self metadata", expression: `self.metadata.namespace == dep.metadata.namespace`, wantAssessments: 1},
		{name: "self conditions", expression: `self.status.conditions.size() >= 0`, wantAssessments: 3},
		{name: "feedback", expression: `self.status.conditions.size() >= 0`, feedback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testDispatchNodeSelfStatusSettlementCase(t, &tc)
		},
		)
	}
}

func TestScheduler_RegisteredFailureRecovery(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			testSchedulerRegisteredFailureRecoveryCase(
				t,
				&workers,
			)
		},
		)
	}
}

func TestScheduler_RegisteredIrrelevantPublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		consumer, driver := &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "driver",
			Namespace: "ns",
		}
		read, published := make(chan struct{}), make(chan struct{})
		var renders atomic.Int64
		var st *store.Store
		s, current := registeredScheduler(
			t,
			4,
			[]manifest.BaseManifest{consumer, driver},
			func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
				if id == driver.Named() {
					e.Tasks.YieldSlot(func() { <-read })
					st.AddObject(&manifest.Secret{Name: "irrelevant", Namespace: "ns"})
					close(published)
					return nil
				}
				renders.Add(1)
				e.Reads.GetObject(
					manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "optional"},
				)
				close(read)
				e.Tasks.YieldSlot(func() { <-published })
				return nil
			},
		)
		st = current
		assert.NoError(t, s.Run(t.Context()))
		assert.Equal(t, renders.Load(), int64(1))
		assert.Equal(t, len(s.Results()), 2)
	})
}

func TestScheduler_RegisteredFixpointBound(t *testing.T) {
	t.Parallel()
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked=%v", blocked), func(t *testing.T) {
			t.Parallel()
			testSchedulerRegisteredFixpointBoundCase(
				t,
				&blocked,
			)
		},
		)
	}
}

func TestScheduler_RegisteredSelection(t *testing.T) {
	t.Parallel()
	for _, matching := range []bool{false, true} {
		t.Run(fmt.Sprintf("matching_bucket=%v", matching), func(t *testing.T) {
			t.Parallel()
			testSchedulerRegisteredSelectionCase(t, &matching)
		},
		)
	}
}

func testSchedulerRegisteredDependencyGraphsCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	chain := make(map[string][]string, 40)
	for i := range 40 {
		chain[fmt.Sprintf("node-%02d", i)] = []string{fmt.Sprintf("node-%02d", i+1)}
	}
	for _, tc := range []struct {
		name  string
		graph map[string][]string
		want  store.Status
	}{
		{name: "linear", graph: map[string][]string{"a": {"b"}, "b": {"c"}, "c": nil}, want: store.StatusReady},
		{name: "diamond", graph: map[string][]string{"d": {"b", "c"}, "b": {"a"}, "c": {"a"}, "a": nil}, want: store.StatusReady},
		{name: "dangling", graph: map[string][]string{"a-top": {"z-leaf"}, "z-leaf": {"absent"}}, want: store.StatusFailed},
		{name: "cycle", graph: map[string][]string{"a": {"b"}, "b": {"a"}}, want: store.StatusFailed},
		{name: "long dangling chain", graph: chain, want: store.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRegisteredDependencyGraph(
				t,
				runRegisteredDependencyGraphInputs{testCaseInput: &testCaseInput, tc: &tc},
			)
		},
		)
	}
}

func testDispatchNodeSelfStatusSettlementCase(
	t *testing.T,
	testCaseInput *struct {
		name            string
		expression      string
		wantAssessments int64
		feedback        bool
	},
) {
	t.Helper()

	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}
	dependency := &manifest.Kustomization{Name: "dependency", Namespace: "ns"}
	var assessments, renders atomic.Int64
	s, st := registeredScheduler(
		t,
		4,
		[]manifest.BaseManifest{consumer, dependency},
		func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			if id == dependency.Named() {
				return nil
			}
			count := assessments.Add(1)
			e.SetPendingUnlessReady(id, "working")
			if err := e.Require(
				ctx,
				[]manifest.DependencyRef{
					{NamedResource: dependency.Named(), ReadyExpr: new(testCaseInput.expression)},
				},
				level,
			); err != nil {
				return err
			}
			if testCaseInput.feedback {
				e.Store.UpdateStatus(
					id,
					store.StatusInfo{
						Status:  store.StatusReady,
						Reason:  "",
						Message: fmt.Sprintf("feedback %d", count),
					},
				)
				return nil
			}
			fp, reused, err := e.FingerprintDedup(t.Context(), id, "unchanged output", &store.Emission{})
			if reused || err != nil {
				return err
			}
			renders.Add(1)
			_, _, output := e.Reads.GetEmission(id)
			output.Fingerprint, output.Complete = fp, true
			e.Emit(t.Context(), id, &output, nil)
			output.Finish()
			return nil
		},
	)
	st.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.Seed([]schedule.NodeID{consumer.Named()})
	err := s.Run(t.Context())
	if testCaseInput.feedback {
		{
			assert.FatalIf(
				t,
				err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches"),
				"feedback did not reach its bound: %v",
				err,
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, assessments.Load(), testCaseInput.wantAssessments)
	assert.Equal(t, renders.Load(), int64(1))
}

func testSchedulerRegisteredFailureRecoveryCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	for _, data := range []bool{false, true} {
		t.Run(fmt.Sprintf("data=%v", data), func(t *testing.T) {
			t.Parallel()
			runRegisteredFailureRecovery(
				t,
				runRegisteredFailureRecoveryInputs{testCaseInput: &testCaseInput, data: &data},
			)
		},
		)
	}
}

func testSchedulerRegisteredFixpointBoundCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		runRegisteredFixpointFeedback(
			t,
			runRegisteredFixpointFeedbackInputs{testCaseInput: &testCaseInput},
		)
	},
	)
}

func testSchedulerRegisteredSelectionCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		selector, driver := &manifest.Kustomization{
			Name:      "selector",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "driver",
			Namespace: "ns",
		}
		read, published := make(chan struct{}), make(chan struct{})
		var runs atomic.Int64
		var st *store.Store
		s, current := registeredScheduler(
			t,
			4,
			[]manifest.BaseManifest{selector, driver},
			func(_ context.Context, e *base.Execution, id schedule.NodeID, _ manifest.DrainLevel) error {
				if id == driver.Named() {
					e.Tasks.YieldSlot(func() { <-read })
					if *testCaseInput {
						st.AddObject(&manifest.ResourceSetInputProvider{Name: "late", Namespace: "ns"})
					} else {
						st.AddObject(&manifest.Secret{Name: "unrelated", Namespace: "ns"})
					}
					close(published)
					return nil
				}
				run := runs.Add(1)
				providers := e.Reads.ListObjects(
					manifest.FluxOperatorDomain,
					manifest.KindResourceSetInputProvider,
				)
				if run == 1 {
					assert.Equal(t, len(providers), 0)
					close(read)
					e.Tasks.YieldSlot(func() { <-published })
				} else {
					assert.Equal(t, len(providers), 1)
				}
				return nil
			},
		)
		st = current
		assert.NoError(t, s.Run(t.Context()))
		want := int64(1)
		if *testCaseInput {
			want = 2
		}
		assert.Equal(t, runs.Load(), want)
	})
}

type runScaleDAGCompletionState struct {
	workers   *int
	producers *int
	objects   *[]manifest.BaseManifest
	deps      *map[manifest.NamedResource][]manifest.DependencyRef
	ids       *[]manifest.NamedResource
}

func runScaleDAGCompletion(t *testing.T, scope *runScaleDAGCompletionState) {
	t.Helper()
	firstWave := min((*scope.workers), (*scope.producers)-128)
	started, release := make(chan struct{}, firstWave), make(chan struct{})
	var active, overBound, roots atomic.Int64
	s, st := registeredScheduler(
		t,
		(*scope.workers),
		(*scope.objects),
		func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
			current := active.Add(1)
			defer active.Add(-1)
			if current > int64((*scope.workers)) {
				overBound.Add(1)
			}
			if len((*scope.deps)[id]) == 0 && roots.Add(1) <= int64(firstWave) {
				started <- struct{}{}
				<-release
			}
			if err := e.Require(ctx, (*scope.deps)[id], level); err != nil {
				return err
			}
			_, _, next := e.Reads.GetEmission(id)
			defer next.Finish()
			for n := range 10 {
				next.Children = append(
					next.Children,
					store.Child{
						Obj: &manifest.ConfigMap{
							Name:      fmt.Sprintf("%s-object-%02d", id.Name, n),
							Namespace: id.Namespace,
						},
					},
				)
			}
			next.Complete = true
			return e.Emit(t.Context(), id, &next, nil)
		},
	)
	var publications atomic.Int64
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		if id.Kind == manifest.KindConfigMap && payload != nil {
			publications.Add(1)
		}
	}, false)
	defer unsubscribe()
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()
	for range firstWave {
		<-started
	}
	assert.Equal(t, active.Load(), int64(firstWave))
	close(release)
	assert.NoError(t, <-done)
	assert.Equal(t, t.Context().Err(), nil)
	assert.Equal(t, active.Load(), int64(0))
	assert.Equal(t, overBound.Load(), int64(0))
	results := s.Results()
	assert.Equal(t, len(results), (*scope.producers))
	for _, id := range *scope.ids {
		result, ok := results[id]
		assert.Equal(t, ok, true)
		if ok {
			assert.Equal(t, result.Assessment.Outcome(), schedule.Terminal)
		}
		status, ok := st.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, store.StatusReady)
		for n := range 10 {
			child := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Name:      fmt.Sprintf("%s-object-%02d", id.Name, n),
				Namespace: id.Namespace,
			}
			assert.Equal(t, st.GetObject(child) != nil, true)
		}
	}
	assert.Equal(t, publications.Load(), int64((*scope.producers)*10))
	assert.Equal(t, len(st.ListObjects("", manifest.KindConfigMap)), (*scope.producers)*10)
	assert.Equal(t, len(st.FailedResources()), 0)
}
