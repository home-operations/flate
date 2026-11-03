package schedule_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
	runScaleDAGCompletion(t, &runScaleDAGCompletionState{workers: &workers, producers: &producers, objects: &objects, deps: &deps, ids: &ids})
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

//nolint:paralleltest // Subtests share a synctest bubble and cannot run in parallel.
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
			objects := make([]manifest.BaseManifest, 0, len(tc.graph))
			counts := make(map[string]*atomic.Int64, len(tc.graph))
			for name := range tc.graph {
				objects = append(objects, &manifest.Kustomization{Name: name, Namespace: "ns"})
				counts[name] = &atomic.Int64{}
			}
			s, st := registeredScheduler(
				t, (*testCaseInput), objects,
				func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
					deps := make([]manifest.DependencyRef, 0, len(tc.graph[id.Name]))
					for _, dep := range tc.graph[id.Name] {
						deps = append(
							deps,
							manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", dep)},
						)
					}
					if err := e.Require(ctx, deps, level); err != nil {
						return err
					}
					counts[id.Name].Add(1)
					return nil
				},
			)
			if tc.name == "cycle" {
				for _, obj := range objects {
					st.SetPreflight(obj.Named(), "dependency cycle detected")
				}
			}
			assert.NoError(t, s.Run(t.Context()))
			for name := range tc.graph {
				status, ok := st.GetStatus(testutil.KustomizationID("ns", name))
				assert.Equal(t, ok, true)
				assert.Equal(t, status.Status, tc.want)
				var wantBlockers []manifest.NamedResource
				if tc.want == store.StatusFailed && tc.name != "cycle" {
					for _, name := range tc.graph[name] {
						wantBlockers = append(wantBlockers, testutil.KustomizationID("ns", name))
					}
					slices.SortFunc(wantBlockers, func(a, b manifest.NamedResource) int { return a.Compare(b) })
				}
				assert.Diff(t, status.BlockedBy(), wantBlockers)

				{
					assert.ErrorIf(
						t,
						tc.want == store.StatusFailed && status.Message == "",
						"%s: terminal failure has no diagnostic",
						name,
					)
				}
				wantRenders := int64(0)
				if tc.want == store.StatusReady {
					wantRenders = 1
				}
				assert.Equal(t, counts[name].Load(), wantRenders)
			}
			assert.Equal(t, len(s.Results()), len(tc.graph))
			assert.Equal(t, t.Context().Err(), nil)
			failures := st.FailedResources()
			if tc.want == store.StatusFailed {
				assert.Equal(t, len(failures), len(tc.graph))
				for name := range tc.graph {
					_, ok := failures[testutil.KustomizationID("ns", name)]
					assert.Equal(t, ok, true)
				}
			} else {
				assert.Equal(t, len(failures), 0)
			}
		})
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
			synctest.Test(t, func(t *testing.T) {
				consumer := &manifest.Kustomization{Name: "consumer", Namespace: "ns"}
				driver := &manifest.Kustomization{Name: "driver", Namespace: "ns"}
				dependency := &manifest.Kustomization{Name: "dependency", Namespace: "ns"}
				read, published, failed := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var runs atomic.Int64
				var st *store.Store
				s, current := registeredScheduler(
					t, (*testCaseInput), []manifest.BaseManifest{consumer, driver, dependency},
					func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
						if id == dependency.Named() {
							if e.Reads.GetObject(id).(*manifest.Kustomization).Path == "recovered" {
								return nil
							}
							return manifest.NewError(manifest.ErrInput, nil, "initial failure")
						}
						if id == driver.Named() {
							e.Tasks.YieldSlot(func() { <-read })
							if data {
								st.AddObject(
									&manifest.ConfigMap{
										Name:      "late",
										Namespace: "ns",
										Data:      map[string]any{"value": "ready"},
									},
								)
							} else {
								replacement := dependency.Clone()
								replacement.Path = "recovered"
								st.AddObject(replacement)
								st.UpdateStatus(
									dependency.Named(),
									store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
								)
							}
							close(published)
							return nil
						}
						if !data {
							e.Tasks.YieldSlot(func() { <-failed })
						}
						run := runs.Add(1)
						var err error
						if data {
							if e.Reads.GetObject(
								manifest.NamedResource{
									Kind:      manifest.KindConfigMap,
									Namespace: "ns",
									Name:      "late",
								},
							) == nil {
								err = manifest.NewError(manifest.ErrInput, nil, "data unavailable")
							}
						} else {
							err = e.Require(
								ctx,
								[]manifest.DependencyRef{{NamedResource: dependency.Named()}},
								level,
							)
						}
						if run == 1 {
							close(read)
							e.Tasks.YieldSlot(func() { <-published })
						}
						return err
					},
				)
				st = current
				u := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
					if id == dependency.Named() && payload.(store.StatusInfo).Status == store.StatusFailed {
						close(failed)
					}
				}, false)
				defer u()
				assert.NoError(t, s.Run(t.Context()))
				status, _ := st.GetStatus(consumer.Named())
				assert.Equal(t, status.Status, store.StatusReady)
				assert.Equal(t, runs.Load(), int64(2))
				assert.Equal(t, t.Context().Err(), nil)
				assert.Equal(t, len(status.BlockedBy()), 0)
				assert.Equal(t, len(s.Results()), 3)
				failures := st.FailedResources()
				if data {
					assert.Equal(t, len(failures), 1)
					_, failedDependency := failures[dependency.Named()]
					assert.Equal(t, failedDependency, true)
				} else {
					assert.Equal(t, len(failures), 0)
				}
			})
		})
	}
}

func testSchedulerRegisteredFixpointBoundCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		x, y := &manifest.Kustomization{
			Name:      "x",
			Namespace: "ns",
		}, &manifest.Kustomization{
			Name:      "y",
			Namespace: "ns",
		}
		started, held := make(chan struct{}, 2), make(chan struct{}, 2)
		release := make(chan struct{})
		var active, total atomic.Int64
		counts := map[schedule.NodeID]*atomic.Int64{x.Named(): {}, y.Named(): {}}
		s, st := registeredScheduler(
			t,
			4,
			[]manifest.BaseManifest{x, y},
			func(ctx context.Context, e *base.Execution, id schedule.NodeID, level manifest.DrainLevel) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				active.Add(1)
				defer active.Add(-1)
				run, count := counts[id].Add(1), total.Add(1)
				other := x.Named()
				if id == other {
					other = y.Named()
				}
				e.Reads.GetObject(
					manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: other.Name},
				)
				if run == 1 {
					started <- struct{}{}
					e.Tasks.YieldSlot(func() { <-release })
				}
				if (*testCaseInput) && count >= 80 {
					held <- struct{}{}
					<-ctx.Done()
				}
				_, _, next := e.Reads.GetEmission(id)
				defer next.Finish()
				child := &manifest.ConfigMap{Name: id.Name, Namespace: "ns", Data: map[string]any{"value": run}}
				next.Children = []store.Child{{Obj: child, Doc: child.Snapshot()}}
				next.Manifests, next.Complete = testutil.DocumentOccurrences(
					[]map[string]any{child.Snapshot()},
				), true
				e.Emit(t.Context(), id, &next, nil)
				if *testCaseInput {
					return e.Require(ctx, []manifest.DependencyRef{{NamedResource: other}}, level)
				}
				return nil
			},
		)
		c := base.New(st, nil)
		for _, obj := range []*manifest.Kustomization{x, y} {
			child := &manifest.ConfigMap{
				Name:      obj.Name,
				Namespace: "ns",
				Data:      map[string]any{"value": int64(0)},
			}
			controllertest.Emit(
				t.Context(),
				c,
				obj.Named(),
				&store.Emission{Children: []store.Child{{Obj: child, Doc: child.Snapshot()}}, Complete: true},
			)
		}
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		<-started
		<-started
		assert.Equal(t, active.Load(), int64(2))
		close(release)
		if *testCaseInput {
			<-held
			synctest.Wait()
			{
				got := active.Load()
				assert.FatalIf(t, got < 1 || got > 2, "active feedback executions = %d, want one or two", got)
			}
			cancel()
		}
		err := <-done
		assert.Equal(t, active.Load(), int64(0))
		if *testCaseInput {
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			{
				got := total.Load()
				assert.FatalIf(
					t,
					got < 80 || got > 81,
					"feedback executions = %d, want the threshold plus at most one in-flight peer",
					got,
				)
			}
		} else {
			if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
				t.Fatalf(
					"registered terminal feedback did not hit its bound: error=%v context=%v runs=%d",
					err,
					ctx.Err(),
					total.Load(),
				)
			}
			for id, runs := range counts {
				{
					got := runs.Load()
					assert.ErrorIf(
						t,
						got > 33,
						"%s: %d executions exceeds the initial execution plus 32 successors",
						id,
						got,
					)
				}
			}
		}
	})
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
