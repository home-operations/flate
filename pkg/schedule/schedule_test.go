package schedule

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

type dispatchFunc func(context.Context, NodeID, manifest.DrainLevel) Result

func (f dispatchFunc) Dispatch(ctx context.Context, id NodeID, drain manifest.DrainLevel) Result {
	return f(ctx, id, drain)
}

func (dispatchFunc) Sweep(context.Context, bool) []NodeID { return nil }

type sweepDispatcher struct {
	dispatchFunc
	sweep func(context.Context) []NodeID
}

func (d sweepDispatcher) Sweep(ctx context.Context, _ bool) []NodeID { return d.sweep(ctx) }

type testAssessment struct {
	invalid    atomic.Bool
	reused     bool
	unresolved bool
	valid      func() bool
	settle     func(context.Context, manifest.DrainLevel)
	retire     func()
	close      func()
}

func (a *testAssessment) Valid() bool {
	if a.valid != nil {
		return a.valid()
	}
	return !a.invalid.Load()
}

func (a *testAssessment) Outcome() Kind {
	if a.unresolved {
		return Blocked
	}
	return Terminal
}

func (a *testAssessment) Progressed() bool { return !a.reused }

func (a *testAssessment) Settle(ctx context.Context, level manifest.DrainLevel) {
	if a.settle != nil {
		a.settle(ctx, level)
	}
}

func (a *testAssessment) Retire() {
	if a.retire != nil {
		a.retire()
	}
}

func (a *testAssessment) Close() {
	if a.close != nil {
		a.close()
	}
}

type admissionTasks struct {
	*task.Service
	names []string
}

func (ts *admissionTasks) Go(ctx context.Context, name string, fn func(context.Context)) {
	ts.names = append(ts.names, name)
	ts.Service.Go(ctx, name, fn)
}

func TestScheduler_WakeOrder(t *testing.T) {
	t.Parallel()
	ids := []NodeID{
		{Kind: manifest.KindHelmRelease, Namespace: "z", Name: "a"},
		{Group: "example.test", Kind: manifest.KindHelmRelease, Namespace: "z", Name: "a"},
		testutil.HelmReleaseID("a", "a"),
		testutil.HelmReleaseID("a", "b"),
		testutil.HelmReleaseID("b", "a"),
		testutil.KustomizationID("a", "a"),
	}
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprintf("reversed=%v", reversed), func(t *testing.T) {
			t.Parallel()
			order := slices.Clone(ids)
			if reversed {
				slices.Reverse(order)
			}
			tasks := &admissionTasks{Service: task.NewBounded(2)}
			var wave atomic.Int64
			assessments := make(map[NodeID]*testAssessment, len(ids))
			for _, id := range ids {
				assessments[id] = &testAssessment{}
			}
			s := New(tasks, sweepDispatcher{
				dispatchFunc: func(_ context.Context, id NodeID, level manifest.DrainLevel) Result {
					assert.Equal(t, level, manifest.DrainNone)
					if assessments[id].invalid.Load() {
						return Result{Assessment: &testAssessment{}}
					}
					return Result{Assessment: assessments[id]}
				},
				sweep: func(context.Context) []NodeID {
					if wave.Add(1) != 1 {
						return nil
					}
					for _, a := range assessments {
						a.invalid.Store(true)
					}
					return order
				},
			})
			s.Seed(append(slices.Clone(order), order...))
			assert.NoError(t, s.Run(t.Context()))
			want := make([]string, 0, 2*len(ids))
			for range 2 {
				for _, id := range ids {
					want = append(want, "schedule/"+id.String())
				}
			}
			assert.Diff(t, tasks.names, want)
		})
	}
}

func TestScheduler_RunDispatchPanic(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				tasks := task.NewBounded(workers)
				s := New(tasks, dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
					panic("dispatch panic")
				}))
				s.Seed([]NodeID{testutil.KustomizationID("ns", "panic")})
				assert.NoError(t, s.Run(t.Context()))
				assert.Equal(t, tasks.Failures(), int64(1))
				assert.Equal(t, len(s.Results()), 0)
			})
		})
	}
}

func TestScheduler_RunCancellationBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		seed           bool
		cancelBefore   bool
		cancelDispatch bool
	}{
		{name: "canceled empty", cancelBefore: true},
		{name: "final dispatch cancellation", seed: true, cancelDispatch: true},
		{name: "clean completion", seed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
				if tc.cancelDispatch {
					cancel()
				}
				return Result{Assessment: &testAssessment{}}
			}))
			if tc.seed {
				s.Seed([]NodeID{testutil.KustomizationID("ns", "last")})
			}
			if tc.cancelBefore {
				cancel()
			}
			err := s.Run(ctx)
			assert.Equal(t, errors.Is(err, context.Canceled), tc.cancelBefore || tc.cancelDispatch)
		})
	}
}

func TestScheduler_RunStopDrains(t *testing.T) {
	t.Parallel()
	for _, capFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("cap_first=%t", capFirst), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				started, stopping := make(chan struct{}, 2), make(chan struct{}, 2)
				fail, release := make(chan struct{}), make(chan struct{})
				victim, held := testutil.KustomizationID("ns", "feedback"), testutil.KustomizationID("ns", "held")
				var active, runs atomic.Int64
				s := New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, id NodeID, _ manifest.DrainLevel) Result {
					active.Add(1)
					defer active.Add(-1)
					runs.Add(1)
					started <- struct{}{}
					if capFirst && id == victim {
						<-fail
						a := &testAssessment{}
						a.invalid.Store(true)
						return Result{Assessment: a}
					}
					<-ctx.Done()
					stopping <- struct{}{}
					<-release
					return Result{Assessment: &testAssessment{}}
				}))
				s.Seed([]NodeID{victim, held})
				if capFirst {
					s.nodes[victim].redispatches = maxRedispatches
				}
				done := make(chan error, 1)
				go func() { done <- s.Run(ctx) }()
				<-started
				<-started
				assert.Equal(t, active.Load(), int64(2))
				wantStopping := 2
				if capFirst {
					close(fail)
					wantStopping = 1
				} else {
					cancel()
				}
				for range wantStopping {
					<-stopping
				}
				synctest.Wait()
				s.OnArrival(testutil.KustomizationID("ns", "late"), true)
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("Run returned before body drain: %v", err)
				default:
				}
				close(release)
				err := <-done
				assert.Equal(t, errors.Is(err, context.Canceled), !capFirst)
				if capFirst {
					assert.Equal(t, strings.Contains(err.Error(), victim.String()), true)
					assert.Equal(t, strings.Contains(err.Error(), "exceeded 32 redispatches"), true)
				}
				assert.Equal(t, active.Load(), int64(0))
				assert.Equal(t, runs.Load(), int64(2))
				assert.Equal(t, len(s.nodes), 2)
			})
		})
	}
}

func TestScheduler_InvalidAcceptance(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("ns", "consumer")
	old, fresh := &testAssessment{}, &testAssessment{}
	var runs, retired, closed atomic.Int64
	old.invalid.Store(true)
	old.retire = func() { retired.Add(1) }
	fresh.close = func() { closed.Add(1) }
	s := New(task.NewBounded(2), sweepDispatcher{
		dispatchFunc: func(context.Context, NodeID, manifest.DrainLevel) Result {
			if runs.Add(1) == 1 {
				return Result{Assessment: old}
			}
			return Result{Assessment: fresh}
		},
		sweep: func(context.Context) []NodeID { return []NodeID{id, id} },
	})
	s.Seed([]NodeID{id})
	assert.NoError(t, s.Run(t.Context()))
	assert.Equal(t, runs.Load(), int64(2))
	assert.Equal(t, retired.Load(), int64(1))
	assert.Equal(t, closed.Load(), int64(1))
	assert.Equal(t, s.Results()[id].Assessment == fresh, true)
}

func TestScheduler_RetirementPrecedesStructuralBoundary(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		old := &testAssessment{retire: func() { close(entered); <-release }}
		old.invalid.Store(true)
		var runs, sweeps atomic.Int64
		s := New(task.NewBounded(2), sweepDispatcher{
			dispatchFunc: func(context.Context, NodeID, manifest.DrainLevel) Result {
				if runs.Add(1) == 1 {
					return Result{Assessment: old}
				}
				return Result{Assessment: &testAssessment{}}
			},
			sweep: func(context.Context) []NodeID { sweeps.Add(1); return nil },
		})
		s.Seed([]NodeID{testutil.KustomizationID("ns", "consumer")})
		done := make(chan error, 1)
		go func() { done <- s.Run(t.Context()) }()
		<-entered
		synctest.Wait()
		assert.Equal(t, sweeps.Load(), int64(0))
		close(release)
		assert.NoError(t, <-done)
	})
}

func TestScheduler_SettleBoundedRetainedAssessment(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}, 2), make(chan struct{})
		var runs, active, retired, closed atomic.Int64
		s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
			runs.Add(1)
			a := &testAssessment{unresolved: true}
			a.retire = func() { retired.Add(1) }
			a.close = func() { closed.Add(1) }
			a.settle = func(_ context.Context, level manifest.DrainLevel) {
				assert.Equal(t, level, manifest.DrainCascade)
				assert.Equal(t, active.Add(1) <= 2, true)
				entered <- struct{}{}
				<-release
				active.Add(-1)
				a.unresolved = false
			}
			return Result{Assessment: a}
		}))
		s.Seed([]NodeID{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c")})
		done := make(chan error, 1)
		go func() { done <- s.Run(t.Context()) }()
		<-entered
		<-entered
		synctest.Wait()
		assert.Equal(t, active.Load(), int64(2))
		close(release)
		assert.NoError(t, <-done)
		assert.Equal(t, runs.Load(), int64(3))
		assert.Equal(t, retired.Load(), int64(0))
		assert.Equal(t, closed.Load(), int64(3))
		assert.Equal(t, len(s.Results()), 3)
	})
}

func TestScheduler_UnresolvedAfterForce(t *testing.T) {
	t.Parallel()
	var levels []manifest.DrainLevel
	s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
		return Result{Assessment: &testAssessment{unresolved: true, settle: func(_ context.Context, level manifest.DrainLevel) {
			levels = append(levels, level)
		}}}
	}))
	s.Seed([]NodeID{testutil.KustomizationID("ns", "stuck")})
	err := s.Run(t.Context())
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, strings.Contains(err.Error(), "unresolved assessment"), true)
	assert.Diff(t, levels, []manifest.DrainLevel{manifest.DrainCascade, manifest.DrainForce})
	assert.Equal(t, len(s.Results()), 0)
}

func TestScheduler_LateArrivalAfterDrain(t *testing.T) {
	t.Parallel()
	s := New(task.NewBounded(2), sweepDispatcher{
		dispatchFunc: func(context.Context, NodeID, manifest.DrainLevel) Result { return Result{} },
		sweep:        func(context.Context) []NodeID { return nil },
	})
	assert.NoError(t, s.Run(t.Context()))
	s.OnArrival(testutil.KustomizationID("ns", "late"), true)
	assert.Equal(t, len(s.nodes), 0)
}

func TestScheduler_SettleArrivalRestartsNormalDispatch(t *testing.T) {
	t.Parallel()
	parent, child := testutil.KustomizationID("ns", "parent"), testutil.KustomizationID("ns", "child")
	var runs atomic.Int64
	var s *Scheduler
	s = New(task.NewBounded(2), dispatchFunc(func(_ context.Context, id NodeID, level manifest.DrainLevel) Result {
		runs.Add(1)
		assert.Equal(t, level, manifest.DrainNone)
		a := &testAssessment{unresolved: id == parent}
		a.settle = func(_ context.Context, level manifest.DrainLevel) {
			assert.Equal(t, level, manifest.DrainCascade)
			a.unresolved = false
			s.OnArrival(child, true)
		}
		return Result{Assessment: a}
	}))
	s.Seed([]NodeID{parent})
	assert.NoError(t, s.Run(t.Context()))
	assert.Equal(t, runs.Load(), int64(2))
	assert.Equal(t, len(s.Results()), 2)
}

func TestScheduler_RunSettleCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		started, stopping := make(chan struct{}, 2), make(chan struct{}, 2)
		release := make(chan struct{})
		var active, closed atomic.Int64
		s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
			return Result{Assessment: &testAssessment{
				unresolved: true,
				settle: func(ctx context.Context, _ manifest.DrainLevel) {
					active.Add(1)
					defer active.Add(-1)
					started <- struct{}{}
					<-ctx.Done()
					stopping <- struct{}{}
					<-release
				},
				close: func() { closed.Add(1) },
			}}
		}))
		s.Seed([]NodeID{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b")})
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		<-started
		<-started
		assert.Equal(t, active.Load(), int64(2))
		cancel()
		<-stopping
		<-stopping
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Run returned before classification drain: %v", err)
		default:
		}
		close(release)
		assert.Equal(t, errors.Is(<-done, context.Canceled), true)
		assert.Equal(t, active.Load(), int64(0))
		assert.Equal(t, closed.Load(), int64(2))
	})
}

func TestScheduler_FinalReclamationCanceled(t *testing.T) {
	t.Parallel()
	var canceled []bool
	s := New(task.NewBounded(2), sweepDispatcher{
		dispatchFunc: func(context.Context, NodeID, manifest.DrainLevel) Result { return Result{} },
		sweep: func(ctx context.Context) []NodeID {
			canceled = append(canceled, errors.Is(ctx.Err(), context.Canceled))
			return nil
		},
	})
	assert.NoError(t, s.Run(t.Context()))
	assert.Diff(t, canceled, []bool{false, true})
}

func TestScheduler_UnprogressedInvalidAssessment(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		id := testutil.KustomizationID("apps", "consumer")
		var runs, settlements atomic.Int64
		retained := &testAssessment{reused: true, unresolved: true}
		retained.settle = func(_ context.Context, level manifest.DrainLevel) {
			assert.Equal(t, level, manifest.DrainCascade)
			settlements.Add(1)
			retained.unresolved = false
		}
		s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
			if runs.Add(1) == 1 {
				stale := &testAssessment{reused: true}
				stale.invalid.Store(true)
				return Result{Assessment: stale}
			}
			return Result{Assessment: retained}
		}))
		s.Seed([]NodeID{id})
		assert.NoError(t, s.Run(t.Context()))
		assert.Equal(t, runs.Load(), int64(2))
		assert.Equal(t, settlements.Load(), int64(1))
		assert.Equal(t, s.nodes[id].redispatches, 0)
		assert.Equal(t, s.nodes[id].state, stateTerminal)
	})
}

func TestScheduler_CompletionRetainsRunningInvalidation(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("current_changed=%t", changed), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				tasks := task.NewBounded(2)
				id := NodeID{Kind: "Reader", Name: "reader"}
				captured, release := make(chan struct{}), make(chan struct{})
				var checked atomic.Bool
				current := &testAssessment{}
				current.valid = func() bool {
					valid := !current.invalid.Load()
					if checked.CompareAndSwap(false, true) {
						close(captured)
						tasks.YieldSlot(func() { <-release })
					}
					return valid
				}
				var runs atomic.Int64
				s := New(tasks, dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
					if runs.Add(1) == 1 {
						return Result{Assessment: current}
					}
					return Result{Assessment: &testAssessment{}}
				}))
				s.Seed([]NodeID{id})
				done := make(chan error, 1)
				go func() { done <- s.Run(t.Context()) }()
				<-captured
				if changed {
					current.invalid.Store(true)
				}
				s.invalidate([]NodeID{id})
				close(release)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				want := int64(1)
				if changed {
					want = 2
				}
				if got := runs.Load(); got != want {
					t.Fatalf("dispatches=%d, want %d after running invalidation", got, want)
				}
			})
		})
	}
}
