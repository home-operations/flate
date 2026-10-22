package schedule

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func TestRedispatch_ContentBudget(t *testing.T) {
	t.Parallel()
	for _, requests := range []int{maxRedispatches, maxRedispatches + 1} {
		t.Run(fmt.Sprintf("requests=%d", requests), func(t *testing.T) {
			t.Parallel()
			var runs atomic.Int64
			id := testutil.KustomizationID("ns", "feedback")
			s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
				a := &testAssessment{}
				a.invalid.Store(runs.Add(1) <= int64(requests))
				return Result{Assessment: a}
			}))
			s.Seed([]NodeID{id})
			err := s.Run(t.Context())
			if requests == maxRedispatches {
				assert.NoError(t, err)
				assert.Equal(t, len(s.Results()), 1)
			} else {
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, strings.Contains(err.Error(), id.String()), true)
				assert.Equal(t, strings.Contains(err.Error(), "exceeded 32 redispatches"), true)
				assert.Equal(t, len(s.Results()), 0)
			}
			assert.Equal(t, runs.Load(), int64(maxRedispatches+1))
			assert.Equal(t, s.nodes[id].redispatches, maxRedispatches)
		})
	}
}

func TestRedispatch_BlockedDependencyRetries(t *testing.T) {
	t.Parallel()
	var runs atomic.Int64
	id := testutil.KustomizationID("ns", "blocked")
	s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
		a := &testAssessment{}
		a.unresolved = runs.Add(1) <= maxRedispatches+8
		a.invalid.Store(a.unresolved)
		return Result{Assessment: a}
	}))
	s.Seed([]NodeID{id})
	assert.NoError(t, s.Run(t.Context()))
	assert.Equal(t, runs.Load(), int64(maxRedispatches+9))
	assert.Equal(t, s.nodes[id].redispatches, 0)
}

type callbackTasks struct {
	*task.Service
	callback func()
}

func (ts callbackTasks) Go(ctx context.Context, name string, fn func(context.Context)) {
	ts.callback()
	ts.Service.Go(ctx, name, fn)
}

func (ts callbackTasks) BlockTillDone() {
	ts.callback()
	ts.Service.BlockTillDone()
}

func TestScheduler_CallbacksOutsideMutex(t *testing.T) {
	t.Parallel()
	var s *Scheduler
	call := func() { s.OnPublication() }
	var runs atomic.Int64
	s = New(callbackTasks{Service: task.NewBounded(2), callback: call}, sweepDispatcher{
		dispatchFunc: func(context.Context, NodeID, manifest.DrainLevel) Result {
			call()
			run := runs.Add(1)
			a := &testAssessment{unresolved: run == 2, retire: call, close: call}
			a.valid = func() bool { call(); return run != 1 }
			a.settle = func(context.Context, manifest.DrainLevel) { call(); a.unresolved = false }
			return Result{Assessment: a}
		},
		sweep: func(context.Context) []NodeID { call(); return nil },
	})
	s.Seed([]NodeID{testutil.KustomizationID("ns", "callback")})
	assert.NoError(t, s.Run(t.Context()))
	assert.Equal(t, len(s.Results()), 1)
	assert.Equal(t, runs.Load(), int64(2))
}

func TestRedispatch_SlotWaitPreservesContent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tasks := task.NewBounded(2)
		occupied, admitted := make(chan struct{}, 2), make(chan struct{})
		release := make(chan struct{})
		for range 2 {
			tasks.Go(t.Context(), "occupy", func(context.Context) {
				occupied <- struct{}{}
				<-release
			})
			<-occupied
		}
		var version, observed, runs atomic.Int64
		id := testutil.KustomizationID("ns", "waiting")
		s := New(callbackTasks{Service: tasks, callback: func() {
			select {
			case <-admitted:
			default:
				close(admitted)
			}
		}}, dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
			runs.Add(1)
			observed.Store(version.Load())
			return Result{Assessment: &testAssessment{}}
		}))
		s.Seed([]NodeID{id})
		done := make(chan error, 1)
		go func() { done <- s.Run(t.Context()) }()
		<-admitted
		synctest.Wait()
		version.Store(1)
		s.OnArrival(id, true)
		close(release)
		assert.NoError(t, <-done)
		assert.Equal(t, observed.Load(), int64(1))
		assert.Equal(t, runs.Load(), int64(1))
		assert.Equal(t, s.nodes[id].redispatches, 0)
	})
}
