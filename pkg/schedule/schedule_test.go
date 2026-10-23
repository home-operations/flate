package schedule

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

type dispatchFunc func(context.Context, NodeID, manifest.DrainLevel) (Outcome, manifest.ReconcileWait)

func (f dispatchFunc) Dispatch(ctx context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
	return f(ctx, id, attempt.Drain)
}

func TestEdgeIdx_AddAndDelete(t *testing.T) {
	t.Parallel()
	dep, otherDep, first, second := testutil.KustomizationID("ns", "dep"), testutil.KustomizationID("ns", "other-dep"), testutil.KustomizationID("ns", "first"), testutil.KustomizationID("ns", "second")
	for _, tc := range []struct {
		name string
		add  [][2]NodeID
		del  [][2]NodeID
		want edgeIdx
	}{
		{
			name: "duplicate adds",
			add:  [][2]NodeID{{dep, first}, {dep, first}},
			want: edgeIdx{dep: {first: {}}},
		},
		{
			name: "missing deletes",
			add:  [][2]NodeID{{dep, first}},
			del:  [][2]NodeID{{dep, second}, {otherDep, first}},
			want: edgeIdx{dep: {first: {}}},
		},
		{
			name: "shared consumers",
			add:  [][2]NodeID{{dep, first}, {dep, second}},
			del:  [][2]NodeID{{dep, first}},
			want: edgeIdx{dep: {second: {}}},
		},
		{
			name: "last consumer prunes only its dependency",
			add:  [][2]NodeID{{dep, first}, {dep, second}, {otherDep, first}},
			del:  [][2]NodeID{{dep, first}, {dep, second}},
			want: edgeIdx{otherDep: {first: {}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := edgeIdx{}
			for _, edge := range tc.add {
				e.add(edge[0], edge[1])
			}
			for _, edge := range tc.del {
				e.del(edge[0], edge[1])
			}
			assert.Diff(t, e, tc.want)
		})
	}
}

func TestScheduler_WakeOrder(t *testing.T) {
	t.Parallel()
	ordered := []NodeID{
		{Kind: manifest.KindHelmRelease, Namespace: "z", Name: "a"},
		{Group: "example.test", Kind: manifest.KindHelmRelease, Namespace: "z", Name: "a"},
		testutil.HelmReleaseID("a", "a"),
		testutil.HelmReleaseID("a", "b"),
		testutil.HelmReleaseID("b", "a"),
		testutil.KustomizationID("a", "a"),
	}
	for _, path := range []string{"parked", "failed"} {
		for _, tc := range []struct {
			name  string
			order []NodeID
			want  []NodeID
		}{
			{name: "empty"},
			{name: "singleton", order: ordered[:1], want: ordered[:1]},
			{name: "multiple sorted", order: ordered, want: ordered},
			{name: "multiple reversed", order: []NodeID{ordered[5], ordered[4], ordered[3], ordered[2], ordered[1], ordered[0]}, want: ordered},
			{name: "multiple permuted", order: []NodeID{ordered[2], ordered[0], ordered[5], ordered[3], ordered[1], ordered[4]}, want: ordered},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				for range 64 {
					s := New(task.NewBounded(2), nil)
					dep, otherDep, prefix := testutil.KustomizationID("ns", "dependency"), testutil.KustomizationID("ns", "other-dependency"), testutil.KustomizationID("ns", "queued")
					s.runq = []NodeID{prefix}
					for _, nid := range tc.order {
						n := &node{id: nid, state: stateParked, blockedOn: []NodeID{dep, otherDep}}
						idx := s.parkedIdx
						if path == "failed" {
							n.state, n.blockedOn, n.failedOn = stateTerminal, nil, []NodeID{dep, otherDep}
							idx = s.failedIdx
						}
						s.nodes[nid] = n
						idx.add(dep, nid)
						idx.add(otherDep, nid)
					}
					s.mu.Lock()
					if path == "parked" {
						s.wakeWaitersLocked(dep)
					} else {
						s.recordProgressLocked(dep)
					}
					s.mu.Unlock()
					assert.Diff(t, s.runq, append([]NodeID{prefix}, tc.want...))
					for _, nid := range tc.order {
						n := s.nodes[nid]
						assert.Equal(t, n.state, stateRunnable)
						assert.Equal(t, len(n.blockedOn), 0)
						if path == "parked" {
							assert.Equal(t, n.redispatches, 0)
						} else {
							assert.Equal(t, n.redispatches, 1)
							for _, blocker := range []NodeID{dep, otherDep} {
								_, retained := s.failedIdx[blocker][nid]
								assert.Equal(t, retained, true)
							}
							s.mu.Lock()
							n.state = stateRunning
							s.inFlight++
							s.mu.Unlock()
							s.complete(s.nodes[nid], Outcome{Rerun: false}, &manifest.ReconcileWait{Deps: nil})
						}
					}
					assert.Equal(t, len(s.parkedIdx), 0)
					assert.Equal(t, len(s.failedIdx), 0)
				}
			})
		}
	}
}

func TestScheduler_WakeAllocations(t *testing.T) {
	for _, path := range []string{"parked", "failed"} {
		for _, count := range []int{0, 1, 4} {
			t.Run(fmt.Sprintf("%s/waiters=%d", path, count), func(t *testing.T) {
				s := New(task.NewBounded(2), nil)
				dep := testutil.KustomizationID("ns", "dependency")
				idx := s.parkedIdx
				if path == "failed" {
					idx = s.failedIdx
				}
				for i := range count {
					nid := testutil.KustomizationID("ns", fmt.Sprintf("waiter-%d", i))
					s.nodes[nid] = &node{id: nid, state: stateRunning}
					idx.add(dep, nid)
				}
				allocs := testing.AllocsPerRun(100, func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					if path == "parked" {
						s.wakeWaitersLocked(dep)
					} else {
						s.recordProgressLocked(dep)
					}
				})
				assert.Equal(t, allocs, float64(0))
			})
		}
	}
}

func TestScheduler_WakeRunningWaiter(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"parked", "failed"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			s := New(task.NewBounded(2), nil)
			dep, nid := testutil.KustomizationID("ns", "dependency"), testutil.KustomizationID("ns", "running")
			n := &node{id: nid, state: stateRunning}
			s.nodes[nid], s.inFlight = n, 1
			idx := s.parkedIdx
			if path == "failed" {
				n.failedOn = []NodeID{dep}
				idx = s.failedIdx
			} else {
				n.blockedOn = []NodeID{dep}
			}
			idx.add(dep, nid)
			s.mu.Lock()
			if path == "parked" {
				s.wakeWaitersLocked(dep)
			} else {
				s.recordProgressLocked(dep)
			}
			s.mu.Unlock()
			assert.Equal(t, n.rerunRequested, true)
			assert.Equal(t, n.productive, path == "failed")
			assert.Equal(t, n.state, stateRunning)
			assert.Equal(t, len(s.runq), 0)
			_, retained := idx[dep][nid]
			assert.Equal(t, retained, true)
		})
	}
}

func TestScheduler_FailedWakeStopsAtCap(t *testing.T) {
	t.Parallel()
	s := New(task.NewBounded(2), nil)
	dep, first, second := testutil.KustomizationID("ns", "dependency"), testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b")
	for _, nid := range []NodeID{second, first} {
		s.nodes[nid] = &node{id: nid, state: stateTerminal, failedOn: []NodeID{dep}}
		s.failedIdx.add(dep, nid)
	}
	s.nodes[first].redispatches = maxRedispatches
	s.mu.Lock()
	s.recordProgressLocked(dep)
	s.mu.Unlock()
	assert.Equal(t, strings.Contains(s.err.Error(), first.String()), true)
	assert.Equal(t, len(s.runq), 0)
	assert.Equal(t, s.nodes[second].productive, false)
	assert.Equal(t, len(s.failedIdx[dep]), 2)
}

func TestScheduler_RunDispatchPanic(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				tasks := task.NewBounded(workers)
				started := make(chan struct{})
				s := New(tasks, dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
					close(started)
					panic("dispatch panic")
				}))
				s.Seed([]NodeID{testutil.KustomizationID("ns", "panic")})
				done := make(chan error, 1)
				go func() { done <- s.Run(ctx) }()
				returned := false
				defer func() {
					cancel()
					if !returned {
						<-done
					}
				}()

				<-started
				tasks.BlockTillDone()
				synctest.Wait()
				select {
				case err := <-done:
					returned = true
					assert.NoError(t, err)
					assert.NoError(t, ctx.Err())
					assert.Equal(t, tasks.Failures(), int64(1))
				default:
					t.Error("Run did not return after dispatch panic")
				}
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
			s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
				if tc.cancelDispatch {
					cancel()
				}
				return Outcome{}, manifest.ReconcileWait{}
			}))
			if tc.seed {
				s.Seed([]NodeID{testutil.KustomizationID("ns", "last")})
			}
			if tc.cancelBefore {
				cancel()
			}
			err := s.Run(ctx)
			if tc.cancelBefore || tc.cancelDispatch {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			} else {
				assert.Equal(t, err, nil)
			}
		})
	}
}

func TestScheduler_RunStopDrains(t *testing.T) {
	for _, capFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("cap_first=%t", capFirst), func(t *testing.T) {
			checkCtx, stopChecks := context.WithTimeout(t.Context(), 5*time.Second)
			defer stopChecks()
			ctx, cancel := context.WithCancel(checkCtx)
			var run sync.WaitGroup
			defer run.Wait()
			started, stopping := make(chan struct{}, 2), make(chan struct{}, 2)
			fail, release := make(chan struct{}), make(chan struct{})
			releaseBodies := sync.OnceFunc(func() { close(release) })
			defer releaseBodies()
			defer cancel()
			victim, held := testutil.KustomizationID("ns", "feedback"), testutil.KustomizationID("ns", "held")
			var active, runs atomic.Int64
			var s *Scheduler
			s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
				active.Add(1)
				defer active.Add(-1)
				runs.Add(1)
				started <- struct{}{}
				if capFirst && nid == victim {
					if awaitSignal(ctx, fail) {
						s.OnArrival(victim, true)
					}
					return Outcome{}, manifest.ReconcileWait{}
				}
				<-ctx.Done()
				stopping <- struct{}{}
				awaitSignal(checkCtx, release)
				return Outcome{}, manifest.ReconcileWait{}
			}))
			s.Seed([]NodeID{victim, held})
			if capFirst {
				s.nodes[victim].redispatches = maxRedispatches
			}
			done := make(chan error, 1)
			run.Go(func() { done <- s.Run(ctx) })
			for range 2 {
				if !awaitSignal(checkCtx, started) {
					t.Fatal("two worker bodies did not start")
				}
			}
			assert.Equal(t, active.Load(), int64(2))
			wantStopping := 2
			if capFirst {
				close(fail)
				wantStopping = 1
			} else {
				cancel()
			}
			for range wantStopping {
				if !awaitSignal(checkCtx, stopping) {
					t.Fatal("bodies did not observe the stop")
				}
			}
			s.mu.Lock()
			for s.err == nil {
				s.cond.Wait()
			}
			recorded := s.err
			s.mu.Unlock()
			cancel()
			s.OnArrival(testutil.KustomizationID("ns", "late"), true)
			select {
			case err := <-done:
				t.Fatalf("Run returned before body drain: %v", err)
			default:
			}
			releaseBodies()
			select {
			case err := <-done:
				if capFirst {
					assert.Equal(t, errors.Is(err, context.Canceled), false)
					assert.Equal(t, strings.Contains(err.Error(), victim.String()), true)
					assert.Equal(t, strings.Contains(err.Error(), "exceeded 32 redispatches"), true)
				} else {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
				}
				assert.Equal(t, err, recorded)
			case <-checkCtx.Done():
				t.Fatal("Run did not finish after body drain")
			}
			assert.Equal(t, active.Load(), int64(0))
			assert.Equal(t, runs.Load(), int64(2))
			assert.Equal(t, len(s.nodes), 2)
		})
	}
}

func TestScheduler_FailedBlockerSets(t *testing.T) {
	a, b, c, d := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c"), testutil.KustomizationID("ns", "d")
	for _, tc := range []struct {
		name    string
		blocked []NodeID
		want    bool
	}{
		{name: "unchanged", blocked: []NodeID{a, b, c}, want: true},
		{name: "reordered", blocked: []NodeID{c, a, b}, want: true},
		{name: "nonadjacent duplicates", blocked: []NodeID{c, a, c, b, a}, want: true},
		{name: "duplicates hide removal", blocked: []NodeID{a, b, a}},
		{name: "fewer blockers", blocked: []NodeID{a, b}},
		{name: "replacement", blocked: []NodeID{a, b, d}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(task.NewBounded(2), nil)
			n := &node{id: testutil.KustomizationID("ns", "consumer"), state: stateRunning}
			s.nodes[n.id], s.inFlight = n, 1
			s.complete(n, Outcome{Kind: DependencyFailed, Rerun: false}, &manifest.ReconcileWait{Deps: []NodeID{a, b, c}})
			var got bool
			allocs := testing.AllocsPerRun(100, func() {
				got = s.sameFailedLocked(n, tc.blocked)
			})
			assert.Equal(t, got, tc.want)
			assert.Equal(t, allocs, float64(0))
			assert.Diff(t, n.failedOn, []NodeID{a, b, c})
			for _, dep := range n.failedOn {
				_, retained := s.failedIdx[dep][n.id]
				assert.Equal(t, retained, true)
			}
		})
	}
}

func TestScheduler_FailedReplacementPreservesOtherConsumers(t *testing.T) {
	t.Parallel()
	for _, fanIn := range []int{2, 32} {
		for _, position := range []int{0, fanIn - 1} {
			t.Run(fmt.Sprintf("fanin_%d/position_%d", fanIn, position), func(t *testing.T) {
				t.Parallel()
				s := New(task.NewBounded(2), nil)
				n := &node{id: testutil.KustomizationID("ns", "consumer"), state: stateRunning}
				s.nodes[n.id], s.inFlight = n, 1
				previous, blocked := make([]NodeID, fanIn), make([]NodeID, fanIn)
				other := testutil.KustomizationID("ns", "other")
				for i := range previous {
					previous[i] = testutil.KustomizationID("ns", fmt.Sprintf("blocker-%02d", i))
					blocked[i] = previous[i]
					s.failedIdx[previous[i]] = map[NodeID]struct{}{other: {}}
				}
				s.complete(n, Outcome{Kind: DependencyFailed, Rerun: false}, &manifest.ReconcileWait{Deps: previous})
				blocked[position] = testutil.KustomizationID("ns", "replacement")
				n.state, s.inFlight = stateRunning, 1
				s.complete(n, Outcome{Kind: DependencyFailed, Rerun: false}, &manifest.ReconcileWait{Deps: blocked})
				assert.Diff(t, n.failedOn, blocked)
				for i, dep := range previous {
					_, retained := s.failedIdx[dep][n.id]
					assert.Equal(t, retained, i != position)
					_, retained = s.failedIdx[dep][other]
					assert.Equal(t, retained, true)
				}
				_, retained := s.failedIdx[blocked[position]][n.id]
				assert.Equal(t, retained, true)
			})
		}
	}
}

func TestScheduler_FailedBlockerWidthChanges(t *testing.T) {
	t.Parallel()
	s := New(task.NewBounded(2), nil)
	n := &node{id: testutil.KustomizationID("ns", "consumer")}
	s.nodes[n.id] = n
	a, b, c := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c")
	complete := func(out Outcome, blocked, want []NodeID) {
		t.Helper()
		n.state, s.inFlight = stateRunning, 1
		s.complete(n, Outcome{Kind: out.Kind, Noop: out.Noop, Rerun: false}, &manifest.ReconcileWait{Deps: blocked})
		assert.Diff(t, n.failedOn, want)
		assert.Equal(t, len(s.failedIdx), len(want))
		for _, dep := range want {
			_, retained := s.failedIdx[dep][n.id]
			assert.Equal(t, retained, true)
		}
	}
	complete(Outcome{Kind: DependencyFailed}, []NodeID{a, b, c}, []NodeID{a, b, c})
	complete(Outcome{Kind: DependencyFailed}, []NodeID{b, a}, []NodeID{b, a})
	complete(Outcome{Kind: DependencyFailed}, []NodeID{a, a}, []NodeID{a})
	complete(Outcome{Kind: DependencyFailed}, []NodeID{b, a, b, c, a}, []NodeID{b, a, c})
	complete(Outcome{Kind: DependencyFailed}, []NodeID{c, a, b, c, a}, []NodeID{b, a, c})
	complete(Outcome{}, nil, nil)
	complete(Outcome{Kind: DependencyFailed}, []NodeID{a, b, c}, []NodeID{a, b, c})
}

func TestRedispatchLimit(t *testing.T) {
	for _, mode := range []string{"running arrival", "terminal arrival", "drain replay"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			var s *Scheduler
			var helpers sync.WaitGroup
			var runs atomic.Int64
			terminal, arrived := make(chan struct{}), make(chan struct{})
			victim := testutil.KustomizationID("ns", "feedback")
			producer := testutil.KustomizationID("ns", "producer")
			disp := dispatchFunc(func(ctx context.Context, nid NodeID, _ manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
				if nid == producer {
					for awaitSignal(ctx, terminal) {
						s.OnArrival(victim, true)
						select {
						case arrived <- struct{}{}:
						case <-ctx.Done():
							return Outcome{Rerun: mode == "drain replay"}, manifest.ReconcileWait{}
						}
					}
					return Outcome{Rerun: mode == "drain replay"}, manifest.ReconcileWait{}
				}
				runs.Add(1)
				switch mode {
				case "running arrival":
					s.OnArrival(victim, true)
				case "drain replay":
					s.OnArrival(NodeID{Kind: manifest.KindConfigMap, Name: "values"}, false)
				}
				return Outcome{Rerun: mode == "drain replay"}, manifest.ReconcileWait{}
			})
			s = New(task.NewBounded(2), disp)
			defer func() {
				cancel()
				s.mu.Lock()
				s.cond.Broadcast()
				s.mu.Unlock()
				helpers.Wait()
			}()
			s.Seed([]NodeID{victim})
			if mode == "terminal arrival" {
				s.Seed([]NodeID{producer})
				helpers.Go(func() {
					for {
						s.mu.Lock()
						for s.nodes[victim].state != stateTerminal && s.err == nil && ctx.Err() == nil {
							s.cond.Wait()
						}
						stopped := s.err != nil || ctx.Err() != nil
						s.mu.Unlock()
						if stopped {
							return
						}
						select {
						case terminal <- struct{}{}:
						case <-ctx.Done():
							return
						}
						if !awaitSignal(ctx, arrived) {
							return
						}
					}
				})
			}
			err := s.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), victim.String()) || !strings.Contains(err.Error(), "exceeded 32 redispatches") {
				t.Fatalf("Run error = %v, want node-specific redispatch limit", err)
			}
			if ctx.Err() != nil {
				t.Fatalf("redispatch limit must terminate before deadline: %v", ctx.Err())
			}
			if got := runs.Load(); got != int64(maxRedispatches+1) {
				t.Fatalf("dispatches = %d, want %d", got, maxRedispatches+1)
			}
		})
	}
}

func TestRedispatchBudgetAllowsConvergence(t *testing.T) {
	for _, outcome := range []Outcome{{}, {Kind: Blocked}} {
		t.Run(fmt.Sprintf("outcome_%d", outcome.Kind), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			limit := maxRedispatches
			var s *Scheduler
			runs := 0
			s = New(task.NewBounded(2), dispatchFunc(func(_ context.Context, nid NodeID, _ manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
				runs++
				if runs <= limit {
					s.OnArrival(nid, true)
					outcome.Rerun = false
					outcomeWait := manifest.ReconcileWait{Deps: []NodeID{testutil.KustomizationID("ns", "dep")}}
					return outcome, outcomeWait
				}
				return Outcome{}, manifest.ReconcileWait{}
			}))
			s.Seed([]NodeID{testutil.KustomizationID("ns", "finite")})
			if err := s.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if runs != limit+1 {
				t.Fatalf("dispatches = %d, want %d", runs, limit+1)
			}
		})
	}
}

// fakeDisp is a store-free, controller-free Dispatcher driven by a fixed
// dependency graph. It mimics classifyDep semantics exactly:
//   - a dep present-and-Ready              -> satisfied
//   - a dep present-and-Failed             -> cascade-fail this node
//   - a dep absent (no graph node):
//     manifest.DrainNone           -> block
//     manifest.DrainCascade/Force  -> fail ("dependency not found")
//   - a dep present-but-Pending (a graph node not yet terminal):
//     manifest.DrainNone/Cascade   -> block
//     manifest.DrainForce          -> fail ("not ready")
//
// A "failing leaf" is modeled as a node depending on an id absent from the
// graph. The fake records terminal state + per-node dispatch counts + the max
// drain level it observed, all under its own mutex.
type fakeDisp struct {
	graph map[NodeID][]NodeID // node -> deps

	// gate, if set for an id, blocks that id's Dispatch until the channel is
	// closed — used to force interleavings (e.g. the lost-wakeup race).
	gate map[NodeID]chan struct{}

	// drainRerun, if set for an id, makes Dispatch return rerun=true for
	// it — modeling a selector ResourceSet that re-expands at the fixpoint.
	drainRerun map[NodeID]bool

	mu        sync.Mutex
	termReady map[NodeID]bool
	termAny   map[NodeID]bool
	runs      map[NodeID]int
	maxDrain  manifest.DrainLevel
}

func newFake(graph map[NodeID][]NodeID) *fakeDisp {
	return &fakeDisp{
		graph:      graph,
		gate:       map[NodeID]chan struct{}{},
		drainRerun: map[NodeID]bool{},
		termReady:  map[NodeID]bool{},
		termAny:    map[NodeID]bool{},
		runs:       map[NodeID]int{},
	}
}

func (f *fakeDisp) Dispatch(_ context.Context, nid NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
	drainLevel := attempt.Drain
	f.mu.Lock()
	g := f.gate[nid]
	f.mu.Unlock()
	if g != nil {
		<-g // block until the test releases this node
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[nid]++
	f.maxDrain = max(f.maxDrain, drainLevel)
	deps := f.graph[nid]
	var blocked []NodeID
	var failed []NodeID
	for _, d := range deps {
		if f.termAny[d] {
			if !f.termReady[d] {
				failed = append(failed, d)
			}
			continue // terminal-ready -> satisfied
		}
		if _, isNode := f.graph[d]; !isNode {
			// absent dep
			if drainLevel >= manifest.DrainCascade {
				failed = append(failed, d)
				continue
			}
			blocked = append(blocked, d)
			continue
		}
		// present but not yet terminal (pending)
		if drainLevel >= manifest.DrainForce {
			failed = append(failed, d)
			continue
		}
		blocked = append(blocked, d)
	}
	switch {
	case len(failed) > 0:
		f.termAny[nid] = true
		f.termReady[nid] = false
		return Outcome{Kind: DependencyFailed, Rerun: f.drainRerun[nid]}, manifest.ReconcileWait{Deps: failed}
	case len(blocked) > 0:
		return Outcome{Kind: Blocked, Rerun: f.drainRerun[nid]}, manifest.ReconcileWait{Deps: blocked}
	default:
		f.termAny[nid] = true
		f.termReady[nid] = true
		return Outcome{Rerun: f.drainRerun[nid]}, manifest.ReconcileWait{}
	}
}

func (f *fakeDisp) state(nid NodeID) (terminal, ready bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.termAny[nid], f.termReady[nid]
}

func (f *fakeDisp) runCount(nid NodeID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs[nid]
}

func (f *fakeDisp) maxDrainLevel() manifest.DrainLevel {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxDrain
}

// run seeds the graph keys and runs the scheduler to a fixpoint on a pool of
// the given width.
func run(t *testing.T, f *fakeDisp, workers int) *Scheduler {
	t.Helper()
	ts := task.NewBounded(workers)
	s := New(ts, f)
	seeds := make([]NodeID, 0, len(f.graph))
	for k := range f.graph {
		seeds = append(seeds, k)
	}
	s.Seed(seeds)
	if err := s.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}

func assertReady(t *testing.T, f *fakeDisp, name string) {
	t.Helper()
	term, ready := f.state(testutil.KustomizationID("ns", name))
	if !term || !ready {
		t.Fatalf("%s: want terminal-Ready, got terminal=%v ready=%v", name, term, ready)
	}
}

func assertFailed(t *testing.T, f *fakeDisp, name string) {
	t.Helper()
	term, ready := f.state(testutil.KustomizationID("ns", name))
	if !term || ready {
		t.Fatalf("%s: want terminal-Failed, got terminal=%v ready=%v", name, term, ready)
	}
}

func TestLinearChainReadyPropagation(t *testing.T) {
	t.Parallel()
	// a -> b -> c (c is a ready leaf).
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "a"): {testutil.KustomizationID("ns", "b")},
		testutil.KustomizationID("ns", "b"): {testutil.KustomizationID("ns", "c")},
		testutil.KustomizationID("ns", "c"): nil,
	})
	run(t, f, 8)
	assertReady(t, f, "a")
	assertReady(t, f, "b")
	assertReady(t, f, "c")
	if f.maxDrainLevel() != manifest.DrainNone {
		t.Fatalf("a healthy chain must resolve without draining; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestDiamondReadyPropagation(t *testing.T) {
	t.Parallel()
	// d -> {b, c}; b -> a; c -> a; a ready leaf.
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "d"): {testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "c")},
		testutil.KustomizationID("ns", "b"): {testutil.KustomizationID("ns", "a")},
		testutil.KustomizationID("ns", "c"): {testutil.KustomizationID("ns", "a")},
		testutil.KustomizationID("ns", "a"): nil,
	})
	run(t, f, 8)
	for _, n := range []string{"a", "b", "c", "d"} {
		assertReady(t, f, n)
	}
}

func TestDanglingChainCascadeFails(t *testing.T) {
	t.Parallel()
	// a -> b -> (absent X). Both must fail; the leaf's "dependency not found"
	// cascades up. manifest.DrainCascade suffices (no manifest.DrainForce needed for a chain).
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "a"): {testutil.KustomizationID("ns", "b")},
		testutil.KustomizationID("ns", "b"): {testutil.KustomizationID("ns", "missing")}, // "missing" is absent from the graph
	})
	run(t, f, 8)
	assertFailed(t, f, "a")
	assertFailed(t, f, "b")
	if f.maxDrainLevel() != manifest.DrainCascade {
		t.Fatalf("a dangling chain should drain at manifest.DrainCascade, not manifest.DrainForce; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestParkThenArrivalWake(t *testing.T) {
	t.Parallel()
	// a -> x, but x is NOT seeded; it must arrive via OnArrival (render
	// discovery) and wake the parked a. A gated "keepalive" node holds the pool
	// non-idle so the scheduler cannot reach the fixpoint and drain a before x
	// arrives — forcing the genuine park-then-wake path deterministically.
	foreign := testutil.KustomizationID("ns", "x")
	foreign.Group = "example.test"
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "a"):         {testutil.KustomizationID("ns", "x")},
		testutil.KustomizationID("ns", "x"):         nil,
		foreign:                                     nil,
		testutil.KustomizationID("ns", "keepalive"): nil,
	})
	gate := make(chan struct{})
	f.gate[testutil.KustomizationID("ns", "keepalive")] = gate
	ts := task.NewBounded(8)
	s := New(ts, f)
	s.Seed([]NodeID{testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "keepalive")}) // x absent at start
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()

	waitUntil(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.nodes[testutil.KustomizationID("ns", "a")].state == stateParked
	})
	s.OnArrival(foreign, true)
	waitUntil(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.nodes[foreign] != nil && s.nodes[foreign].state == stateTerminal
	})
	s.OnStatusWake(foreign, true, false)
	s.mu.Lock()
	assert.Equal(t, s.nodes[testutil.KustomizationID("ns", "a")].state, stateParked)
	assert.Diff(t, s.nodes[testutil.KustomizationID("ns", "a")].blockedOn, []NodeID{testutil.KustomizationID("ns", "x")})
	assert.Equal(t, s.nodes[testutil.KustomizationID("ns", "x")], nil)
	s.mu.Unlock()
	assert.Equal(t, f.runCount(testutil.KustomizationID("ns", "a")), 1)
	s.OnArrival(testutil.KustomizationID("ns", "x"), true)
	waitUntil(t, func() bool { term, _ := f.state(testutil.KustomizationID("ns", "a")); return term })
	close(gate) // let keepalive finish so Run can reach the fixpoint
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertReady(t, f, "a")
	assertReady(t, f, "x")
}

// waitUntil polls cond until true or the test times out.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	for range 5_000_000 {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("waitUntil: condition not met")
}

func TestLostWakeupRecheck(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		reader, supplier := testutil.KustomizationID("ns", "reader"), testutil.KustomizationID("ns", "supplier")
		release := make(chan struct{})
		runs := 0
		d := opportunityDispatcher(func(_ context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
			if id == supplier {
				return Outcome{}, manifest.ReconcileWait{}
			}
			runs++
			assert.Equal(t, attempt.Drain, manifest.DrainNone)
			if runs == 1 {
				<-release
				return Outcome{Kind: Blocked}, manifest.ReconcileWait{Deps: []NodeID{supplier}}
			}
			return Outcome{}, manifest.ReconcileWait{}
		})
		tasks := task.NewBounded(2)
		sched := New(tasks, d)
		sched.Seed([]NodeID{reader, supplier})
		done := make(chan error, 1)
		go func() { done <- sched.Run(t.Context()) }()
		synctest.Wait()
		sched.mu.Lock()
		assert.Equal(t, sched.nodes[supplier].state, stateTerminal)
		sched.mu.Unlock()
		close(release)
		assert.NoError(t, <-done)
		assert.Equal(t, runs, 2)
		assert.Equal(t, tasks.Failures(), int64(0))
		assert.Equal(t, sched.nodes[reader].state, stateTerminal)
		assert.Equal(t, len(sched.parkedIdx), 0)
	})
}

func TestTransitiveBlockCompareOrderCascades(t *testing.T) {
	t.Parallel()
	// Names chosen so the CONSUMER sorts before its blocker in requeue order
	// ("a-top" < "z-leaf"), forcing the draining sweep to re-run the consumer
	// first; the complete() wake must still cascade the leaf's failure upward.
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "a-top"):  {testutil.KustomizationID("ns", "z-leaf")},
		testutil.KustomizationID("ns", "z-leaf"): {testutil.KustomizationID("ns", "absent")},
	})
	run(t, f, 8)
	assertFailed(t, f, "a-top")
	assertFailed(t, f, "z-leaf")
}

func TestCrossKindCycleForceDrains(t *testing.T) {
	t.Parallel()
	// a <-> b mutual dependency (no same-kind preflight here): manifest.DrainCascade
	// cannot break it (each sees the other present-Pending and re-parks), so
	// the scheduler must escalate to manifest.DrainForce and fail both.
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "a"): {testutil.KustomizationID("ns", "b")},
		testutil.KustomizationID("ns", "b"): {testutil.KustomizationID("ns", "a")},
	})
	run(t, f, 8)
	assertFailed(t, f, "a")
	assertFailed(t, f, "b")
	if f.maxDrainLevel() != manifest.DrainForce {
		t.Fatalf("a cycle must escalate to manifest.DrainForce; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestScheduler_PermanentlyBlockedTerminates(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := testutil.KustomizationID("ns", "reader")
	absent := testutil.KustomizationID("ns", "absent")
	maxDrain := manifest.DrainNone
	forceDispatches := 0
	d := opportunityDispatcher(func(_ context.Context, _ NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
		maxDrain = max(maxDrain, attempt.Drain)
		if attempt.Drain == manifest.DrainForce {
			forceDispatches++
			if forceDispatches > 1 {
				cancel()
			}
		}
		return Outcome{Kind: Blocked}, manifest.ReconcileWait{Deps: []NodeID{absent}}
	})
	s := New(task.NewBounded(2), d)
	s.Seed([]NodeID{id})
	if err := s.Run(ctx); err != nil {
		t.Fatalf("permanently blocked node did not reach a fixpoint: %v", err)
	}
	if s.nodes[id].state != stateTerminal {
		t.Fatalf("seeded node state = %v, want terminal", s.nodes[id].state)
	}
	if maxDrain != manifest.DrainForce {
		t.Fatalf("max drain = %v, want DrainForce", maxDrain)
	}
}

func TestDrainRerunReexpandsOnArrival(t *testing.T) {
	t.Parallel()
	// A rerun node (a selector ResourceSet) terminalizes immediately, then a
	// later arrival dirties the store. At the structural fixpoint the node must
	// re-run once — re-expanding against the now-complete store — even though it
	// parked on nothing and the arriving id is not one it waited on.
	f := newFake(map[NodeID][]NodeID{
		testutil.KustomizationID("ns", "rs"):        nil,
		testutil.KustomizationID("ns", "keepalive"): nil,
	})
	f.drainRerun[testutil.KustomizationID("ns", "rs")] = true
	gate := make(chan struct{})
	f.gate[testutil.KustomizationID("ns", "keepalive")] = gate

	ts := task.NewBounded(8)
	s := New(ts, f)
	s.Seed([]NodeID{testutil.KustomizationID("ns", "rs"), testutil.KustomizationID("ns", "keepalive")})
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()

	// rs ran once. Now a late data arrival dirties the store; with the pool held
	// non-idle by keepalive, the fixpoint can't fire until we release.
	waitUntil(t, func() bool { return f.runCount(testutil.KustomizationID("ns", "rs")) >= 1 })
	s.OnArrival(NodeID{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "late"}, false)
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if rc := f.runCount(testutil.KustomizationID("ns", "rs")); rc != 2 {
		t.Fatalf("rerun node ran %d times; want exactly 2 (initial + one re-expansion)", rc)
	}
	assertReady(t, f, "rs")
}

func TestDrainRerunTerminatesWithoutArrival(t *testing.T) {
	t.Parallel()
	// With no arrival after its run, a rerun node converges: the store is never
	// re-dirtied, so the fixpoint does NOT re-run it. This is the termination
	// bound — a sweep requires a fresh arrival.
	f := newFake(map[NodeID][]NodeID{testutil.KustomizationID("ns", "rs"): nil})
	f.drainRerun[testutil.KustomizationID("ns", "rs")] = true
	run(t, f, 8)
	if rc := f.runCount(testutil.KustomizationID("ns", "rs")); rc != 1 {
		t.Fatalf("drain-rerun node ran %d times with no arrival; want exactly 1 (no re-run)", rc)
	}
	assertReady(t, f, "rs")
}

func TestDanglingChainRunCountBounded(t *testing.T) {
	t.Parallel()
	// A depth-5 dangling chain must terminalize with a bounded number of
	// re-runs per node (no exponential blowup / no infinite re-park loop).
	g := map[NodeID][]NodeID{}
	names := []string{"n0", "n1", "n2", "n3", "n4"}
	for i, n := range names {
		if i+1 < len(names) {
			g[testutil.KustomizationID("ns", n)] = []NodeID{testutil.KustomizationID("ns", names[i+1])}
		} else {
			g[testutil.KustomizationID("ns", n)] = []NodeID{testutil.KustomizationID("ns", "absent")} // leaf blocks on absent
		}
	}
	f := newFake(g)
	run(t, f, 8)
	for _, n := range names {
		assertFailed(t, f, n)
		if rc := f.runCount(testutil.KustomizationID("ns", n)); rc > 6 {
			t.Fatalf("%s ran %d times; expected a small bounded count", n, rc)
		}
	}
}

func TestRedispatch_ContentBudget(t *testing.T) {
	for _, mode := range []string{"terminal", "blocked", "alternating"} {
		for _, requests := range []int{maxRedispatches, maxRedispatches + 1} {
			t.Run(fmt.Sprintf("%s/%d", mode, requests), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var s *Scheduler
				var runs int
				started, finished := make(chan struct{}), make(chan struct{})
				victim, companion := testutil.KustomizationID("ns", "content"), testutil.KustomizationID("ns", "companion")
				s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ manifest.DrainLevel) (Outcome, manifest.ReconcileWait) {
					if nid == companion {
						close(started)
						select {
						case <-finished:
						case <-ctx.Done():
						}
						return Outcome{}, manifest.ReconcileWait{}
					}
					select {
					case <-started:
					case <-ctx.Done():
						return Outcome{}, manifest.ReconcileWait{}
					}
					runs++
					if runs > requests {
						close(finished)
						return Outcome{}, manifest.ReconcileWait{}
					}
					s.OnArrival(nid, true)
					s.OnArrival(nid, true)
					if mode == "blocked" || mode == "alternating" && runs%2 == 0 {
						return Outcome{Kind: Blocked}, manifest.ReconcileWait{Deps: []NodeID{testutil.KustomizationID("ns", "missing")}}
					}
					return Outcome{}, manifest.ReconcileWait{}
				}))
				s.Seed([]NodeID{victim, companion})
				err := s.Run(ctx)
				if requests == maxRedispatches {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil || !strings.Contains(err.Error(), victim.String()) {
					t.Fatalf("Run error = %v, want node-specific non-convergence", err)
				}
				if ctx.Err() != nil || runs != maxRedispatches+1 {
					t.Fatalf("dispatches=%d, context=%v; want %d executions before deadline", runs, ctx.Err(), maxRedispatches+1)
				}
			})
		}
	}
}

type opportunityDispatcher func(context.Context, NodeID, manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait)

func (d opportunityDispatcher) Dispatch(ctx context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
	return d(ctx, id, attempt)
}

func TestOpportunity_UnscheduledAdmission(t *testing.T) {
	t.Parallel()
	for _, viaSeed := range []bool{false, true} {
		t.Run(fmt.Sprintf("seed=%t", viaSeed), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				reader, supplier := testutil.KustomizationID("ns", "reader"), testutil.KustomizationID("ns", "supplier")
				target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "values"}
				declaration := &manifest.Kustomization{Name: reader.Name, Namespace: reader.Namespace}
				bound := make(chan manifest.Opportunity, 1)
				releaseReader, releaseSupplier := make(chan struct{}), make(chan struct{})
				var supplied atomic.Int64
				first := true
				d := opportunityDispatcher(func(_ context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
					if id == supplier {
						supplied.Add(1)
						<-releaseSupplier
						return Outcome{}, manifest.ReconcileWait{}
					}
					o := manifest.Opportunity{NamedResource: supplier}
					if attempt.Content != nil && len(attempt.Content.Suppliers) > 0 {
						o = attempt.Content.Suppliers[0]
						if first {
							first = false
							bound <- o
							<-releaseReader
						}
						o.Observed = o.Word.Load()
						if o.Observed&1 == 0 {
							return Outcome{}, manifest.ReconcileWait{}
						}
					}
					return Outcome{Kind: Blocked}, manifest.ReconcileWait{Content: &manifest.ContentWait{Targets: []NodeID{target}, Suppliers: []manifest.Opportunity{o}, Declaration: declaration}}
				})
				sched := New(task.NewBounded(2), d)
				sched.Seed([]NodeID{reader})
				done := make(chan error, 1)
				go func() { done <- sched.Run(t.Context()) }()
				cell := <-bound
				assert.Equal(t, cell.Word.Load(), uint64(0))
				sched.OnArrival(supplier, false)
				synctest.Wait()
				assert.Equal(t, supplied.Load(), int64(0))
				assert.Equal(t, cell.Word.Load(), uint64(0))
				if viaSeed {
					sched.Seed([]NodeID{supplier})
				} else {
					sched.OnArrival(supplier, true)
				}
				synctest.Wait()
				wantDispatches := int64(1)
				if viaSeed {
					wantDispatches = 0
				}
				assert.Equal(t, supplied.Load(), wantDispatches)
				if cell.Word.Load()&1 == 0 {
					t.Error("admission did not open the bound cell")
				}
				sched.mu.Lock()
				if cell.Word != sched.nodes[supplier].opportunity {
					t.Error("admission replaced the stable cell")
				}
				sched.mu.Unlock()
				close(releaseReader)
				synctest.Wait()
				close(releaseSupplier)
				assert.NoError(t, <-done)
				assert.Equal(t, cell.Word.Load()&1, uint64(0))
			})
		})
	}
}

func TestOpportunity_OpenBeforeEnqueue(t *testing.T) {
	t.Parallel()
	sched := New(task.NewBounded(2), opportunityDispatcher(func(context.Context, NodeID, manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
		return Outcome{}, manifest.ReconcileWait{}
	}))
	seed, arrival := testutil.KustomizationID("ns", "seed"), testutil.KustomizationID("ns", "arrival")
	sched.nodes[seed] = &node{id: seed, state: stateUnscheduled, opportunity: new(atomic.Uint64)}
	sched.nodes[arrival] = &node{id: arrival, state: stateUnscheduled, opportunity: new(atomic.Uint64)}
	sched.Seed([]NodeID{seed})
	assert.Equal(t, sched.nodes[seed].opportunity.Load()&1, uint64(1))
	assert.Equal(t, slices.Contains(sched.runq, seed), true)
	sched.OnArrival(arrival, true)
	assert.Equal(t, sched.nodes[arrival].opportunity.Load()&1, uint64(1))
	assert.Equal(t, slices.Contains(sched.runq, arrival), true)
	word := sched.nodes[arrival].opportunity.Load()
	sched.OnArrival(arrival, true)
	assert.Equal(t, sched.nodes[arrival].opportunity.Load(), word+2)
}

func TestOpportunity_UnobservedNode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome Outcome
	}{
		{"terminal", Outcome{}}, {"noop", Outcome{Noop: true}}, {"dependency failed", Outcome{Kind: DependencyFailed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := testutil.KustomizationID("ns", "unobserved")
			var sched *Scheduler
			tasks := task.NewBounded(2)
			runs := 0
			sched = New(tasks, opportunityDispatcher(func(_ context.Context, _ NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
				runs++
				assert.Equal(t, attempt.Content == nil, true)
				sched.mu.Lock()
				assert.Equal(t, len(sched.nodes[id].blockedOn), 0)
				assert.Equal(t, sched.nodes[id].opportunity == nil, true)
				sched.mu.Unlock()
				if runs == 1 {
					sched.OnArrival(id, true)
				}
				wait := manifest.ReconcileWait{}
				if tc.outcome.Kind == DependencyFailed {
					wait.Deps = []NodeID{testutil.KustomizationID("ns", "missing")}
				}
				return tc.outcome, wait
			}))
			sched.Seed([]NodeID{id})
			assert.Equal(t, sched.nodes[id].opportunity == nil, true)
			assert.NoError(t, sched.Run(t.Context()))
			assert.Equal(t, tasks.Failures(), int64(0))
			assert.Equal(t, runs, 2)
			assert.Equal(t, sched.nodes[id].opportunity == nil, true)
			assert.Equal(t, sched.nodes[id].content == nil, true)
			assert.Equal(t, len(sched.nodes[id].blockedOn), 0)
		})
	}
}

func TestOpportunity_InvocationSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome Outcome
	}{
		{"terminal", Outcome{}}, {"noop", Outcome{Noop: true}}, {"dependency failed", Outcome{Kind: DependencyFailed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				reader, supplier, unobserved := testutil.KustomizationID("ns", "reader"), testutil.KustomizationID("ns", "supplier"), testutil.KustomizationID("ns", "unobserved")
				declarations := map[NodeID]*manifest.Kustomization{
					reader:   {Name: reader.Name, Namespace: reader.Namespace},
					supplier: {Name: supplier.Name, Namespace: supplier.Namespace},
				}
				captured := make(chan manifest.ReconcileAttempt, 1)
				release := make(chan struct{})
				var first atomic.Pointer[manifest.ContentWait]
				var cutSeen atomic.Bool
				var successors atomic.Int64
				var readerDispatches atomic.Int64
				var sched *Scheduler
				d := opportunityDispatcher(func(_ context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
					if id == reader {
						readerDispatches.Add(1)
					}
					if id == unobserved {
						wait := manifest.ReconcileWait{}
						if tc.outcome.Kind == DependencyFailed {
							wait.Deps = []NodeID{testutil.KustomizationID("ns", "missing")}
						}
						return tc.outcome, wait
					}
					if id == reader && cutSeen.Load() {
						successors.Add(1)
						if attempt.Content == nil || attempt.Content.Declaration != nil || len(attempt.Content.Suppliers) != 1 || !slices.Equal(attempt.Content.Targets, []NodeID{supplier}) {
							t.Errorf("successor lost retained bindings: %+v", attempt)
						}
						return Outcome{}, manifest.ReconcileWait{}
					}
					other := reader
					if id == reader {
						other = supplier
					}
					o := manifest.Opportunity{NamedResource: other}
					if attempt.Content != nil && len(attempt.Content.Suppliers) != 0 {
						o = attempt.Content.Suppliers[0]
						if attempt.Drain == manifest.DrainForce && o.Cut != 0 {
							if id == reader {
								cutSeen.Store(true)
								captured <- attempt
							}
							<-release
							return Outcome{}, manifest.ReconcileWait{}
						}
						o.Observed = o.Word.Load()
					}
					content := &manifest.ContentWait{Targets: []NodeID{other}, Suppliers: []manifest.Opportunity{o}, Declaration: declarations[id]}
					if id == reader {
						first.CompareAndSwap(nil, content)
					}
					return Outcome{Kind: Blocked}, manifest.ReconcileWait{Content: content}
				})
				sched = New(task.NewBounded(2), d)
				sched.Seed([]NodeID{reader, supplier})
				sched.OnArrival(unobserved, true)
				done := make(chan error, 1)
				go func() { done <- sched.Run(t.Context()) }()
				invocation := <-captured
				before := slices.Clone(invocation.Content.Suppliers)
				sched.OnArrival(reader, true)
				assert.Equal(t, invocation.Content.Declaration, manifest.BaseManifest(declarations[reader]))
				assert.Equal(t, slices.Equal(invocation.Content.Suppliers, before), true)
				assert.Equal(t, first.Load().Suppliers[0].Word == nil, true)
				sched.mu.Lock()
				assert.Equal(t, sched.nodes[reader].content != invocation.Content, true)
				successor := sched.nodes[reader].content
				assert.Equal(t, slices.Equal(successor.Targets, invocation.Content.Targets), true)
				assert.Equal(t, slices.Equal(successor.Suppliers, invocation.Content.Suppliers), true)
				assert.Equal(t, sched.nodes[unobserved].opportunity == nil, true)
				assert.Equal(t, sched.nodes[unobserved].content == nil, true)
				sched.mu.Unlock()
				sched.OnArrival(reader, true)
				sched.mu.Lock()
				assert.Equal(t, sched.nodes[reader].content == successor, true)
				sched.mu.Unlock()
				close(release)
				assert.NoError(t, <-done)
				assert.Equal(t, successors.Load(), int64(1))
				assert.Equal(t, readerDispatches.Load(), int64(5))
				t.Logf("reader dispatches=%d, successor dispatches=%d", readerDispatches.Load(), successors.Load())
				for _, n := range sched.nodes {
					assert.Equal(t, len(n.blockedOn), 0)
					assert.Equal(t, n.content == nil, true)
				}
			})
		})
	}
}

func TestOpportunity_TerminalBinding(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		reader, supplier := testutil.KustomizationID("ns", "reader"), testutil.KustomizationID("ns", "supplier")
		release := make(chan struct{})
		runs := 0
		d := opportunityDispatcher(func(_ context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait) {
			if id == supplier {
				return Outcome{}, manifest.ReconcileWait{}
			}
			runs++
			<-release
			if attempt.Content == nil {
				return Outcome{Kind: Blocked}, manifest.ReconcileWait{Content: &manifest.ContentWait{Suppliers: []manifest.Opportunity{{NamedResource: supplier}}}}
			}
			o := attempt.Content.Suppliers[0]
			assert.Equal(t, o.Observed, uint64(0))
			assert.Equal(t, o.Word.Load(), uint64(0))
			return Outcome{}, manifest.ReconcileWait{}
		})
		sched := New(task.NewBounded(2), d)
		sched.Seed([]NodeID{reader, supplier})
		done := make(chan error, 1)
		go func() { done <- sched.Run(t.Context()) }()
		synctest.Wait()
		sched.mu.Lock()
		assert.Equal(t, sched.nodes[supplier].state, stateTerminal)
		assert.Equal(t, sched.nodes[supplier].opportunity == nil, true)
		sched.mu.Unlock()
		close(release)
		assert.NoError(t, <-done)
		assert.Equal(t, runs, 2)
	})
}
