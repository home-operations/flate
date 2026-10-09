package schedule

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

// id builds a Kustomization-kind NodeID for tests.
func id(name string) NodeID {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

type dispatchFunc func(context.Context, NodeID, int) (Outcome, []NodeID)

func (f dispatchFunc) Dispatch(ctx context.Context, id NodeID, drain int) (Outcome, []NodeID) {
	return f(ctx, id, drain)
}

func TestScheduler_RunCancellationBoundary(t *testing.T) {
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) {
				if tc.cancelDispatch {
					cancel()
				}
				return OutcomeTerminal, nil
			}))
			if tc.seed {
				s.Seed([]NodeID{id("last")})
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
			victim, held := id("feedback"), id("held")
			var active, runs atomic.Int64
			var s *Scheduler
			s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				active.Add(1)
				defer active.Add(-1)
				runs.Add(1)
				started <- struct{}{}
				if capFirst && nid == victim {
					if awaitSignal(ctx, fail) {
						s.OnArrival(victim, true)
					}
					return OutcomeTerminal, nil
				}
				<-ctx.Done()
				stopping <- struct{}{}
				awaitSignal(checkCtx, release)
				return OutcomeTerminal, nil
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
			s.OnArrival(id("late"), true)
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
	a, b, c, d := id("a"), id("b"), id("c"), id("d")
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
			n := &node{id: id("consumer"), state: stateRunning}
			s.nodes[n.id], s.inFlight = n, 1
			s.complete(n.id, OutcomeDependencyFailed, []NodeID{a, b, c}, false)
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
	for _, fanIn := range []int{2, 32} {
		for _, position := range []int{0, fanIn - 1} {
			t.Run(fmt.Sprintf("fanin_%d/position_%d", fanIn, position), func(t *testing.T) {
				s := New(task.NewBounded(2), nil)
				n := &node{id: id("consumer"), state: stateRunning}
				s.nodes[n.id], s.inFlight = n, 1
				previous, blocked := make([]NodeID, fanIn), make([]NodeID, fanIn)
				other := id("other")
				for i := range previous {
					previous[i] = id(fmt.Sprintf("blocker-%02d", i))
					blocked[i] = previous[i]
					s.failedIdx[previous[i]] = map[NodeID]struct{}{other: {}}
				}
				s.complete(n.id, OutcomeDependencyFailed, previous, false)
				blocked[position] = id("replacement")
				n.state, s.inFlight = stateRunning, 1
				s.complete(n.id, OutcomeDependencyFailed, blocked, false)
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
	s := New(task.NewBounded(2), nil)
	n := &node{id: id("consumer")}
	s.nodes[n.id] = n
	a, b, c := id("a"), id("b"), id("c")
	complete := func(out Outcome, blocked, want []NodeID) {
		t.Helper()
		n.state, s.inFlight = stateRunning, 1
		s.complete(n.id, out, blocked, false)
		assert.Diff(t, n.failedOn, want)
		assert.Equal(t, len(s.failedIdx), len(want))
		for _, dep := range want {
			_, retained := s.failedIdx[dep][n.id]
			assert.Equal(t, retained, true)
		}
	}
	complete(OutcomeDependencyFailed, []NodeID{a, b, c}, []NodeID{a, b, c})
	complete(OutcomeDependencyFailed, []NodeID{b, a}, []NodeID{b, a})
	complete(OutcomeDependencyFailed, []NodeID{a, a}, []NodeID{a})
	complete(OutcomeDependencyFailed, []NodeID{b, a, b, c, a}, []NodeID{b, a, c})
	complete(OutcomeDependencyFailed, []NodeID{c, a, b, c, a}, []NodeID{b, a, c})
	complete(OutcomeTerminal, nil, nil)
	complete(OutcomeDependencyFailed, []NodeID{a, b, c}, []NodeID{a, b, c})
}

func TestRedispatchLimit(t *testing.T) {
	for _, mode := range []string{"running arrival", "terminal arrival", "drain replay"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var s *Scheduler
			var runs atomic.Int64
			victim := id("feedback")
			producer := id("producer")
			disp := dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				if nid == producer {
					for ctx.Err() == nil {
						s.mu.Lock()
						terminal := s.nodes[victim].state == stateTerminal
						err := s.err
						s.mu.Unlock()
						if err != nil {
							break
						}
						if terminal {
							s.OnArrival(victim, true)
						} else {
							runtime.Gosched()
						}
					}
					return OutcomeTerminal, nil
				}
				runs.Add(1)
				switch mode {
				case "running arrival":
					s.OnArrival(victim, true)
				case "drain replay":
					s.OnArrival(NodeID{Kind: manifest.KindConfigMap, Name: "values"}, false)
				}
				return OutcomeTerminal, nil
			})
			s = New(task.NewBounded(2), disp)
			s.SetRerunAtDrain(func(NodeID) bool { return mode == "drain replay" })
			s.Seed([]NodeID{victim})
			if mode == "terminal arrival" {
				s.Seed([]NodeID{producer})
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
	for _, outcome := range []Outcome{OutcomeTerminal, OutcomeBlocked} {
		t.Run(fmt.Sprintf("outcome_%d", outcome), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			limit := maxRedispatches
			var s *Scheduler
			runs := 0
			s = New(task.NewBounded(2), dispatchFunc(func(_ context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				runs++
				if runs <= limit {
					s.OnArrival(nid, true)
					return outcome, []NodeID{id("dep")}
				}
				return OutcomeTerminal, nil
			}))
			s.Seed([]NodeID{id("finite")})
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
//     DrainNone           -> block
//     DrainCascade/Force  -> fail ("dependency not found")
//   - a dep present-but-Pending (a graph node not yet terminal):
//     DrainNone/Cascade   -> block
//     DrainForce          -> fail ("not ready")
//
// A "failing leaf" is modeled as a node depending on an id absent from the
// graph. The fake records terminal state + per-node dispatch counts + the max
// drain level it observed, all under its own mutex.
type fakeDisp struct {
	graph map[NodeID][]NodeID // node -> deps

	// gate, if set for an id, blocks that id's Dispatch until the channel is
	// closed — used to force interleavings (e.g. the lost-wakeup race).
	gate map[NodeID]chan struct{}

	// drainRerun, if set for an id, makes Dispatch return rerunAtDrain=true for
	// it — modeling a selector ResourceSet that re-expands at the fixpoint.
	drainRerun map[NodeID]bool

	mu        sync.Mutex
	termReady map[NodeID]bool
	termAny   map[NodeID]bool
	runs      map[NodeID]int
	maxDrain  int
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

func (f *fakeDisp) Dispatch(_ context.Context, nid NodeID, drainLevel int) (Outcome, []NodeID) {
	f.mu.Lock()
	g := f.gate[nid]
	f.mu.Unlock()
	if g != nil {
		<-g // block until the test releases this node
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[nid]++
	if drainLevel > f.maxDrain {
		f.maxDrain = drainLevel
	}
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
			if drainLevel >= DrainCascade {
				failed = append(failed, d)
				continue
			}
			blocked = append(blocked, d)
			continue
		}
		// present but not yet terminal (pending)
		if drainLevel >= DrainForce {
			failed = append(failed, d)
			continue
		}
		blocked = append(blocked, d)
	}
	switch {
	case len(failed) > 0:
		f.termAny[nid] = true
		f.termReady[nid] = false
		return OutcomeDependencyFailed, failed
	case len(blocked) > 0:
		return OutcomeBlocked, blocked
	default:
		f.termAny[nid] = true
		f.termReady[nid] = true
		return OutcomeTerminal, nil
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

func (f *fakeDisp) maxDrainLevel() int {
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
	s.SetRerunAtDrain(func(id NodeID) bool { return f.drainRerun[id] })
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
	term, ready := f.state(id(name))
	if !term || !ready {
		t.Fatalf("%s: want terminal-Ready, got terminal=%v ready=%v", name, term, ready)
	}
}

func assertFailed(t *testing.T, f *fakeDisp, name string) {
	t.Helper()
	term, ready := f.state(id(name))
	if !term || ready {
		t.Fatalf("%s: want terminal-Failed, got terminal=%v ready=%v", name, term, ready)
	}
}

func TestLinearChainReadyPropagation(t *testing.T) {
	// a -> b -> c (c is a ready leaf).
	f := newFake(map[NodeID][]NodeID{
		id("a"): {id("b")},
		id("b"): {id("c")},
		id("c"): nil,
	})
	run(t, f, 8)
	assertReady(t, f, "a")
	assertReady(t, f, "b")
	assertReady(t, f, "c")
	if f.maxDrainLevel() != DrainNone {
		t.Fatalf("a healthy chain must resolve without draining; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestDiamondReadyPropagation(t *testing.T) {
	// d -> {b, c}; b -> a; c -> a; a ready leaf.
	f := newFake(map[NodeID][]NodeID{
		id("d"): {id("b"), id("c")},
		id("b"): {id("a")},
		id("c"): {id("a")},
		id("a"): nil,
	})
	run(t, f, 8)
	for _, n := range []string{"a", "b", "c", "d"} {
		assertReady(t, f, n)
	}
}

func TestDanglingChainCascadeFails(t *testing.T) {
	// a -> b -> (absent X). Both must fail; the leaf's "dependency not found"
	// cascades up. DrainCascade suffices (no DrainForce needed for a chain).
	f := newFake(map[NodeID][]NodeID{
		id("a"): {id("b")},
		id("b"): {id("missing")}, // "missing" is absent from the graph
	})
	run(t, f, 8)
	assertFailed(t, f, "a")
	assertFailed(t, f, "b")
	if f.maxDrainLevel() != DrainCascade {
		t.Fatalf("a dangling chain should drain at DrainCascade, not DrainForce; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestParkThenArrivalWake(t *testing.T) {
	// a -> x, but x is NOT seeded; it must arrive via OnArrival (render
	// discovery) and wake the parked a. A gated "keepalive" node holds the pool
	// non-idle so the scheduler cannot reach the fixpoint and drain a before x
	// arrives — forcing the genuine park-then-wake path deterministically.
	f := newFake(map[NodeID][]NodeID{
		id("a"):         {id("x")},
		id("x"):         nil, // x is a ready leaf once known
		id("keepalive"): nil,
	})
	gate := make(chan struct{})
	f.gate[id("keepalive")] = gate
	ts := task.NewBounded(8)
	s := New(ts, f)
	s.Seed([]NodeID{id("a"), id("keepalive")}) // x absent at start
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()

	// Wait until a has actually parked (its first Dispatch ran and reported
	// Blocked) before introducing x, so we exercise park-then-wake.
	waitUntil(t, func() bool { return f.runCount(id("a")) >= 1 })
	s.OnArrival(id("x"), true) // x appears (render-discovered) -> wakes a
	waitUntil(t, func() bool { term, _ := f.state(id("a")); return term })
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
	// Force the race: a's Dispatch returns OutcomeBlocked on b only AFTER b has
	// already terminalized. complete(a)'s scheduler-state re-check must see b
	// terminal and re-queue a (not park it on an already-fired wake). a must
	// resolve Ready WITHOUT entering draining — proving the re-check, not the
	// draining backstop, recovered it.
	f := newFake(map[NodeID][]NodeID{
		id("a"): {id("b")},
		id("b"): nil,
	})
	gate := make(chan struct{})
	f.gate[id("a")] = gate
	// Release a's Dispatch once b is terminal.
	go func() {
		for {
			if term, _ := f.state(id("b")); term {
				close(gate)
				return
			}
		}
	}()
	run(t, f, 8)
	assertReady(t, f, "a")
	assertReady(t, f, "b")
	if f.maxDrainLevel() != DrainNone {
		t.Fatalf("lost-wakeup re-check should recover at DrainNone, not via draining; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestTransitiveBlockCompareOrderCascades(t *testing.T) {
	// Names chosen so the CONSUMER sorts before its blocker in requeue order
	// ("a-top" < "z-leaf"), forcing the draining sweep to re-run the consumer
	// first; the complete() wake must still cascade the leaf's failure upward.
	f := newFake(map[NodeID][]NodeID{
		id("a-top"):  {id("z-leaf")},
		id("z-leaf"): {id("absent")},
	})
	run(t, f, 8)
	assertFailed(t, f, "a-top")
	assertFailed(t, f, "z-leaf")
}

func TestCrossKindCycleForceDrains(t *testing.T) {
	// a <-> b mutual dependency (no same-kind preflight here): DrainCascade
	// cannot break it (each sees the other present-Pending and re-parks), so
	// the scheduler must escalate to DrainForce and fail both.
	f := newFake(map[NodeID][]NodeID{
		id("a"): {id("b")},
		id("b"): {id("a")},
	})
	run(t, f, 8)
	assertFailed(t, f, "a")
	assertFailed(t, f, "b")
	if f.maxDrainLevel() != DrainForce {
		t.Fatalf("a cycle must escalate to DrainForce; maxDrain=%d", f.maxDrainLevel())
	}
}

func TestDrainRerunReexpandsOnArrival(t *testing.T) {
	// A rerun node (a selector ResourceSet) terminalizes immediately, then a
	// later arrival dirties the store. At the structural fixpoint the node must
	// re-run once — re-expanding against the now-complete store — even though it
	// parked on nothing and the arriving id is not one it waited on.
	f := newFake(map[NodeID][]NodeID{
		id("rs"):        nil,
		id("keepalive"): nil,
	})
	f.drainRerun[id("rs")] = true
	gate := make(chan struct{})
	f.gate[id("keepalive")] = gate

	ts := task.NewBounded(8)
	s := New(ts, f)
	s.SetRerunAtDrain(func(id NodeID) bool { return f.drainRerun[id] })
	s.Seed([]NodeID{id("rs"), id("keepalive")})
	done := make(chan error, 1)
	go func() { done <- s.Run(t.Context()) }()

	// rs ran once. Now a late data arrival dirties the store; with the pool held
	// non-idle by keepalive, the fixpoint can't fire until we release.
	waitUntil(t, func() bool { return f.runCount(id("rs")) >= 1 })
	s.OnArrival(NodeID{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "late"}, false)
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if rc := f.runCount(id("rs")); rc != 2 {
		t.Fatalf("rerun node ran %d times; want exactly 2 (initial + one re-expansion)", rc)
	}
	assertReady(t, f, "rs")
}

func TestDrainRerunTerminatesWithoutArrival(t *testing.T) {
	// With no arrival after its run, a rerun node converges: the store is never
	// re-dirtied, so the fixpoint does NOT re-run it. This is the termination
	// bound — a sweep requires a fresh arrival.
	f := newFake(map[NodeID][]NodeID{id("rs"): nil})
	f.drainRerun[id("rs")] = true
	run(t, f, 8)
	if rc := f.runCount(id("rs")); rc != 1 {
		t.Fatalf("drain-rerun node ran %d times with no arrival; want exactly 1 (no re-run)", rc)
	}
	assertReady(t, f, "rs")
}

func TestDanglingChainRunCountBounded(t *testing.T) {
	// A depth-5 dangling chain must terminalize with a bounded number of
	// re-runs per node (no exponential blowup / no infinite re-park loop).
	g := map[NodeID][]NodeID{}
	names := []string{"n0", "n1", "n2", "n3", "n4"}
	for i, n := range names {
		if i+1 < len(names) {
			g[id(n)] = []NodeID{id(names[i+1])}
		} else {
			g[id(n)] = []NodeID{id("absent")} // leaf blocks on absent
		}
	}
	f := newFake(g)
	run(t, f, 8)
	for _, n := range names {
		assertFailed(t, f, n)
		if rc := f.runCount(id(n)); rc > 6 {
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
				victim, companion := id("content"), id("companion")
				s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
					if nid == companion {
						close(started)
						select {
						case <-finished:
						case <-ctx.Done():
						}
						return OutcomeTerminal, nil
					}
					select {
					case <-started:
					case <-ctx.Done():
						return OutcomeTerminal, nil
					}
					runs++
					if runs > requests {
						close(finished)
						return OutcomeTerminal, nil
					}
					s.OnArrival(nid, true)
					s.OnArrival(nid, true)
					if mode == "blocked" || mode == "alternating" && runs%2 == 0 {
						return OutcomeBlocked, []NodeID{id("missing")}
					}
					return OutcomeTerminal, nil
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
