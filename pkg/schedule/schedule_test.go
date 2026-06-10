package schedule

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

// id builds a Kustomization-kind NodeID for tests.
func id(name string) NodeID {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
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

func (f *fakeDisp) Dispatch(_ context.Context, nid NodeID, drainLevel int) (Outcome, []NodeID, bool) {
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
	failed := false
	for _, d := range deps {
		if f.termAny[d] {
			if !f.termReady[d] {
				failed = true
			}
			continue // terminal-ready -> satisfied
		}
		if _, isNode := f.graph[d]; !isNode {
			// absent dep
			if drainLevel >= DrainCascade {
				failed = true
				continue
			}
			blocked = append(blocked, d)
			continue
		}
		// present but not yet terminal (pending)
		if drainLevel >= DrainForce {
			failed = true
			continue
		}
		blocked = append(blocked, d)
	}
	switch {
	case failed:
		f.termAny[nid] = true
		f.termReady[nid] = false
		return OutcomeTerminal, nil, false
	case len(blocked) > 0:
		return OutcomeBlocked, blocked, false
	default:
		f.termAny[nid] = true
		f.termReady[nid] = true
		return OutcomeTerminal, nil, true
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
	s.Run(context.Background())
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
	done := make(chan struct{})
	go func() { s.Run(context.Background()); close(done) }()

	// Wait until a has actually parked (its first Dispatch ran and reported
	// Blocked) before introducing x, so we exercise park-then-wake.
	waitUntil(t, func() bool { return f.runCount(id("a")) >= 1 })
	s.OnArrival(id("x"), true) // x appears (render-discovered) -> wakes a
	waitUntil(t, func() bool { term, _ := f.state(id("a")); return term })
	close(gate) // let keepalive finish so Run can reach the fixpoint
	<-done
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
	done := make(chan struct{})
	go func() { s.Run(context.Background()); close(done) }()

	// rs ran once. Now a late data arrival dirties the store; with the pool held
	// non-idle by keepalive, the fixpoint can't fire until we release.
	waitUntil(t, func() bool { return f.runCount(id("rs")) >= 1 })
	s.OnArrival(NodeID{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "late"}, false)
	close(gate)
	<-done

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
