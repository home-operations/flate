// Package schedule provides flate's dependency-driven reconcile scheduler:
// a re-entrant fixpoint engine that runs each node's reconcile body on a
// bounded task pool, parks a body that reports unsatisfied dependencies
// (Dispatcher OutcomeBlocked), and re-runs it when any of those dependencies
// advances.
//
// Termination is STRUCTURAL, not timed. Every render emission is a synchronous
// store write on the body's own task goroutine, completing before the body
// returns and before the scheduler decrements its in-flight count. Therefore
// when no body is in flight and the runnable frontier is empty, no new object
// can ever appear — so any still-parked node's dependencies are provably
// unproducible. A draining sweep then terminalizes those nodes with the
// canonical "dependency not found" / cascade / "not ready" statuses. There is
// no per-dependency timeout and no shared
// quiescence counter, so the #666 transient-drain false-drop cannot occur: a
// parked node is never counted in flight, and nothing drops it except the
// fixpoint, which only fires when nothing is running.
//
// The package depends only on pkg/manifest and pkg/task; all store and
// controller interaction is behind the Dispatcher seam, so the scheduler is
// unit-testable against a fake Dispatcher with no store or controllers.
package schedule

import (
	"context"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

// NodeID identifies a schedulable node — a Kustomization, HelmRelease, or
// source CR — by its store identity.
type NodeID = manifest.NamedResource

// Outcome is what one reconcile-body invocation reported via the Dispatcher.
type Outcome int

const (
	// OutcomeTerminal means the body wrote a terminal store status
	// (Ready/Skipped/Failed); the node is done unless a later store event
	// re-emits or resets it.
	OutcomeTerminal Outcome = iota
	// OutcomeBlocked means the body could not proceed because one or more
	// dependencies are unsatisfied; the scheduler parks the node keyed on the
	// returned ids and re-runs it when any advances.
	OutcomeBlocked
)

// Drain levels passed to Dispatcher.Dispatch. 0 is normal operation; the
// scheduler escalates only at the structural fixpoint (nothing in flight,
// nodes still parked).
const (
	// DrainNone: normal operation — an unsatisfiable dependency parks.
	DrainNone = 0
	// DrainCascade: an absent dependency (and a never-true ReadyExpr)
	// terminalizes as a failure; a present-but-Pending dependency still
	// parks, so a dangling chain fails leaf-first and each level cascades
	// the child's real terminal message upward.
	DrainCascade = 1
	// DrainForce: a present-but-Pending dependency ALSO terminalizes ("not
	// ready"). Reached only when a DrainCascade pass made no progress — i.e.
	// a cross-kind structural cycle the same-kind preflight detector cannot
	// represent; forcing the failure breaks it.
	DrainForce = 2
)

// Dispatcher runs a node's reconcile body. The orchestrator supplies the
// concrete implementation, closing over the store and the three controllers;
// the scheduler never sees a store or controller type.
type Dispatcher interface {
	// Dispatch invokes id's reconcile body synchronously on the calling
	// goroutine (a task.Service worker) and reports back:
	//   - out: OutcomeTerminal or OutcomeBlocked.
	//   - blocked: unsatisfied dependency ids (non-nil only when Blocked).
	// drainLevel is one of DrainNone/DrainCascade/DrainForce.
	Dispatch(ctx context.Context, id NodeID, drainLevel int) (out Outcome, blocked []NodeID)
}

type nodeState uint8

const (
	stateRunnable nodeState = iota
	stateRunning
	stateParked
	stateTerminal
)

type node struct {
	id        NodeID
	state     nodeState
	blockedOn []NodeID // deps recorded at the last OutcomeBlocked
	// rerunRequested is set when a wake arrives while the node is running, so
	// complete() re-queues it once instead of dropping the wake (the re-run
	// re-reads the store and re-evaluates its gate against current state).
	rerunRequested bool
	// rerun marks a node that re-runs at the structural fixpoint — a
	// ResourceSet whose selector-only inputsFrom has no nameable producer to
	// park on, so it must re-expand once the store has quiesced. Set from the
	// scheduler's rerunAtDrain predicate after the node's first dispatch.
	rerun bool
}

// Scheduler is a re-entrant fixpoint reconcile driver. Construct with New,
// Seed the initial node set, wire store events to OnArrival/OnStatusWake,
// then call Run.
type Scheduler struct {
	tasks *task.Service
	disp  Dispatcher

	mu        sync.Mutex
	cond      *sync.Cond
	nodes     map[NodeID]*node
	runq      []NodeID
	parkedIdx map[NodeID]map[NodeID]struct{} // dep id -> set of nodes parked on it
	inFlight  int                            // count of stateRunning nodes (EXCLUDES parked)
	draining  int                            // DrainNone/DrainCascade/DrainForce
	canceled  bool
	// dirty records that an object arrived since the last quiescence sweep. A
	// rerun node re-expands at the structural fixpoint only when the store has
	// grown since it last ran; the sweep clears dirty, so a sweep that produces
	// nothing new (every re-render a dedup no-op) leaves it clear and the run
	// terminates. Arrivals are finite and monotone, so sweeps are bounded.
	dirty bool
	// rerunAtDrain reports whether a node wants to re-run at the fixpoint. Set
	// by the orchestrator (SetRerunAtDrain); evaluated off the hot path in the
	// dispatch goroutine, never under mu.
	rerunAtDrain func(NodeID) bool
}

// SetRerunAtDrain installs the predicate that decides whether a node re-runs at
// the structural fixpoint (a selector-only ResourceSet, which has no nameable
// input provider to park on). It is evaluated in the dispatch goroutine after
// each Dispatch, so it may read the store. Optional — nil means no node reruns.
func (s *Scheduler) SetRerunAtDrain(fn func(NodeID) bool) { s.rerunAtDrain = fn }

// New constructs a Scheduler that runs bodies on tasks via disp.
func New(tasks *task.Service, disp Dispatcher) *Scheduler {
	s := &Scheduler{
		tasks:     tasks,
		disp:      disp,
		nodes:     map[NodeID]*node{},
		parkedIdx: map[NodeID]map[NodeID]struct{}{},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Seed registers the initial node set (file-loaded Kustomizations,
// HelmReleases, and source CRs from Bootstrap) as runnable, in deterministic
// id order. Duplicates and already-known ids are ignored.
func (s *Scheduler) Seed(ids []NodeID) {
	ordered := slices.Clone(ids)
	slices.SortFunc(ordered, func(a, b NodeID) int { return a.Compare(b) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ordered {
		if _, ok := s.nodes[id]; ok {
			continue
		}
		s.nodes[id] = &node{id: id, state: stateRunnable}
		s.runq = append(s.runq, id)
	}
}

// Run drives the scheduler to a fixpoint, returning when every node is
// terminal (or ctx is canceled) after the in-flight bodies drain.
func (s *Scheduler) Run(ctx context.Context) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.canceled = true
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-stop:
		}
	}()

	s.mu.Lock()
	for !s.canceled {
		// 1. Dispatch the runnable frontier onto the bounded pool.
		for len(s.runq) > 0 && !s.canceled {
			id := s.runq[0]
			s.runq = s.runq[1:]
			n := s.nodes[id]
			if n == nil || n.state != stateRunnable {
				continue
			}
			n.state = stateRunning
			n.rerunRequested = false
			n.blockedOn = nil
			s.inFlight++
			level := s.draining
			s.mu.Unlock()
			s.tasks.Go(ctx, "schedule/"+id.String(), func(ctx context.Context) {
				out, blocked := s.disp.Dispatch(ctx, id, level)
				rerun := s.rerunAtDrain != nil && s.rerunAtDrain(id)
				s.complete(id, out, blocked, rerun)
			})
			s.mu.Lock()
		}
		// 2. Frontier empty. If nothing is in flight, we are at a fixpoint:
		//    either done, or the remaining parked nodes are unproducible and
		//    must be drained.
		if s.inFlight == 0 {
			if !s.hasParkedLocked() {
				// Quiescent: nothing running, nothing parked. If the store grew
				// since the last sweep, re-run the rerun nodes (a selector
				// ResourceSet re-expands against the now-complete store) and loop;
				// the dirty bit, cleared here and re-set only by a fresh arrival,
				// bounds the sweeps.
				if s.dirty {
					s.dirty = false
					if s.requeueRerunLocked() {
						continue
					}
				}
				break // clean fixpoint: every node terminal
			}
			// Parked nodes remain with nothing in flight. Escalate the drain
			// level and re-queue all parked once. WITHIN a level the cascade
			// propagates via complete() wakes — inFlight never settles to 0
			// mid-cascade, because a terminalize that wakes a waiter pushes it
			// to runq before the loop re-checks inFlight — so reaching here
			// again means the level made no progress: a cross-kind structural
			// cycle (same-kind cycles fail earlier at preflight). DrainForce
			// fails present-Pending deps, breaking the cycle the way the event
			// engine's quiescence does; finalSweep is the last-resort backstop.
			if s.draining >= DrainForce {
				break
			}
			s.draining++
			s.requeueAllParkedLocked()
			continue
		}
		// 3. Work in flight, frontier empty: wait for a completion or arrival.
		s.cond.Wait()
	}
	if !s.canceled {
		s.finalSweepLocked()
	}
	s.mu.Unlock()
	s.tasks.BlockTillDone()
}

// complete records the result of one Dispatch. Runs on the worker goroutine;
// acquires mu. Touches ONLY scheduler state — never the store or the pool.
func (s *Scheduler) complete(id NodeID, out Outcome, blocked []NodeID, rerun bool) {
	s.mu.Lock()
	// Broadcast on EVERY path (terminalize, park, re-queue) so the Run loop
	// re-evaluates inFlight/runq after any transition — a terminalize that
	// drops inFlight to 0 with the loop in cond.Wait MUST wake it. Deferred
	// after Unlock so (LIFO) it runs first, still under mu.
	defer s.mu.Unlock()
	defer s.cond.Broadcast()
	n := s.nodes[id]
	s.inFlight--
	// Record the node's rerun intent, re-evaluated at each dispatch. The value
	// is stable per node — a selector-only ResourceSet's rerun status is a fixed
	// spec property — so each write sets the same value. Read by
	// requeueRerunLocked at the fixpoint.
	n.rerun = rerun

	// A wake landed while this body was running: honor it exactly once by
	// re-queuing, regardless of the outcome just reported. The re-run
	// re-reads the store and re-evaluates against the now-current state
	// (covers a dep that advanced mid-run, and the #102 parent-mutated-spec
	// re-emit). We do NOT mark it terminal, so a parker never sees a stale
	// terminal here.
	if n.rerunRequested {
		n.rerunRequested = false
		n.state = stateRunnable
		n.blockedOn = nil
		s.runq = append(s.runq, id)
		return
	}

	switch out {
	case OutcomeTerminal:
		n.state = stateTerminal
		n.blockedOn = nil
		s.wakeWaitersLocked(id)
	case OutcomeBlocked:
		// Lost-wakeup re-check using ONLY scheduler state (never the store —
		// reading the store under mu would invert lock order against an
		// emitting worker that wants mu via OnArrival). If ANY blocked dep is
		// already terminal in our node map it will deliver no further
		// terminalize-wake, so parking risks hanging until draining; re-run now
		// instead (the re-run re-classifies: a terminal-Ready dep is satisfied,
		// a terminal-Failed dep cascades).
		for _, dep := range blocked {
			if d := s.nodes[dep]; d != nil && d.state == stateTerminal {
				n.state = stateRunnable
				s.runq = append(s.runq, id)
				return
			}
		}
		// Park: every blocked dep is live (a non-terminal node that will
		// terminalize and wake us) or has no scheduler node (an absent dep,
		// woken by a future OnArrival or terminalized by the draining sweep).
		n.state = stateParked
		n.blockedOn = blocked
		for _, dep := range blocked {
			set := s.parkedIdx[dep]
			if set == nil {
				set = map[NodeID]struct{}{}
				s.parkedIdx[dep] = set
			}
			set[id] = struct{}{}
		}
	}
}

// wakeWaitersLocked re-queues every node parked on depID (because depID
// terminalized, arrived, or reached a terminal status). Caller holds mu.
func (s *Scheduler) wakeWaitersLocked(depID NodeID) {
	set := s.parkedIdx[depID]
	if len(set) == 0 {
		return
	}
	waiters := make([]NodeID, 0, len(set))
	for w := range set {
		waiters = append(waiters, w)
	}
	for _, w := range waiters {
		n := s.nodes[w]
		if n == nil {
			continue
		}
		switch n.state {
		case stateParked:
			s.unparkLocked(n)
		case stateRunning:
			n.rerunRequested = true
		}
	}
}

// unparkLocked moves a parked node to runnable and removes it from every
// parkedIdx set it was registered in. Caller holds mu.
func (s *Scheduler) unparkLocked(n *node) {
	for _, dep := range n.blockedOn {
		if set := s.parkedIdx[dep]; set != nil {
			delete(set, n.id)
			if len(set) == 0 {
				delete(s.parkedIdx, dep)
			}
		}
	}
	n.blockedOn = nil
	n.state = stateRunnable
	s.runq = append(s.runq, n.id)
}

// OnArrival is called from the store's EventObjectAdded subscription (which
// fires only when an object's content actually changed — including a Refire's
// status reset). schedulable reports whether id is a node the scheduler runs (a
// Kustomization/HelmRelease/source) versus pure data (a ConfigMap/Secret): a
// data arrival must WAKE nodes parked on it (a KS waiting on a substituteFrom
// CM) but is never registered as a runnable node. A content-changed arrival of
// a terminal node re-dispatches it (the re-run re-reads and is idempotent via
// fingerprint dedup), which is what restores a Refired producer/source.
func (s *Scheduler) OnArrival(id NodeID, schedulable bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// An arrival can change a rerun node's resolved input set; mark the store
	// dirty so the next quiescence sweep re-expands rerun nodes.
	s.dirty = true
	if n := s.nodes[id]; n == nil {
		if schedulable {
			// Render-discovered node: register and queue it.
			s.nodes[id] = &node{id: id, state: stateRunnable}
			s.runq = append(s.runq, id)
		}
		// Non-schedulable unknown id (ConfigMap/Secret): fall through to wake
		// nodes parked on it, but do not register it.
	} else {
		switch n.state {
		case stateTerminal:
			// Content changed (Refire reset, or a parent re-emitted a mutated
			// spec): re-run so the new content is reconciled.
			n.state = stateRunnable
			s.runq = append(s.runq, id)
		case stateRunning:
			n.rerunRequested = true
		}
	}
	// Always wake nodes parked ON id — a node parked on its own emitted child
	// (HR -> synthetic HelmChart), or on a dep (CM/source/KS) that just arrived.
	s.wakeWaitersLocked(id)
	s.cond.Broadcast()
}

// OnStatusWake is called from the store's EventStatusUpdated subscription. It
// acts only on a TERMINAL store status (Ready or Failed): a parked node gates
// on Ready/Failed, so intermediate Pending progress writes never change its
// gate answer and waking on them is pure churn that also widens race windows.
func (s *Scheduler) OnStatusWake(id NodeID, ready, failed bool) {
	if !ready && !failed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wakeWaitersLocked(id)
	s.cond.Broadcast()
}

// unparkSelfLocked removes n from every parkedIdx set without queuing it (used
// when forcibly terminalizing a parked node). Caller holds mu.
func (s *Scheduler) unparkSelfLocked(n *node) {
	for _, dep := range n.blockedOn {
		if set := s.parkedIdx[dep]; set != nil {
			delete(set, n.id)
			if len(set) == 0 {
				delete(s.parkedIdx, dep)
			}
		}
	}
	n.blockedOn = nil
}

func (s *Scheduler) hasParkedLocked() bool {
	for _, n := range s.nodes {
		if n.state == stateParked {
			return true
		}
	}
	return false
}

// requeueAllParkedLocked moves every parked node back to runnable, in id
// order, so the draining sweep re-runs them leaf-first. Caller holds mu.
func (s *Scheduler) requeueAllParkedLocked() {
	var parked []*node
	for _, n := range s.nodes {
		if n.state == stateParked {
			parked = append(parked, n)
		}
	}
	slices.SortFunc(parked, func(a, b *node) int { return a.id.Compare(b.id) })
	for _, n := range parked {
		s.unparkLocked(n)
	}
}

// requeueRerunLocked re-queues every terminal rerun node, in id order, so each
// re-expands against the now-complete store at quiescence. Returns whether any
// node was re-queued. The caller gates this on (and clears) the dirty bit, so a
// sweep whose re-renders all dedup-no-op produces no new arrival, leaves dirty
// clear, and the run terminates. Caller holds mu.
func (s *Scheduler) requeueRerunLocked() bool {
	var due []*node
	for _, n := range s.nodes {
		if n.state == stateTerminal && n.rerun {
			due = append(due, n)
		}
	}
	if len(due) == 0 {
		return false
	}
	slices.SortFunc(due, func(a, b *node) int { return a.id.Compare(b.id) })
	for _, n := range due {
		n.state = stateRunnable
		n.blockedOn = nil
		s.runq = append(s.runq, n.id)
	}
	return true
}

// finalSweepLocked is a defensive backstop: after DrainForce every parked node
// should have terminalized (DrainForce fails present-Pending deps), so nothing
// should remain parked. If a node somehow does, force it terminal so a parked
// node can never masquerade as a clean run. Caller holds mu.
func (s *Scheduler) finalSweepLocked() {
	for _, n := range s.nodes {
		if n.state == stateParked {
			n.state = stateTerminal
			s.unparkSelfLocked(n)
		}
	}
}
