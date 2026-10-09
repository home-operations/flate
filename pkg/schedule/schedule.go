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
	"fmt"
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
	// returned ids and re-runs it when any advances. Blocked bodies MUST gate
	// before publishing: emit-before-block feedback cannot be distinguished
	// from unchanged dependency retries and is outside the convergence guard.
	OutcomeBlocked
	// OutcomeDependencyFailed is terminal for this attempt, with dependency
	// identities retained so actual Ready progress can reconsider the failure.
	OutcomeDependencyFailed
	// OutcomeTerminalNoop is terminal with no object arrivals from this body.
	// Selector clients may use it for verified dedup no-ops; ambiguous results
	// MUST remain OutcomeTerminal for conservative replay accounting.
	OutcomeTerminalNoop
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
// The returned blocked slice MUST NOT be mutated by the dispatcher after return;
// the scheduler retains it until the node's next completion with a different
// blocker set.
type Dispatcher interface {
	// Dispatch invokes id's reconcile body synchronously on the calling
	// goroutine (a task.Service worker) and reports back:
	//   - out: OutcomeTerminal, OutcomeTerminalNoop, OutcomeBlocked, or OutcomeDependencyFailed.
	//   - blocked: unsatisfied or failed dependency ids for the latter two outcomes.
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

// Existing scheduler fixtures need at most three dispatches per node under
// -race. Allow 32 content-driven redispatches, excluding dependency retries,
// to leave ample room for healthy propagation while bounding feedback loops.
const maxRedispatches = 32

type failedMark struct {
	check uint64
	next  *failedMark
}

type node struct {
	id           NodeID
	state        nodeState
	blockedOn    []NodeID // deps recorded at the last OutcomeBlocked
	redispatches int
	failedOn     []NodeID // unique dependencies; borrowed dispatcher slices MUST stay immutable
	failedSeen   map[NodeID]*failedMark
	failedFree   *failedMark
	failedCheck  uint64
	startedAt    uint64
	readyAt      uint64
	// Unrelated progress permits only one free retry per unchanged blocker set.
	conservativeUsed bool
	// rerunRequested is set when a wake arrives while the node is running, so
	// complete() re-queues it once instead of dropping the wake (the re-run
	// re-reads the store and re-evaluates its gate against current state).
	rerunRequested bool
	// Content, dependency recovery and selector productivity coalesce into one charge
	// for the next execution, including a free runnable execution upgraded by
	// a later arrival. queuedCharged is consumed when dispatch starts.
	contentRequested bool
	productive       bool
	queuedCharged    bool
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
	failedIdx map[NodeID]map[NodeID]struct{} // dep id -> terminal dependency failures
	// One overwriteable witness preserves unknown-ID registration evidence.
	untracked   NodeID
	untrackedAt uint64
	generation  uint64
	inFlight    int // count of stateRunning nodes (EXCLUDES parked)
	draining    int // DrainNone/DrainCascade/DrainForce
	err         error
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
// Ordinary terminal results conservatively charge the next selector execution;
// only OutcomeTerminalNoop allows free dedup sweeps. Blocked retries gate before
// publication and contribute no productivity charge.
func (s *Scheduler) SetRerunAtDrain(fn func(NodeID) bool) { s.rerunAtDrain = fn }

// New constructs a Scheduler that runs bodies on tasks via disp.
func New(tasks *task.Service, disp Dispatcher) *Scheduler {
	s := &Scheduler{
		tasks:     tasks,
		disp:      disp,
		nodes:     map[NodeID]*node{},
		parkedIdx: map[NodeID]map[NodeID]struct{}{},
		failedIdx: map[NodeID]map[NodeID]struct{}{},
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
// terminal, ctx is canceled, or a node exceeds its redispatch budget.
// In-flight bodies drain before returning.
func (s *Scheduler) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			if s.err == nil {
				s.err = ctx.Err()
			}
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-stop:
		}
	}()

	s.mu.Lock()
	for s.err == nil {
		// 1. Dispatch the runnable frontier onto the bounded pool.
		for len(s.runq) > 0 && s.err == nil {
			id := s.runq[0]
			s.runq = s.runq[1:]
			n := s.nodes[id]
			if n == nil || n.state != stateRunnable {
				continue
			}
			n.state = stateRunning
			n.startedAt = s.generation
			n.queuedCharged = false
			n.blockedOn = nil
			s.inFlight++
			level := s.draining
			s.mu.Unlock()
			s.tasks.Go(runCtx, "schedule/"+id.String(), func(ctx context.Context) {
				out, blocked := s.disp.Dispatch(ctx, id, level)
				rerun := s.rerunAtDrain != nil && s.rerunAtDrain(id)
				s.complete(id, out, blocked, rerun)
			})
			s.mu.Lock()
		}
		if s.err != nil {
			break
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
	if s.err == nil {
		s.err = ctx.Err()
	}
	if s.err == nil {
		s.finalSweepLocked()
	}
	s.mu.Unlock()
	cancel()
	s.tasks.BlockTillDone()
	s.mu.Lock()
	if s.err == nil {
		s.err = ctx.Err()
	}
	err := s.err
	s.mu.Unlock()
	return err
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
	sameFailed := out == OutcomeDependencyFailed && s.sameFailedLocked(n, blocked)
	if !sameFailed {
		n.conservativeUsed = false
		if out != OutcomeDependencyFailed {
			s.clearFailedLocked(n)
		}
	}
	if s.err != nil {
		s.clearFailedLocked(n)
		n.state = stateTerminal
		n.conservativeUsed = false
		return
	}
	// Record the node's rerun intent, re-evaluated at each dispatch. The value
	// is stable per node — a selector-only ResourceSet's rerun status is a fixed
	// spec property — so each write sets the same value. Read by
	// requeueRerunLocked at the fixpoint.
	n.rerun = rerun
	if out == OutcomeDependencyFailed {
		if !sameFailed {
			previous := n.failedOn
			n.failedOn = blocked
			if len(previous) <= 2 && len(blocked) <= 2 {
				if len(blocked) > 1 && blocked[0] == blocked[1] {
					n.failedOn = slices.Clone(blocked[:1])
				}
				for _, dep := range n.failedOn {
					if len(previous) > 0 && (dep == previous[0] || len(previous) == 2 && dep == previous[1]) {
						continue
					}
					set := s.failedIdx[dep]
					if set == nil {
						set = map[NodeID]struct{}{}
						s.failedIdx[dep] = set
					}
					set[id] = struct{}{}
				}
				for _, dep := range previous {
					if len(n.failedOn) > 0 && (dep == n.failedOn[0] || len(n.failedOn) == 2 && dep == n.failedOn[1]) {
						continue
					}
					set := s.failedIdx[dep]
					delete(set, id)
					if len(set) == 0 {
						delete(s.failedIdx, dep)
					}
				}
				n.failedSeen, n.failedFree = nil, nil
			} else {
				if n.failedSeen == nil {
					n.failedSeen = make(map[NodeID]*failedMark, max(len(previous), len(blocked)))
					marks := make([]failedMark, max(len(previous), len(blocked))+1)
					for i := range marks {
						marks[i].next = n.failedFree
						n.failedFree = &marks[i]
					}
					for _, dep := range previous {
						n.failedSeen[dep] = n.failedFree
						n.failedFree = n.failedFree.next
					}
				}
				n.failedCheck++
				duplicate := false
				for i, dep := range blocked {
					mark := n.failedSeen[dep]
					if mark != nil && mark.check == n.failedCheck {
						if !duplicate {
							// Dispatcher slices may be shared; only a private copy is compacted.
							n.failedOn = make([]NodeID, i, len(blocked))
							copy(n.failedOn, blocked[:i])
							duplicate = true
						}
						continue
					}
					if mark == nil {
						if n.failedFree == nil {
							mark = &failedMark{}
						} else {
							mark = n.failedFree
							n.failedFree = mark.next
						}
						n.failedSeen[dep] = mark
						set := s.failedIdx[dep]
						if set == nil {
							set = map[NodeID]struct{}{}
							s.failedIdx[dep] = set
						}
						set[id] = struct{}{}
					}
					mark.check = n.failedCheck
					if duplicate {
						n.failedOn = append(n.failedOn, dep)
					}
				}
				for _, dep := range previous {
					mark := n.failedSeen[dep]
					if mark.check != n.failedCheck {
						set := s.failedIdx[dep]
						delete(set, id)
						if len(set) == 0 {
							delete(s.failedIdx, dep)
						}
						delete(n.failedSeen, dep)
						mark.next = n.failedFree
						n.failedFree = mark
					}
				}
			}
		}
		// Only progress after dispatch start can race failed-edge registration.
		if s.generation > n.startedAt {
			for _, dep := range blocked {
				d := s.nodes[dep]
				if (d != nil && d.readyAt > n.startedAt) || (dep == s.untracked && s.untrackedAt > n.startedAt) {
					n.productive = true
					n.rerunRequested = true
					break
				}
			}
		}
		if !n.rerunRequested && !n.conservativeUsed && s.generation > n.startedAt {
			n.conservativeUsed = true
			n.rerunRequested = true
		}
	}
	n.productive = n.productive || rerun && out == OutcomeTerminal

	// A wake landed while this body was running: honor it exactly once by
	// re-queuing, regardless of the outcome just reported. The re-run
	// re-reads the store and re-evaluates against the now-current state
	// (covers a dep that advanced mid-run, and the #102 parent-mutated-spec
	// re-emit). We do NOT mark it terminal, so a parker never sees a stale
	// terminal here.
	if n.rerunRequested {
		s.redispatchLocked(n)
		return
	}

	switch out {
	case OutcomeTerminal, OutcomeTerminalNoop, OutcomeDependencyFailed:
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
				s.redispatchLocked(n)
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
	s.unparkSelfLocked(n)
	s.redispatchLocked(n)
}

func (s *Scheduler) sameFailedLocked(n *node, blocked []NodeID) bool {
	if len(blocked) < len(n.failedOn) {
		return false
	}
	if len(blocked) == len(n.failedOn) {
		equal := true
		for i, dep := range blocked {
			if dep != n.failedOn[i] {
				// A replacement can follow a long unchanged prefix; check its edge first.
				if _, ok := s.failedIdx[dep][n.id]; !ok {
					return false
				}
				equal = false
				break
			}
		}
		if equal {
			return true
		}
	}
	count := 0
	n.failedCheck++
	var seen uint8
	for _, dep := range blocked {
		if _, ok := s.failedIdx[dep][n.id]; !ok {
			return false
		}
		if len(n.failedOn) <= 2 {
			bit := uint8(1)
			if dep != n.failedOn[0] {
				bit = 2
			}
			if seen&bit == 0 {
				seen |= bit
				count++
			}
		} else {
			// Only registered marks are updated, so duplicate checks never allocate.
			mark := n.failedSeen[dep]
			if mark.check != n.failedCheck {
				mark.check = n.failedCheck
				count++
			}
		}
	}
	return count == len(n.failedOn)
}

func (s *Scheduler) clearFailedLocked(n *node) {
	for _, dep := range n.failedOn {
		if set := s.failedIdx[dep]; set != nil {
			delete(set, n.id)
			if len(set) == 0 {
				delete(s.failedIdx, dep)
			}
		}
	}
	n.failedOn = nil
	n.failedSeen, n.failedFree = nil, nil
}

// Callers bypass idle events to keep the non-inlineable waiter wake path off arrivals.
func (s *Scheduler) recordProgressLocked(id NodeID) {
	if s.inFlight == 0 && (len(s.failedIdx) == 0 || len(s.failedIdx[id]) == 0) {
		return
	}
	s.generation++
	if n := s.nodes[id]; n != nil {
		n.readyAt = s.generation
	} else {
		s.untracked = id
		s.untrackedAt = s.generation
	}
	set := s.failedIdx[id]
	if len(set) == 0 {
		return
	}
	waiters := make([]NodeID, 0, len(set))
	for waiter := range set {
		waiters = append(waiters, waiter)
	}
	slices.SortFunc(waiters, func(a, b NodeID) int { return a.Compare(b) })
	for _, waiter := range waiters {
		n := s.nodes[waiter]
		switch n.state {
		case stateTerminal, stateRunnable:
			n.productive = true
			if !s.redispatchLocked(n) {
				return
			}
		case stateRunning:
			n.productive = true
			n.rerunRequested = true
		}
	}
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
	if s.err != nil {
		return
	}
	// An arrival can change a rerun node's resolved input set; mark the store
	// dirty so the next quiescence sweep re-expands rerun nodes.
	s.dirty = true
	if !schedulable && (s.inFlight > 0 || len(s.failedIdx) > 0) {
		s.recordProgressLocked(id)
	}
	if n := s.nodes[id]; n == nil {
		if schedulable {
			// Render-discovered node: register and queue it.
			s.nodes[id] = &node{id: id, state: stateRunnable}
			s.runq = append(s.runq, id)
		}
		// Non-schedulable unknown id (ConfigMap/Secret): fall through to wake
		// nodes parked on it, but do not register it.
	} else {
		n.contentRequested = true
		if n.state == stateRunning {
			n.rerunRequested = true
		} else {
			if n.state == stateParked {
				s.unparkSelfLocked(n)
			}
			s.redispatchLocked(n)
		}
	}
	// Always wake nodes parked ON id — a node parked on its own emitted child
	// (HR -> synthetic HelmChart), or on a dep (CM/source/KS) that just arrived.
	s.wakeWaitersLocked(id)
	s.cond.Broadcast()
}

// OnStatusWake wakes parked nodes on terminal status updates. Store condition
// writes suppress identical values, so every Ready wake records fresh progress.
func (s *Scheduler) OnStatusWake(id NodeID, ready, failed bool) {
	if !ready && !failed {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return
	}
	if ready && (s.inFlight > 0 || len(s.failedIdx) > 0) {
		s.recordProgressLocked(id)
	}
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
		if !s.redispatchLocked(n) {
			break
		}
	}
	return true
}

func (s *Scheduler) redispatchLocked(n *node) bool {
	if s.err != nil {
		return false
	}
	if (n.contentRequested || n.productive) && !n.queuedCharged {
		if n.redispatches >= maxRedispatches {
			s.err = fmt.Errorf("schedule: %s exceeded %d redispatches; reconcile did not converge", n.id, maxRedispatches)
			return false
		}
		n.redispatches++
		n.queuedCharged = true
	}
	if n.productive {
		n.conservativeUsed = false
	}
	n.contentRequested = false
	n.productive = false
	n.rerunRequested = false
	if n.state != stateRunnable {
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
