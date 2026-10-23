// Package schedule provides flate's dependency-driven reconcile scheduler:
// a re-entrant fixpoint engine that runs each node's reconcile body on a
// bounded task pool, parks a body that reports unsatisfied dependencies
// (Blocked), and re-runs it when any of those dependencies
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
// The package depends only on pkg/manifest; all store and
// controller interaction is behind the Dispatcher seam, so the scheduler is
// unit-testable against a fake Dispatcher with no store or controllers.
package schedule

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/pkg/manifest"
)

// NodeID identifies a schedulable node — a Kustomization, HelmRelease, or
// source CR — by its store identity.
type NodeID = manifest.NamedResource

// Kind is the final disposition of an invocation or ordinary dependency.
type Kind uint8

const (
	// Terminal is satisfied or finished without dependency recovery edges.
	Terminal Kind = iota
	// Blocked retains dependencies that may advance.
	Blocked
	// DependencyFailed retains required causes for recovery.
	DependencyFailed
)

// Outcome carries one final disposition and owner-produced scheduling facts.
type Outcome struct {
	Kind  Kind
	Noop  bool
	Rerun bool
}

// Dispatcher returns the owner's final classification with immutable borrowed waits.
type Dispatcher interface {
	Dispatch(ctx context.Context, id NodeID, attempt manifest.ReconcileAttempt) (Outcome, manifest.ReconcileWait)
}

type taskService interface {
	Go(context.Context, string, func(context.Context))
	BlockTillDone()
}

type nodeState uint8

const (
	stateRunnable nodeState = iota
	stateRunning
	stateParked
	stateTerminal
	stateUnscheduled
)

// Bound content-driven feedback per node while leaving dependency retries free
// to propagate through arbitrarily long dependency chains.
const maxRedispatches = 32

type edgeIdx map[NodeID]map[NodeID]struct{}

func (e edgeIdx) add(dep, id NodeID) {
	set := e[dep]
	if set == nil {
		set = map[NodeID]struct{}{}
		e[dep] = set
	}
	set[id] = struct{}{}
}

func (e edgeIdx) del(dep, id NodeID) {
	set := e[dep]
	delete(set, id)
	if len(set) == 0 {
		delete(e, dep)
	}
}

type failedMark struct {
	check uint64
	next  *failedMark
}

type node struct {
	opportunity  *atomic.Uint64
	content      *manifest.ContentWait
	drain        manifest.DrainLevel
	id           NodeID
	state        nodeState
	blockedOn    []NodeID // deps recorded at the last blocked completion
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
	// Dispatcher after every reconcile body.
	rerun bool
}

// Scheduler is a re-entrant fixpoint reconcile driver. Construct with New,
// Seed the initial node set, wire store events to OnArrival/OnStatusWake,
// then call Run.
type Scheduler struct {
	tasks taskService
	disp  Dispatcher

	mu    sync.Mutex
	cond  *sync.Cond
	nodes map[NodeID]*node
	runq  []NodeID
	// Wake snapshots are shared only while mu is held.
	waiters   []NodeID
	parkedIdx edgeIdx // dep id -> set of nodes parked on it
	failedIdx edgeIdx // dep id -> terminal dependency failures
	// One overwriteable witness preserves unknown-ID registration evidence.
	untracked   NodeID
	untrackedAt uint64
	generation  uint64
	inFlight    int                 // count of stateRunning nodes (EXCLUDES parked)
	draining    manifest.DrainLevel // manifest.DrainNone/manifest.DrainCascade/manifest.DrainForce
	err         error
	// dirty records that an object arrived since the last quiescence sweep. A
	// rerun node re-expands at the structural fixpoint only when the store has
	// grown since it last ran; the sweep clears dirty, so a sweep that produces
	// nothing new (every re-render a dedup no-op) leaves it clear and the run
	// terminates. Arrivals are finite and monotone, so sweeps are bounded.
	dirty bool
}

// New constructs a Scheduler that runs bodies on tasks via disp.
func New(tasks taskService, disp Dispatcher) *Scheduler {
	s := &Scheduler{
		tasks:     tasks,
		disp:      disp,
		nodes:     map[NodeID]*node{},
		parkedIdx: edgeIdx{},
		failedIdx: edgeIdx{},
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
		n := s.nodes[id]
		if n != nil && n.state != stateUnscheduled {
			continue
		}
		if n == nil {
			n = &node{id: id}
			s.nodes[id] = n
		}
		n.state = stateRunnable
		n.open()
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
		s.submitFrontier(runCtx)
		if s.err != nil {
			break
		}
		if s.inFlight == 0 {
			if !s.advanceLocked() {
				break
			}
			continue
		}
		s.cond.Wait()
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

// Caller holds mu; task submission releases it before invoking the pool.
func (s *Scheduler) submitFrontier(ctx context.Context) {
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
		attempt := manifest.ReconcileAttempt{Drain: s.draining, Content: n.content}
		n.drain = attempt.Drain
		s.mu.Unlock()
		s.tasks.Go(ctx, "schedule/"+id.String(), func(ctx context.Context) {
			var out Outcome
			var wait manifest.ReconcileWait
			defer func() {
				s.complete(n, out, &wait)
			}()
			out, wait = s.disp.Dispatch(ctx, n.id, attempt)
		})
		s.mu.Lock()
	}
}

// Caller holds mu with an empty runnable frontier and no in-flight work.
func (s *Scheduler) advanceLocked() bool {
	if !s.hasParkedLocked() {
		if !s.dirty {
			return false
		}
		s.dirty = false
		return s.requeueRerunLocked()
	}
	if s.draining < manifest.DrainForce {
		s.draining++
	}
	return s.requeueAllParkedLocked()
}

// complete records the result of one Dispatch. Runs on the worker goroutine;
// acquires mu. Touches ONLY scheduler state — never the store or the pool.
func (s *Scheduler) complete(n *node, out Outcome, wait *manifest.ReconcileWait) {
	s.mu.Lock()
	// Broadcast on EVERY path (terminalize, park, re-queue) so the Run loop
	// re-evaluates inFlight/runq after any transition — a terminalize that
	// drops inFlight to 0 with the loop in cond.Wait MUST wake it. Deferred
	// after Unlock so (LIFO) it runs first, still under mu.
	defer s.mu.Unlock()
	defer s.cond.Broadcast()
	id := n.id
	s.inFlight--
	sameFailed := out.Kind == DependencyFailed && s.sameFailedLocked(n, wait.Deps)
	if !sameFailed {
		n.conservativeUsed = false
		if out.Kind != DependencyFailed {
			s.clearFailedLocked(n)
		}
	}
	if s.err != nil {
		s.clearFailedLocked(n)
		n.state = stateTerminal
		n.exhaust()
		n.conservativeUsed = false
		return
	}
	n.rerun = out.Rerun
	if out.Kind == DependencyFailed {
		if !sameFailed {
			previous := n.failedOn
			n.failedOn = wait.Deps
			if len(previous) <= 2 && len(wait.Deps) <= 2 {
				if len(wait.Deps) > 1 && wait.Deps[0] == wait.Deps[1] {
					n.failedOn = slices.Clone(wait.Deps[:1])
				}
				for _, dep := range n.failedOn {
					if len(previous) > 0 && (dep == previous[0] || len(previous) == 2 && dep == previous[1]) {
						continue
					}
					s.failedIdx.add(dep, id)
				}
				for _, dep := range previous {
					if len(n.failedOn) > 0 && (dep == n.failedOn[0] || len(n.failedOn) == 2 && dep == n.failedOn[1]) {
						continue
					}
					s.failedIdx.del(dep, id)
				}
				n.failedSeen, n.failedFree = nil, nil
			} else {
				if n.failedSeen == nil {
					n.failedSeen = make(map[NodeID]*failedMark, max(len(previous), len(wait.Deps)))
					marks := make([]failedMark, max(len(previous), len(wait.Deps))+1)
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
				for i, dep := range wait.Deps {
					mark := n.failedSeen[dep]
					if mark != nil && mark.check == n.failedCheck {
						if !duplicate {
							// Dispatcher slices may be shared; only a private copy is compacted.
							n.failedOn = make([]NodeID, i, len(wait.Deps))
							copy(n.failedOn, wait.Deps[:i])
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
						s.failedIdx.add(dep, id)
					}
					mark.check = n.failedCheck
					if duplicate {
						n.failedOn = append(n.failedOn, dep)
					}
				}
				for _, dep := range previous {
					mark := n.failedSeen[dep]
					if mark.check != n.failedCheck {
						s.failedIdx.del(dep, id)
						delete(n.failedSeen, dep)
						mark.next = n.failedFree
						n.failedFree = mark
					}
				}
			}
		}
		// Only progress after dispatch start can race failed-edge registration.
		if s.generation > n.startedAt {
			for _, dep := range wait.Deps {
				d := s.nodes[dep]
				if (d != nil && d.readyAt > n.startedAt) || (dep == s.untracked && s.untrackedAt > n.startedAt) {
					n.productive = true
					n.open()
					n.rerunRequested = true
					break
				}
			}
		}
		if !n.rerunRequested && !n.conservativeUsed && s.generation > n.startedAt {
			n.conservativeUsed = true
			n.open()
			n.rerunRequested = true
		}
	}
	n.productive = n.productive || out.Rerun && out.Kind == Terminal && !out.Noop

	// A running wake MUST be honored once before terminalization.
	if n.rerunRequested {
		s.redispatchLocked(n)
		return
	}

	switch out.Kind {
	case Terminal, DependencyFailed:
		n.state = stateTerminal
		n.exhaust()
		s.wakeWaitersLocked(id)
	case Blocked:
		s.parkLocked(n, wait)
	}
}

// Caller holds mu; binding and registration must share the same recheck boundary.
func (s *Scheduler) parkLocked(n *node, wait *manifest.ReconcileWait) {
	retry := false
	if wait.Content != nil {
		content := *wait.Content
		content.Suppliers = slices.Clone(content.Suppliers)
		for i := range content.Suppliers {
			o := &content.Suppliers[i]
			d := s.nodes[o.NamedResource]
			if d == nil {
				d = &node{id: o.NamedResource, state: stateUnscheduled}
				s.nodes[o.NamedResource] = d
			}
			if d.opportunity == nil {
				d.opportunity = new(atomic.Uint64)
				if d.state != stateTerminal && d.state != stateUnscheduled {
					d.open()
				}
			}
			word := d.opportunity.Load()
			retry = retry || o.Word == nil || o.Observed != word
			o.Word, o.Observed = d.opportunity, word
		}
		n.content = &content
	}
	for _, dep := range wait.Deps {
		if d := s.nodes[dep]; d != nil && d.state == stateTerminal {
			retry = true
		}
	}
	if retry {
		s.redispatchLocked(n)
		return
	}
	n.state = stateParked
	n.blockedOn = wait.Deps
	if wait.Content != nil {
		n.blockedOn = append(slices.Clone(wait.Deps), wait.Content.Targets...)
		for _, o := range n.content.Suppliers {
			if o.Observed&1 != 0 {
				n.blockedOn = append(n.blockedOn, o.NamedResource)
			}
		}
	}
	for _, dep := range n.blockedOn {
		s.parkedIdx.add(dep, n.id)
	}
}

// wakeWaitersLocked re-queues every node parked on depID (because depID
// terminalized, arrived, or reached a terminal status). Caller holds mu.
func (s *Scheduler) wakeWaitersLocked(depID NodeID) {
	set := s.parkedIdx[depID]
	if len(set) == 0 {
		return
	}
	waiters := s.sortedWaiters(set)
	for _, w := range waiters {
		n := s.nodes[w]
		if n == nil {
			continue
		}
		switch n.state {
		case stateParked:
			s.unparkLocked(n)
		case stateRunning:
			n.open()
			n.rerunRequested = true
		}
	}
}

// Caller holds mu; the next wake batch invalidates the snapshot.
func (s *Scheduler) sortedWaiters(set map[NodeID]struct{}) []NodeID {
	waiters := slices.Grow(s.waiters[:0], len(set))
	for waiter := range set {
		waiters = append(waiters, waiter)
	}
	if len(waiters) > 1 {
		slices.SortFunc(waiters, func(a, b NodeID) int { return a.Compare(b) })
	}
	s.waiters = waiters
	return waiters
}

// unparkLocked moves a parked node to runnable and removes it from every
// parkedIdx set it was registered in. Caller holds mu.
func (s *Scheduler) unparkLocked(n *node) {
	n.open()
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
		s.failedIdx.del(dep, n.id)
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
	waiters := s.sortedWaiters(set)
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
			n.open()
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
	} else if n.state != stateUnscheduled || schedulable {
		n.open()
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
// Reverse-edge deletion MUST stay inline here to bound the OnArrival stack frame.
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
func (s *Scheduler) requeueAllParkedLocked() bool {
	var parked []*node
	for _, n := range s.nodes {
		if n.state == stateParked {
			parked = append(parked, n)
		}
	}
	slices.SortFunc(parked, func(a, b *node) int { return a.id.Compare(b.id) })
	queued := false
	for _, n := range parked {
		grant := n.drain < s.draining
		if n.content != nil && len(n.content.Suppliers) > 0 && s.draining == manifest.DrainForce {
			for i := range n.content.Suppliers {
				o := &n.content.Suppliers[i]
				word := o.Word.Load()
				grant = grant || o.Cut != word
				o.Cut = word
			}
		}
		if grant {
			s.unparkSelfLocked(n)
			s.redispatchLocked(n)
			queued = true
		}
	}
	return queued
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
	if n.opportunity != nil && n.opportunity.Load()&1 == 0 {
		n.open()
	}
	if n.state != stateRunnable {
		n.state = stateRunnable
		n.blockedOn = nil
		s.runq = append(s.runq, n.id)
	}
	return true
}

// finalSweepLocked is a defensive backstop: after manifest.DrainForce every parked node
// should have terminalized (manifest.DrainForce fails present-Pending deps), so nothing
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

func (n *node) open() {
	if n.opportunity != nil {
		n.opportunity.Store((n.opportunity.Load() + 2) | 1)
	}
	if n.content != nil && n.content.Declaration != nil {
		n.content = &manifest.ContentWait{Targets: n.content.Targets, Suppliers: n.content.Suppliers}
	}
}

func (n *node) exhaust() {
	if n.opportunity != nil {
		n.opportunity.Store((n.opportunity.Load() + 2) &^ 1)
	}
	n.content = nil
}
