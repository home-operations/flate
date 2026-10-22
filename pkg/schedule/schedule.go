// Package schedule drives bounded reconciliation to a structural input fixpoint.
package schedule

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// NodeID identifies a schedulable declaration.
type NodeID = manifest.NamedResource

// Kind is the retained dependency classification of an invocation.
type Kind uint8

const (
	// Terminal means the invocation has no unresolved dependency.
	Terminal Kind = iota
	// Blocked means an input may still become available.
	Blocked
	// DependencyFailed means a required input failed.
	DependencyFailed
)

type assessment interface {
	Valid() bool
	Outcome() Kind
	Progressed() bool
	Settle(context.Context, manifest.DrainLevel)
	Retire()
	Close()
}

// Result retains the inputs and outcome of one execution.
type Result struct {
	Assessment assessment
}

// Dispatcher connects execution and publication delivery to the scheduler.
// SDK embedders implement this seam without exposing their input representation.
type Dispatcher interface {
	Dispatch(context.Context, NodeID, manifest.DrainLevel) Result
	// Sweep permits final classification only when the invocation's work is quiescent.
	Sweep(context.Context, bool) []NodeID
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
)

const maxRedispatches = 32

type node struct {
	id           NodeID
	state        nodeState
	assessment   assessment
	redispatches int
	progressed   bool
	recheck      bool
}

// Scheduler owns execution lifetime and sorted successor admission.
type Scheduler struct {
	tasks     taskService
	disp      Dispatcher
	mu        sync.Mutex
	cond      *sync.Cond
	nodes     map[NodeID]*node
	runq      []NodeID
	unsorted  bool
	inFlight  int
	err       error
	stopped   bool
	published bool
}

// New constructs a scheduler borrowing the invocation's bounded task service.
func New(tasks taskService, disp Dispatcher) *Scheduler {
	s := &Scheduler{tasks: tasks, disp: disp, nodes: make(map[NodeID]*node)}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Seed admits initial declarations in deterministic order.
func (s *Scheduler) Seed(ids []NodeID) {
	s.mu.Lock()
	for _, id := range ids {
		if s.nodes[id] == nil {
			s.nodes[id] = &node{id: id, state: stateRunnable}
			s.runq = append(s.runq, id)
			s.unsorted = true
		}
	}
	s.mu.Unlock()
}

// OnArrival admits a new runnable declaration through the existing event adapter.
// Existing executions observe changes through their retained input registrations.
func (s *Scheduler) OnArrival(id NodeID, runnable bool) {
	s.mu.Lock()
	s.published = true
	if runnable && s.err == nil && !s.stopped && s.nodes[id] == nil {
		s.nodes[id] = &node{id: id, state: stateRunnable}
		s.runq = append(s.runq, id)
		s.unsorted = true
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Scheduler) requeueLocked(n *node, charge bool) {
	if n == nil || n.state == stateRunnable || s.err != nil {
		return
	}
	if charge && n.progressed {
		if n.redispatches >= maxRedispatches {
			s.err = fmt.Errorf("%w: schedule: %s exceeded %d redispatches; reconcile did not converge", manifest.ErrFlux, n.id, maxRedispatches)
			return
		}
		n.redispatches++
	}
	n.state = stateRunnable
	s.runq = append(s.runq, n.id)
	s.unsorted = true
}

// OnPublication signals the structural boundary through existing publication events.
func (s *Scheduler) OnPublication() {
	s.mu.Lock()
	s.published = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Scheduler) complete(n *node, result Result, replace bool) {
	previous := n.assessment
	if replace && previous != nil {
		previous.Retire()
	}
	current := result.Assessment
	valid, unresolved, progressed := true, false, true
	if current != nil {
		valid, unresolved, progressed = current.Valid(), current.Outcome() == Blocked, current.Progressed()
	}
	s.mu.Lock()
	for n.recheck && valid && current != nil {
		n.recheck = false
		s.mu.Unlock()
		valid = current.Valid()
		s.mu.Lock()
	}
	n.recheck = false
	n.assessment = current
	n.progressed = progressed
	s.inFlight--
	n.state = stateTerminal
	if unresolved {
		n.state = stateParked
	}
	if !valid {
		s.requeueLocked(n, !unresolved)
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Scheduler) invalidate(ids []NodeID) {
	for _, id := range ids {
		s.mu.Lock()
		n := s.nodes[id]
		var current assessment
		if n != nil {
			if n.state == stateRunning {
				n.recheck = true
			} else {
				current = n.assessment
			}
		}
		s.mu.Unlock()
		if current == nil || current.Valid() {
			continue
		}
		s.mu.Lock()
		s.requeueLocked(n, n.state != stateParked)
		s.mu.Unlock()
	}
}

// Run drains work, delivers pending heads and settles retained unresolved assessments.
// Dispatcher, assessment and task-service calls MUST run outside mu.
func (s *Scheduler) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		if s.err == nil {
			s.err = ctx.Err()
		}
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	level := manifest.DrainNone
	s.mu.Lock()
	if s.err == nil {
		s.err = ctx.Err()
	}
	for s.err == nil {
		if len(s.runq) > 0 {
			if s.unsorted {
				slices.SortFunc(s.runq, NodeID.Compare)
				s.unsorted = false
			}
			id := s.runq[0]
			s.runq = s.runq[1:]
			n := s.nodes[id]
			n.state = stateRunning
			s.inFlight++
			level = manifest.DrainNone
			s.mu.Unlock()
			s.tasks.Go(runCtx, "schedule/"+id.String(), func(ctx context.Context) {
				var result Result
				defer func() { s.complete(n, result, true) }()
				result = s.disp.Dispatch(ctx, id, manifest.DrainNone)
			})
			s.mu.Lock()
			continue
		}
		if s.inFlight != 0 {
			if !s.published {
				s.cond.Wait()
				continue
			}
			s.published = false
			s.mu.Unlock()
			s.invalidate(s.disp.Sweep(runCtx, false))
			s.mu.Lock()
			continue
		}
		s.published = false
		s.mu.Unlock()
		s.invalidate(s.disp.Sweep(runCtx, true))
		s.mu.Lock()
		if len(s.runq) > 0 || s.err != nil {
			continue
		}
		var parked []*node
		for _, n := range s.nodes {
			if n.state == stateParked {
				parked = append(parked, n)
			}
		}
		if len(parked) == 0 {
			break
		}
		if level == manifest.DrainForce {
			s.err = fmt.Errorf("%w: schedule: unresolved assessment after structural classification", manifest.ErrFlux)
			break
		}
		level++
		slices.SortFunc(parked, func(a, b *node) int { return a.id.Compare(b.id) })
		for _, n := range parked {
			if s.err != nil {
				break
			}
			n.state = stateRunning
			s.inFlight++
			current, drain := n.assessment, level
			s.mu.Unlock()
			s.tasks.Go(runCtx, "schedule/settle/"+n.id.String(), func(ctx context.Context) {
				defer func() { s.complete(n, Result{Assessment: current}, false) }()
				if current.Valid() {
					current.Settle(ctx, drain)
				}
			})
			s.mu.Lock()
		}
	}
	s.stopped = true
	s.mu.Unlock()
	cancel()
	s.tasks.BlockTillDone()
	for _, n := range s.nodes {
		if n.assessment != nil {
			n.assessment.Close()
		}
	}
	s.disp.Sweep(runCtx, true)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = ctx.Err()
	}
	return s.err
}

// Results projects completed accepted assessments after Run has drained.
func (s *Scheduler) Results() map[NodeID]Result {
	results := make(map[NodeID]Result, len(s.nodes))
	for id, n := range s.nodes {
		if n.state == stateTerminal && n.assessment != nil && n.assessment.Valid() {
			results[id] = Result{Assessment: n.assessment}
		}
	}
	return results
}
