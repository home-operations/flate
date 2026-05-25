// Package task provides a lightweight goroutine lifecycle manager
// modeled on flux-local's TaskService. Active tasks are bounded units
// of work (a single reconciliation, a dependency wait) whose completion
// is tracked via BlockTillDone. A single Service should be associated
// with one orchestrator run.
package task

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Service tracks active goroutines.
type Service struct {
	wgActive sync.WaitGroup
	active   atomic.Int64

	// failures is incremented by goroutines that panic; non-zero implies
	// the run is poisoned.
	failures atomic.Int64

	// sem bounds the number of concurrent active task BODIES. The
	// goroutine is launched eagerly but blocks on the semaphore until
	// a worker slot is free, so callers never block on Go().
	//
	// nil = unbounded (every Go runs in parallel). Set via NewBounded.
	sem chan struct{}
}

// New constructs a fresh Service with unbounded concurrency.
func New() *Service { return &Service{} }

// NewBounded constructs a Service that caps the number of concurrently
// executing active-task bodies at workers. Submitting more does not
// block — the surplus goroutines exist but wait on an internal
// semaphore until a slot opens. workers <= 0 disables bounding
// (equivalent to New).
//
// Sized for I/O-bound work: helm template / oras pull / git clone all
// release the worker briefly while blocked on the network. A sensible
// default is runtime.NumCPU() * 4, but callers know their workload
// better than the package does.
func NewBounded(workers int) *Service {
	s := New()
	if workers > 0 {
		s.sem = make(chan struct{}, workers)
	}
	return s
}

// Go launches an active task. ctx is propagated to fn. Completion is
// reported via WaitActive / BlockTillDone. When the Service is
// bounded (NewBounded), fn waits on the worker semaphore before it
// executes — but Go itself never blocks.
func (s *Service) Go(ctx context.Context, name string, fn func(context.Context)) {
	s.active.Add(1)
	s.wgActive.Add(1)
	go func() {
		defer s.wgActive.Done()
		defer s.active.Add(-1)
		defer func() {
			if r := recover(); r != nil {
				s.failures.Add(1)
				slog.Error("task panicked", "name", name, "panic", r)
			}
		}()
		if s.sem != nil {
			select {
			case s.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-s.sem }()
		}
		fn(ctx)
	}()
}

// YieldSlot releases the worker-pool slot held by the current goroutine,
// runs fn, then re-acquires a slot before returning. Use this around
// blocking waits (e.g. depwait) so queued tasks can make progress while
// the holder is parked on external state. Without this, N tasks waiting
// on each other for slot-gated work deadlock under NewBounded(N).
//
// MUST be called only from inside a body launched by Service.Go —
// calling from outside corrupts the semaphore accounting.
//
// The re-acquire is deferred so a panic inside fn still restores the
// slot count; otherwise Service.Go's outer `defer <-s.sem` would drain
// a phantom slot on unwind, eventually hanging another goroutine that
// did own a slot legitimately.
//
// On an unbounded Service (New or NewBounded(<=0)), fn runs unchanged.
func (s *Service) YieldSlot(fn func()) {
	if s.sem == nil {
		fn()
		return
	}
	<-s.sem
	defer func() { s.sem <- struct{}{} }()
	fn()
}

// Failures returns the number of panicked tasks observed.
func (s *Service) Failures() int64 { return s.failures.Load() }

// ActiveCount returns the number of in-flight tasks. Includes
// tasks that have been Go'd but are still parked on the worker
// semaphore (NewBounded). Useful as a quiescence signal — when
// every reconcile has finished, ActiveCount returns 0.
//
// Callers asking from inside a Go'd body should remember their
// own goroutine is counted: a "no other work" check needs
// ActiveCount() > 1, not > 0.
func (s *Service) ActiveCount() int64 { return s.active.Load() }

// BlockTillDone waits until every active task has finished. Safe to
// call concurrently with Go.
func (s *Service) BlockTillDone() { s.wgActive.Wait() }

// Coalescer keeps at most one task per key in flight; submits that
// arrive while a key is running collapse into a single re-run after it
// returns. fn must re-read its inputs each call — a coalesced re-run
// exists precisely because the previous submit's inputs went stale.
type Coalescer[K comparable] struct {
	svc  *Service
	mu   sync.Mutex
	slot map[K]*coalSlot
}

type coalSlot struct {
	running bool
	pending bool
}

// NewCoalescer constructs a Coalescer that schedules work onto svc.
func NewCoalescer[K comparable](svc *Service) *Coalescer[K] {
	return &Coalescer[K]{svc: svc, slot: make(map[K]*coalSlot)}
}

// Submit schedules fn for key, starting a new active task if key is
// idle or marking the running slot pending otherwise.
func (c *Coalescer[K]) Submit(ctx context.Context, name string, key K, fn func(context.Context)) {
	c.mu.Lock()
	s := c.slot[key]
	if s == nil {
		s = &coalSlot{}
		c.slot[key] = s
	}
	if s.running {
		s.pending = true
		c.mu.Unlock()
		return
	}
	s.running = true
	c.mu.Unlock()

	c.svc.Go(ctx, name, func(ctx context.Context) {
		// On panic, Service.Go's recover catches it; we still need to
		// clear running so a future Submit on this key is dispatched
		// instead of silently coalescing into a slot that no longer
		// has a runner.
		defer func() {
			if r := recover(); r != nil {
				c.mu.Lock()
				s.running = false
				s.pending = false
				c.mu.Unlock()
				panic(r)
			}
		}()
		for {
			fn(ctx)
			c.mu.Lock()
			if !s.pending {
				s.running = false
				c.mu.Unlock()
				return
			}
			s.pending = false
			c.mu.Unlock()
		}
	})
}
