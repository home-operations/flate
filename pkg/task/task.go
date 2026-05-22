// Package task provides a lightweight goroutine lifecycle manager
// modeled on flux-local's TaskService. It distinguishes two flavors of
// concurrent work:
//
//   - Active tasks are bounded units of work (a single reconciliation, a
//     dependency wait) whose completion is interesting to the
//     orchestrator. BlockTillDone waits for these to drain.
//   - Background tasks are long-lived stream processors (a watch loop)
//     whose lifetime is bound to the controller, not to the
//     orchestrator's "is the run complete?" check.
//
// A single Service should be associated with one orchestrator run.
package task

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Service tracks active and background goroutines.
type Service struct {
	wgActive sync.WaitGroup
	wgBack   sync.WaitGroup
	active   atomic.Int64
	back     atomic.Int64

	mu    sync.Mutex
	names map[int64]string
	next  atomic.Int64

	// failures is incremented by goroutines that panic; non-zero implies
	// the run is poisoned.
	failures atomic.Int64

	// sem bounds the number of concurrent active task BODIES. The
	// goroutine is launched eagerly but blocks on the semaphore until
	// a worker slot is free, so callers never block on Go(). Background
	// tasks are unaffected — they're long-lived stream processors.
	//
	// nil = unbounded (every Go runs in parallel). Set via NewBounded.
	sem chan struct{}
}

// New constructs a fresh Service with unbounded concurrency.
func New() *Service {
	return &Service{names: make(map[int64]string)}
}

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
	id := s.next.Add(1)
	s.active.Add(1)
	s.wgActive.Add(1)
	s.mu.Lock()
	s.names[id] = name
	s.mu.Unlock()
	go func() {
		defer s.wgActive.Done()
		defer s.active.Add(-1)
		defer func() {
			s.mu.Lock()
			delete(s.names, id)
			s.mu.Unlock()
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
// On an unbounded Service (New or NewBounded(<=0)), fn runs unchanged.
func (s *Service) YieldSlot(fn func()) {
	if s.sem == nil {
		fn()
		return
	}
	<-s.sem
	fn()
	s.sem <- struct{}{}
}

// GoBackground launches a long-lived task whose completion is not
// counted toward BlockTillDone.
func (s *Service) GoBackground(ctx context.Context, name string, fn func(context.Context)) {
	id := s.next.Add(1)
	s.back.Add(1)
	s.wgBack.Add(1)
	s.mu.Lock()
	s.names[id] = "(bg) " + name
	s.mu.Unlock()
	go func() {
		defer s.wgBack.Done()
		defer s.back.Add(-1)
		defer func() {
			s.mu.Lock()
			delete(s.names, id)
			s.mu.Unlock()
			if r := recover(); r != nil {
				s.failures.Add(1)
				slog.Error("background task panicked", "name", name, "panic", r)
			}
		}()
		fn(ctx)
	}()
}

// ActiveCount returns the number of currently active tasks.
func (s *Service) ActiveCount() int64 { return s.active.Load() }

// BackgroundCount returns the number of currently running background tasks.
func (s *Service) BackgroundCount() int64 { return s.back.Load() }

// Failures returns the number of panicked tasks observed.
func (s *Service) Failures() int64 { return s.failures.Load() }

// ActiveNames returns the names of currently running tasks. Useful for
// debug logging when the orchestrator stalls.
func (s *Service) ActiveNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.names))
	for _, n := range s.names {
		out = append(out, n)
	}
	return out
}

// BlockTillDone waits until every active task has finished. Background
// tasks are ignored. Safe to call concurrently with Go.
func (s *Service) BlockTillDone() { s.wgActive.Wait() }

// BlockTillAllDone waits for active AND background tasks.
func (s *Service) BlockTillAllDone() {
	s.wgActive.Wait()
	s.wgBack.Wait()
}

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
