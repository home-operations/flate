// Package task provides a lightweight goroutine lifecycle manager
// modeled on flux-local's TaskService. Active tasks are bounded units
// of work (a single reconciliation, a dependency wait) whose completion
// is tracked via BlockTillDone. A single Service should be associated
// with one orchestrator run.
package task

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// defaultWorkers caps Go-body concurrency in a Service constructed via
// New. Sized for I/O-bound work (helm template / oras pull / git
// clone) — sufficient to saturate the CPU while leaving headroom for
// blocked network calls. Embedders that need a different cap should
// call NewBounded explicitly.
var defaultWorkers = runtime.NumCPU() * 4

// Service tracks active goroutines.
type Service struct {
	wgActive sync.WaitGroup

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

// New constructs a Service with the package-default bounded worker
// pool (runtime.NumCPU() * 4). This was previously unbounded — every
// Go call spawned a goroutine that ran immediately — which is a
// foot-gun for embedders that fan out thousands of submits and rely
// on the pool to throttle. Callers that genuinely need unbounded
// concurrency must use NewUnbounded explicitly.
func New() *Service { return NewBounded(defaultWorkers) }

// NewBounded constructs a Service that caps the number of concurrently
// executing active-task bodies at workers. Submitting more does not
// block — the surplus goroutines exist but wait on an internal
// semaphore until a slot opens. workers <= 0 disables bounding
// (equivalent to NewUnbounded).
//
// Sized for I/O-bound work: helm template / oras pull / git clone all
// release the worker briefly while blocked on the network. A sensible
// default is runtime.NumCPU() * 4, but callers know their workload
// better than the package does.
func NewBounded(workers int) *Service {
	s := &Service{}
	if workers > 0 {
		s.sem = make(chan struct{}, workers)
	}
	return s
}

// NewUnbounded constructs a Service with no concurrency cap — every
// Go submission runs immediately on a fresh goroutine. Use only when
// the caller's workload is naturally bounded (single-digit submits,
// fixed-fanout testing harness) or when the caller manages its own
// throttling on top of the Service. Most production paths should
// prefer NewBounded with a workload-sized cap, or New for the
// package default.
func NewUnbounded() *Service { return &Service{} }

// Go launches an active task. ctx is propagated to fn. Completion is
// reported via BlockTillDone. When the Service is
// bounded (NewBounded), fn waits on the worker semaphore before it
// executes — but Go itself never blocks.
func (s *Service) Go(ctx context.Context, name string, fn func(context.Context)) {
	s.wgActive.Add(1)
	go func() {
		started := time.Now()
		defer s.wgActive.Done()
		defer func() {
			if d := time.Since(started); d > time.Second {
				slog.Debug("task complete", "name", name, "duration", d)
			}
		}()
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
// blocking waits where fn is still doing productive work (helm template
// running, network fetch in flight) so queued tasks can make progress
// while the holder is I/O-bound. Without this, N tasks waiting on each
// other for slot-gated work deadlock under NewBounded(N).
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
	if s.sem != nil {
		defer s.releaseSlot()()
	}
	fn()
}

// releaseSlot drops the calling goroutine's worker slot and returns a
// closure that re-acquires one. Callers defer the returned closure so a
// panic in the released gap still restores the slot count; otherwise
// Service.Go's outer `defer <-s.sem` drains a phantom slot on unwind,
// eventually hanging a goroutine that legitimately owns a slot. Only
// valid on a bounded Service (s.sem != nil).
func (s *Service) releaseSlot() func() {
	<-s.sem
	return func() { s.sem <- struct{}{} }
}

// Failures returns the number of panicked tasks observed.
func (s *Service) Failures() int64 { return s.failures.Load() }

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
		// On panic, clear the slot before re-raising so future Submits
		// on this key dispatch normally. If a pending re-run exists it
		// is dropped — restarting on deterministic-panic input loops —
		// and logged so the loss is observable.
		defer func() {
			if r := recover(); r != nil {
				c.mu.Lock()
				lostPending := s.pending
				delete(c.slot, key)
				c.mu.Unlock()
				if lostPending {
					slog.Warn("task coalescer: dropped pending re-run because the previous run panicked",
						"key", key)
				}
				panic(r)
			}
		}()
		for {
			fn(ctx)
			c.mu.Lock()
			if !s.pending {
				delete(c.slot, key)
				c.mu.Unlock()
				return
			}
			s.pending = false
			c.mu.Unlock()
		}
	})
}
