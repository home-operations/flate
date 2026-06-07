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

	// quiesceMu guards quiesceWaiters. Acquired only on Go/Done and
	// QuiescenceCh; never inside fn. The waiter list is small and
	// short-lived (one entry per depwait waitRenderEmission call).
	quiesceMu      sync.Mutex
	quiesceWaiters []quiesceWaiter
}

// quiesceWaiter pairs a threshold with the channel to close when the
// active count drops to <= threshold.
type quiesceWaiter struct {
	threshold int64
	ch        chan struct{}
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
	s.active.Add(1)
	s.wgActive.Add(1)
	go func() {
		started := time.Now()
		defer s.wgActive.Done()
		defer s.taskDone()
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

// taskDone is deferred in Go so both clean returns and panics fire
// the decrement and quiescence notification.
func (s *Service) taskDone() {
	now := s.active.Add(-1)
	s.notifyQuiescence(now)
}

// notifyQuiescence is called on every active-count decrement —
// goroutine exit (taskDone) AND YieldQuiescent entry — so
// depwait-blocked tasks don't prevent the pool from reaching
// quiescence on their own absence of productive work.
func (s *Service) notifyQuiescence(now int64) {
	s.quiesceMu.Lock()
	defer s.quiesceMu.Unlock()
	if len(s.quiesceWaiters) == 0 {
		return
	}
	kept := s.quiesceWaiters[:0]
	for _, w := range s.quiesceWaiters {
		if now <= w.threshold {
			close(w.ch)
			continue
		}
		kept = append(kept, w)
	}
	s.quiesceWaiters = kept
}

// QuiescenceCh returns a channel closed when the active-task count
// drops to <= threshold. The channel is fresh per call; callers
// waiting on distinct thresholds work independently. When the
// active-task count is already <= threshold at call time, the
// channel is returned closed.
//
// Used by depwait's render-emission wait to fire the moment no other
// reconcile is in flight, instead of polling every 100ms. The
// orchestrator drains exactly once per run, so a successful
// quiescence signal is a one-shot event.
func (s *Service) QuiescenceCh(threshold int64) <-chan struct{} {
	ch := make(chan struct{})
	s.quiesceMu.Lock()
	defer s.quiesceMu.Unlock()
	// Re-read active under quiesceMu so we don't race a concurrent
	// taskDone that already closed waiters at the current threshold.
	if s.active.Load() <= threshold {
		close(ch)
		return ch
	}
	s.quiesceWaiters = append(s.quiesceWaiters, quiesceWaiter{threshold: threshold, ch: ch})
	return ch
}

// YieldSlot releases the worker-pool slot held by the current goroutine,
// runs fn, then re-acquires a slot before returning. Use this around
// blocking waits where fn is still doing productive work (helm template
// running, network fetch in flight) so queued tasks can make progress
// while the holder is I/O-bound. Without this, N tasks waiting on each
// other for slot-gated work deadlock under NewBounded(N).
//
// Compare YieldQuiescent: that variant additionally decrements the
// active count for callers waiting on OTHER tasks' work (depwait).
// YieldSlot keeps the active count incremented because the caller IS
// producing — quiescence-aware consumers must NOT see the caller as
// idle.
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

// YieldQuiescent releases the worker slot AND decrements the active
// count for the duration of fn. Use when fn is a wait on external
// state that cannot itself produce store mutations — typically
// depwait blocking on a dep that other tasks must produce. The
// decrement lets QuiescenceCh fire on the caller's behalf: a
// depwait-blocked task shouldn't keep the orchestrator from
// declaring quiescence on its own absence of productive work.
//
// Without this hop, two reconciles both blocked in depwait (e.g. a
// parent KS waiting on a typo'd dependsOn and its child HR waiting
// on the parent's status) hold the active-task count at 2
// indefinitely — QuiescenceCh(1) never fires and both waiters ride
// the full RenderProducingTimeout cap.
//
// MUST be called only from inside a body launched by Service.Go —
// calling from outside corrupts the active counter.
//
// Both the active increment and the slot re-acquire are deferred so
// a panic inside fn still restores both counters; without that,
// Service.Go's outer `defer s.taskDone()` would over-decrement on
// unwind.
func (s *Service) YieldQuiescent(fn func()) {
	if s.sem != nil {
		<-s.sem
		defer func() { s.sem <- struct{}{} }()
	}
	now := s.active.Add(-1)
	s.notifyQuiescence(now)
	defer s.active.Add(1)
	fn()
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
