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
	"time"
)

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

// NewBounded constructs a Service that caps the number of concurrently
// executing active-task bodies at workers. Submitting more does not
// block — the surplus goroutines exist but wait on an internal
// semaphore until a slot opens. workers <= 0 disables bounding (every
// Go submission runs immediately on a fresh goroutine).
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
// On an unbounded Service (NewBounded(<=0)), fn runs unchanged.
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
