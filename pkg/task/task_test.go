package task

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

func TestNewBounded_LimitsConcurrentBodies(t *testing.T) {
	const workers = 3
	s := NewBounded(workers)

	var inFlight, peak atomic.Int64
	gate := make(chan struct{})

	const submits = 12
	for range submits {
		s.Go(context.Background(), "w", func(_ context.Context) {
			n := inFlight.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-gate
			inFlight.Add(-1)
		})
	}

	// Give the goroutines a moment to acquire / queue on the semaphore.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && inFlight.Load() < workers {
		time.Sleep(5 * time.Millisecond)
	}
	if got := inFlight.Load(); got != workers {
		t.Errorf("expected exactly %d bodies active behind the semaphore, got %d", workers, got)
	}
	close(gate)
	s.BlockTillDone()
	if got := peak.Load(); got > workers {
		t.Errorf("peak in-flight = %d, want <= %d", got, workers)
	}
}

func TestNewBounded_Unbounded(t *testing.T) {
	// Workers <= 0 disables bounding (no semaphore allocated).
	s := NewBounded(0)
	if s.sem != nil {
		t.Errorf("expected nil semaphore for workers=0")
	}
	s = NewBounded(-1)
	if s.sem != nil {
		t.Errorf("expected nil semaphore for workers=-1")
	}
}

func TestService_BlockTillDone(t *testing.T) {
	s := NewBounded(0)
	var n atomic.Int64
	for range 50 {
		s.Go(context.Background(), "w", func(_ context.Context) {
			time.Sleep(time.Millisecond)
			n.Add(1)
		})
	}
	s.BlockTillDone()
	assert.Equal(t, n.Load(), int64(50))
}

func TestService_PanicCountedAndRecovered(t *testing.T) {
	s := NewBounded(0)
	s.Go(context.Background(), "boom", func(_ context.Context) {
		panic("oops")
	})
	s.BlockTillDone()
	assert.Equal(t, s.Failures(), int64(1))
}

// TestYieldSlot_AllowsChildrenWhenParentsFillPool simulates the
// parent/child Kustomization deadlock: two parents each take a slot in
// a 2-worker pool, then block waiting on a child. Without YieldSlot
// the children can never acquire a slot and the whole run hangs.
func TestYieldSlot_AllowsChildrenWhenParentsFillPool(t *testing.T) {
	s := NewBounded(2)
	childrenStarted := make(chan struct{}, 2)
	childrenDone := make(chan struct{})

	// Two children that will run once they get a slot.
	for range 2 {
		s.Go(context.Background(), "child", func(_ context.Context) {
			childrenStarted <- struct{}{}
			<-childrenDone
		})
	}

	// Two parents that occupy the slots and wait for the children.
	parentsDone := make(chan struct{})
	for range 2 {
		s.Go(context.Background(), "parent", func(_ context.Context) {
			s.YieldSlot(func() {
				// Wait until both children have started — which can only
				// happen if YieldSlot actually released our slot.
				<-childrenStarted
			})
		})
	}

	go func() {
		s.BlockTillDone()
		close(parentsDone)
	}()

	// Both children must have started under YieldSlot.
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("children never started — YieldSlot did not release slots")
	case <-parentsDone:
		t.Fatal("parents finished before children started — unexpected ordering")
	default:
	}

	// Release children; parents should reclaim slots and finish.
	close(childrenDone)
	select {
	case <-parentsDone:
	case <-time.After(2 * time.Second):
		t.Fatal("parents never finished after children completed")
	}
}

// TestYieldSlot_UnboundedIsNoOp asserts the no-pool path runs fn
// transparently.
func TestYieldSlot_UnboundedIsNoOp(t *testing.T) {
	s := NewBounded(0)
	called := false
	s.YieldSlot(func() { called = true })
	if !called {
		t.Errorf("YieldSlot did not invoke fn on unbounded Service")
	}
}

// TestYieldSlot_PanicRestoresSlot guards against a phantom-slot drain
// when fn panics: without the deferred re-acquire, Service.Go's outer
// `defer <-s.sem` would consume an extra token on unwind, eventually
// hanging another goroutine that legitimately holds a slot.
func TestYieldSlot_PanicRestoresSlot(t *testing.T) {
	s := NewBounded(1)

	// Run a panicking YieldSlot inside a real Service.Go goroutine so
	// the outer defer chain actually fires; Service.Go's own recover
	// absorbs the panic.
	s.Go(context.Background(), "boom", func(_ context.Context) {
		func() {
			defer func() { _ = recover() }()
			s.YieldSlot(func() { panic("boom") })
		}()
	})
	s.BlockTillDone()

	// After the panicked run, a fresh Go must still acquire a slot
	// promptly. Without the fix the buffered semaphore would have
	// been drained by one token and this second body would hang.
	ran := make(chan struct{})
	s.Go(context.Background(), "next", func(_ context.Context) { close(ran) })
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool drained after YieldSlot panic — subsequent task could not acquire a slot")
	}
	s.BlockTillDone()
}
