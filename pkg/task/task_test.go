package task

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
	// Workers <= 0 disables bounding; matches New().
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
	s := New()
	var n atomic.Int64
	for range 50 {
		s.Go(context.Background(), "w", func(_ context.Context) {
			time.Sleep(time.Millisecond)
			n.Add(1)
		})
	}
	s.BlockTillDone()
	if n.Load() != 50 {
		t.Errorf("expected 50, got %d", n.Load())
	}
	if s.ActiveCount() != 0 {
		t.Errorf("ActiveCount: %d", s.ActiveCount())
	}
}

func TestService_BackgroundDoesNotBlockActive(t *testing.T) {
	s := New()
	bgDone := make(chan struct{})
	defer close(bgDone)
	s.GoBackground(context.Background(), "bg", func(_ context.Context) {
		<-bgDone
	})

	var done atomic.Bool
	s.Go(context.Background(), "active", func(_ context.Context) { done.Store(true) })
	s.BlockTillDone()
	if !done.Load() {
		t.Errorf("active task didn't run")
	}
	if s.BackgroundCount() != 1 {
		t.Errorf("background should still be running, got %d", s.BackgroundCount())
	}
}

func TestService_PanicCountedAndRecovered(t *testing.T) {
	s := New()
	s.Go(context.Background(), "boom", func(_ context.Context) {
		panic("oops")
	})
	s.BlockTillDone()
	if s.Failures() != 1 {
		t.Errorf("expected 1 failure, got %d", s.Failures())
	}
}

// TestCoalescer_RecoversSlotAfterPanic guards a stuck-slot bug: if
// fn(ctx) panicked, the Service-level recover absorbed the panic but
// the per-key slot stayed running=true forever, so every subsequent
// Submit on that key would mark pending and exit without ever
// re-running. The recover in Coalescer.Submit now resets the slot
// before re-raising for the Service-level logger to catch.
func TestCoalescer_RecoversSlotAfterPanic(t *testing.T) {
	s := New()
	c := NewCoalescer[string](s)

	var calls atomic.Int64
	c.Submit(context.Background(), "boom", "k", func(_ context.Context) {
		calls.Add(1)
		panic("boom")
	})
	s.BlockTillDone()
	if got := calls.Load(); got != 1 {
		t.Fatalf("panicking call: expected 1 run, got %d", got)
	}
	if got := s.Failures(); got != 1 {
		t.Errorf("Service.Failures: expected 1, got %d", got)
	}

	// Slot must be unlocked: a fresh Submit on the same key runs.
	c.Submit(context.Background(), "recovery", "k", func(_ context.Context) {
		calls.Add(1)
	})
	s.BlockTillDone()
	if got := calls.Load(); got != 2 {
		t.Errorf("post-panic Submit blocked: expected 2 runs total, got %d", got)
	}
}

func TestCoalescer_SerializesPerKey(t *testing.T) {
	s := New()
	c := NewCoalescer[string](s)

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var runs atomic.Int64
	gate := make(chan struct{})

	bumpPeak := func() {
		now := concurrent.Add(1)
		for {
			peak := maxConcurrent.Load()
			if now <= peak || maxConcurrent.CompareAndSwap(peak, now) {
				return
			}
		}
	}

	c.Submit(context.Background(), "k", "k", func(_ context.Context) {
		bumpPeak()
		runs.Add(1)
		<-gate
		concurrent.Add(-1)
	})
	for range 5 {
		c.Submit(context.Background(), "k", "k", func(_ context.Context) {
			bumpPeak()
			runs.Add(1)
			concurrent.Add(-1)
		})
	}

	close(gate)
	s.BlockTillDone()

	if got := runs.Load(); got != 2 {
		t.Errorf("expected exactly 2 runs (initial + 1 coalesced re-run), got %d", got)
	}
	if peak := maxConcurrent.Load(); peak > 1 {
		t.Errorf("Coalescer permitted %d concurrent runs for same key; must be 1", peak)
	}
}

func TestCoalescer_DistinctKeysRunConcurrently(t *testing.T) {
	s := New()
	c := NewCoalescer[string](s)

	bothStarted := make(chan struct{}, 2)
	release := make(chan struct{})

	for _, k := range []string{"a", "b"} {
		c.Submit(context.Background(), k, k, func(_ context.Context) {
			bothStarted <- struct{}{}
			<-release
		})
	}

	deadline := time.After(2 * time.Second)
	for range 2 {
		select {
		case <-bothStarted:
		case <-deadline:
			t.Fatal("distinct keys did not run concurrently")
		}
	}
	close(release)
	s.BlockTillDone()
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
	s := New()
	called := false
	s.YieldSlot(func() { called = true })
	if !called {
		t.Errorf("YieldSlot did not invoke fn on unbounded Service")
	}
}

func TestService_ActiveNames(t *testing.T) {
	s := New()
	gate := make(chan struct{})
	started := make(chan struct{})
	s.Go(context.Background(), "alpha", func(_ context.Context) {
		close(started)
		<-gate
	})
	<-started
	if names := s.ActiveNames(); len(names) != 1 || names[0] != "alpha" {
		t.Errorf("ActiveNames: %v", names)
	}
	close(gate)
	s.BlockTillDone()
}
