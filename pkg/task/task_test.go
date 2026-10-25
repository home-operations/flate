package task

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

func TestNewBounded_LimitsConcurrentBodies(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const workers = 3
		s := NewBounded(workers)
		trees := []*Service{s, s.Fork(), s.Fork()}

		var inFlight, peak atomic.Int64
		gate := make(chan struct{})

		const submits = 12
		for i := range submits {
			trees[i%len(trees)].Go(t.Context(), "w", func(_ context.Context) {
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

		synctest.Wait()
		if got := inFlight.Load(); got != workers {
			t.Errorf("expected exactly %d bodies active behind the semaphore, got %d", workers, got)
		}
		close(gate)
		for _, tree := range trees {
			tree.BlockTillDone()
		}
		if got := peak.Load(); got > workers {
			t.Errorf("peak in-flight = %d, want <= %d", got, workers)
		}
	})
}

func TestNewBounded_Unbounded(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		parent := NewBounded(2)
		first, second := parent.Fork(), parent.Fork()
		release := make(chan struct{})
		second.Go(t.Context(), "held", func(context.Context) { <-release })
		first.Go(t.Context(), "complete", func(context.Context) {})
		firstDone, secondDone := make(chan struct{}), make(chan struct{})
		go func() { first.BlockTillDone(); close(firstDone) }()
		go func() { second.BlockTillDone(); close(secondDone) }()
		synctest.Wait()
		select {
		case <-firstDone:
		default:
			t.Fatal("completed tree waited for its sibling")
		}
		select {
		case <-secondDone:
			t.Fatal("held tree drained before its body completed")
		default:
		}
		parent.BlockTillDone()
		close(release)
		<-secondDone
	})
}

func TestService_PanicCountedAndRecovered(t *testing.T) {
	t.Parallel()
	parent := NewBounded(2)
	first, second := parent.Fork(), parent.Fork()
	first.Go(t.Context(), "boom", func(context.Context) { panic("oops") })
	second.Go(t.Context(), "complete", func(context.Context) {})
	first.BlockTillDone()
	second.BlockTillDone()
	assert.Equal(t, first.Failures(), int64(1))
	assert.Equal(t, second.Failures(), int64(0))
	assert.Equal(t, parent.Failures(), int64(0))
}

func TestYieldSlot_AllowsChildrenWhenParentsFillPool(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		parent := NewBounded(2)
		parents, children := parent.Fork(), parent.Fork()
		childrenDone := make(chan struct{})
		for range 2 {
			parents.Go(t.Context(), "parent", func(context.Context) {
				parents.YieldSlot(func() { <-childrenDone })
			})
		}
		synctest.Wait()
		var completed atomic.Int64
		for range 2 {
			children.Go(t.Context(), "child", func(context.Context) { completed.Add(1) })
		}
		children.BlockTillDone()
		assert.Equal(t, completed.Load(), int64(2))
		close(childrenDone)
		parents.BlockTillDone()
		assert.Equal(t, len(parent.sem), 0)
	})
}

// TestYieldSlot_UnboundedIsNoOp asserts the no-pool path runs fn
// transparently.
func TestYieldSlot_UnboundedIsNoOp(t *testing.T) {
	t.Parallel()
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
