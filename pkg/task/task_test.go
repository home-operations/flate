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
