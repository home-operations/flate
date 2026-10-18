package keylock

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestKeyMap_SerializesSameKey(t *testing.T) {
	t.Parallel()
	m := New[string]()
	var concurrent atomic.Int32
	var peak atomic.Int32
	bumpPeak := func() {
		n := concurrent.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				return
			}
		}
	}

	done := make(chan struct{}, 4)
	for range 4 {
		go func() {
			defer func() { done <- struct{}{} }()
			release, err := m.Acquire(context.Background(), "k")
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer release()
			bumpPeak()
			time.Sleep(10 * time.Millisecond)
			concurrent.Add(-1)
		}()
	}
	for range 4 {
		<-done
	}
	if got := peak.Load(); got != 1 {
		t.Errorf("expected peak 1, got %d", got)
	}
}

func TestKeyMap_DistinctKeysRunParallel(t *testing.T) {
	t.Parallel()
	m := New[string]()
	a, _ := m.Acquire(context.Background(), "a")
	defer a()
	b, _ := m.Acquire(context.Background(), "b")
	defer b()
}

func TestKeyMap_CancelledCtxReturnsErr(t *testing.T) {
	t.Parallel()
	m := New[string]()
	release1, _ := m.Acquire(context.Background(), "k")
	defer release1()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release2, err := m.Acquire(ctx, "k")
	defer release2()
	if err == nil {
		t.Errorf("expected ctx.Err, got nil")
	}
}

func TestKeyMap_PreCancelledUncontendedReturnsErr(t *testing.T) {
	t.Parallel()
	m := New[string]()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 100 {
		release, err := m.Acquire(ctx, "k")
		release()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire: got %v, want context.Canceled", err)
		}
	}
}

func TestKeyMap_CancelUnblocksWaiter(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := New[string]()
		first, _ := m.Acquire(t.Context(), "k")
		defer first()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			release, err := m.Acquire(ctx, "k")
			defer release()
			done <- err
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-done; err == nil {
			t.Errorf("expected non-nil err on cancelled acquire")
		}
	})
}
