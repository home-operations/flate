package depwait

import (
	"context"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestWatchOne_QuiescenceBound_WaitsPastWallClock is the core determinism
// regression: a PRESENT but not-Ready dependency (a source still fetching, an
// HR still rendering) must be waited for by the producer's OUTCOME, not
// abandoned at the per-dep wall clock. With a RenderInflight wired, the wait is
// bound to quiescence on the parent ctx; a Ready that lands well after the
// 50ms Timeout must still be observed. Before the fix watchOne capped the wait
// at Timeout and returned DepTimeout — the chart-source / dependsOn flake.
func TestWatchOne_QuiescenceBound_WaitsPastWallClock(t *testing.T) {
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "slow"}
	s := store.New()
	s.UpdateStatus(dep, store.StatusPending, "fetching")

	// Quiescence never fires (a producer is still in flight), so the wait can
	// only end on the dep going Ready.
	neverQuiesce := make(chan struct{})
	w := &Waiter{
		Store:   s,
		Timeout: 50 * time.Millisecond,
		Renders: rendersStub{quiesce: func() <-chan struct{} { return neverQuiesce }},
	}

	go func() {
		time.Sleep(150 * time.Millisecond) // > the 50ms wall clock
		s.UpdateStatus(dep, store.StatusReady, "")
	}()

	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if sum.AnyFailed() {
		t.Fatalf("present-Pending dep with a Ready at 150ms must be waited for past the %v wall clock; got failure %q",
			w.Timeout, sum.Messages[dep])
	}
}

// TestWatchOne_QuiescenceBound_GivesUpOnQuiescence verifies the terminator: a
// present dep that never becomes Ready is given up the moment the pool drains
// (no producer left), classified DepTimeout "not ready" — NOT rid to the full
// (here 5s) wall clock.
func TestWatchOne_QuiescenceBound_GivesUpOnQuiescence(t *testing.T) {
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "stuck"}
	s := store.New()
	s.UpdateStatus(dep, store.StatusPending, "fetching")

	quiesce := make(chan struct{})
	w := &Waiter{
		Store:   s,
		Timeout: 5 * time.Second, // long: prove we give up on quiescence, not this
		Renders: rendersStub{quiesce: func() <-chan struct{} { return quiesce }},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(quiesce) // pool drained, dep still Pending
	}()

	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if !sum.AnyFailed() {
		t.Fatal("a never-Ready dep must be given up on quiescence")
	}
	if got := sum.Messages[dep]; got != "not ready" {
		t.Errorf("give-up reason = %q; want %q", got, "not ready")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("give-up took %v; expected to terminate at quiescence (~50ms), not the 5s wall clock", elapsed)
	}
}

// TestWatchOne_NoRenders_KeepsWallClock confirms the legacy path: with no
// RenderInflight wired, a present-Pending dep still fails at the wall-clock
// Timeout (quiescence-binding only applies when a signal is wired).
func TestWatchOne_NoRenders_KeepsWallClock(t *testing.T) {
	dep := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "ns", Name: "p"}
	s := store.New()
	s.UpdateStatus(dep, store.StatusPending, "fetching")

	w := &Waiter{Store: s, Timeout: 50 * time.Millisecond} // Renders nil
	start := time.Now()
	sum := WaitAll(w.Watch(context.Background(), testutil.DepRefs(dep)))
	if !sum.AnyFailed() {
		t.Fatal("without a quiescence signal a never-Ready dep must time out at the wall clock")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("wall-clock fail took %v; expected ~Timeout", elapsed)
	}
}
