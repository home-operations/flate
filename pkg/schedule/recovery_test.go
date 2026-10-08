package schedule

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

func awaitTerminal(ctx context.Context, s *Scheduler, nid NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.nodes[nid].state != stateTerminal && s.err == nil && !s.canceled {
		s.cond.Wait()
	}
	return ctx.Err() == nil && s.err == nil
}

func awaitSignal(ctx context.Context, ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func TestDependencyFailureRecovery(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, mode := range []string{"after terminal", "during registration", "data arrival", "permanent failure"} {
			t.Run(fmt.Sprintf("%d/%s", workers, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				victim, driver, dep := id("consumer"), id("driver"), id("dependency")
				if mode == "data arrival" {
					dep.Kind = manifest.KindConfigMap
				}
				observed, finish := make(chan struct{}), make(chan struct{})
				var runs, active atomic.Int64
				var recovered atomic.Bool
				var s *Scheduler
				s = New(task.NewBounded(workers), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
					if nid == driver {
						if !awaitSignal(ctx, observed) {
							return OutcomeTerminal, nil
						}
						if mode != "during registration" && !awaitTerminal(ctx, s, victim) {
							return OutcomeTerminal, nil
						}
						s.OnStatusWake(dep, false, true)
						if mode != "permanent failure" {
							if mode == "data arrival" {
								s.OnArrival(dep, false)
							} else {
								s.OnStatusWake(dep, true, false)
								s.OnStatusWake(dep, true, false)
							}
						}
						if mode == "during registration" {
							close(finish)
						}
						return OutcomeTerminal, nil
					}
					if active.Add(1) != 1 {
						t.Error("consumer dispatched concurrently")
					}
					defer active.Add(-1)
					if runs.Add(1) == 1 {
						close(observed)
						if mode == "during registration" {
							awaitSignal(ctx, finish)
						}
						return OutcomeDependencyFailed, []NodeID{dep}
					}
					recovered.Store(true)
					return OutcomeTerminal, nil
				}))
				s.Seed([]NodeID{victim, driver})
				if err := s.Run(ctx); err != nil {
					t.Fatal(err)
				}
				want := int64(2)
				if mode == "permanent failure" {
					want = 1
				}
				if runs.Load() != want || recovered.Load() != (want == 2) {
					t.Fatalf("runs=%d recovered=%v, want %d", runs.Load(), recovered.Load(), want)
				}
				if s.nodes[victim].redispatches != int(want-1) {
					t.Fatalf("recovery bypassed budget: %+v", s.nodes[victim])
				}
				if want == 2 && (len(s.failedIdx) != 0 || len(s.nodes[victim].failedOn) != 0) {
					t.Fatalf("successful recovery retained blockers: %v", s.failedIdx)
				}
			})
		}
	}
}

func TestDependencyFailureRunningWakeAndReplacement(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, mode := range []string{"success", "changed dependencies", "primary failure", "cancellation"} {
			t.Run(fmt.Sprintf("reverse_%v/%s", reverse, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				victim, driver := id("consumer"), id("driver")
				first, second, replacement := id("a"), id("b"), id("c")
				if reverse {
					first, second = second, first
				}
				entered, release, third := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var runs, active atomic.Int64
				var s *Scheduler
				s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
					if nid == driver {
						if !awaitTerminal(ctx, s, victim) {
							return OutcomeTerminal, nil
						}
						s.OnStatusWake(first, true, false)
						if !awaitSignal(ctx, entered) {
							return OutcomeTerminal, nil
						}
						s.OnStatusWake(second, true, false)
						s.OnStatusWake(second, true, false)
						if mode == "cancellation" {
							cancel()
							close(release)
							return OutcomeTerminal, nil
						}
						close(release)
						if !awaitSignal(ctx, third) || !awaitTerminal(ctx, s, victim) {
							return OutcomeTerminal, nil
						}
						s.OnStatusWake(first, false, false)
						s.OnStatusWake(first, true, false)
						if mode == "changed dependencies" {
							s.mu.Lock()
							_, old := s.failedIdx[first]
							_, fresh := s.failedIdx[replacement]
							s.mu.Unlock()
							if old || !fresh {
								t.Errorf("old blockers retained or replacement absent: old=%v new=%v", old, fresh)
							}
							s.OnStatusWake(replacement, true, false)
						}
						return OutcomeTerminal, nil
					}
					if active.Add(1) != 1 {
						t.Error("consumer dispatched concurrently")
					}
					defer active.Add(-1)
					switch runs.Add(1) {
					case 1:
						return OutcomeDependencyFailed, []NodeID{first, second}
					case 2:
						close(entered)
						awaitSignal(ctx, release)
						if mode == "changed dependencies" {
							return OutcomeDependencyFailed, []NodeID{replacement}
						}
					case 3:
						close(third)
						if mode == "changed dependencies" {
							return OutcomeDependencyFailed, []NodeID{replacement}
						}
					}
					return OutcomeTerminal, nil
				}))
				s.Seed([]NodeID{victim, driver})
				err := s.Run(ctx)
				if mode == "cancellation" {
					if !errors.Is(err, context.Canceled) || active.Load() != 0 {
						t.Fatalf("cancellation did not drain: err=%v active=%d", err, active.Load())
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				want := int64(3)
				if mode == "changed dependencies" {
					want = 4
				}
				if runs.Load() != want || len(s.failedIdx) != 0 {
					t.Fatalf("runs=%d blockers=%v, want %d with no stale edges", runs.Load(), s.failedIdx, want)
				}
				if s.nodes[victim].redispatches != int(want-1) {
					t.Fatalf("budget=%d, want %d", s.nodes[victim].redispatches, want-1)
				}
			})
		}
	}
}

func TestDependencyFailureMultiHopRecovery(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			a, b, c, driver := id("a"), id("b"), id("c"), id("driver")
			var readyA, readyB, readyC atomic.Bool
			var runsB, runsC atomic.Int64
			var s *Scheduler
			s = New(task.NewBounded(workers), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				switch nid {
				case a:
					return OutcomeTerminal, nil
				case driver:
					if awaitTerminal(ctx, s, b) && awaitTerminal(ctx, s, c) {
						readyA.Store(true)
						s.OnStatusWake(a, true, false)
					}
				case b:
					runsB.Add(1)
					if !readyA.Load() {
						return OutcomeDependencyFailed, []NodeID{a}
					}
					readyB.Store(true)
					s.OnStatusWake(b, true, false)
				case c:
					runsC.Add(1)
					if !readyB.Load() {
						return OutcomeDependencyFailed, []NodeID{b}
					}
					readyC.Store(true)
					s.OnStatusWake(c, true, false)
				}
				return OutcomeTerminal, nil
			}))
			s.Seed([]NodeID{a, b, c, driver})
			if err := s.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if !readyB.Load() || !readyC.Load() || runsB.Load() != 2 || runsC.Load() != 2 || len(s.failedIdx) != 0 {
				t.Fatalf("multi-hop recovery incomplete: B=%v/%d C=%v/%d blockers=%v", readyB.Load(), runsB.Load(), readyC.Load(), runsC.Load(), s.failedIdx)
			}
		})
	}
}

func TestDependencyFailureRedispatchLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var s *Scheduler
	var runs atomic.Int64
	dep, victim := id("dep"), id("consumer")
	s = New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) {
		runs.Add(1)
		s.OnStatusWake(dep, false, false)
		s.OnStatusWake(dep, true, false)
		return OutcomeDependencyFailed, []NodeID{dep}
	}))
	s.Seed([]NodeID{victim})
	if err := s.Run(ctx); err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || ctx.Err() != nil {
		t.Fatalf("Run error=%v context=%v, want redispatch limit", err, ctx.Err())
	}
	if runs.Load() != maxRedispatches+1 {
		t.Fatalf("runs=%d, want %d", runs.Load(), maxRedispatches+1)
	}
}
