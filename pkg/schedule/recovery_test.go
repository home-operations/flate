package schedule

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
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

func TestProgress_RegistrationEpisodes(t *testing.T) {
	a, b, c := id("a-dependency"), id("b-dependency"), id("c-dependency")
	cases := []struct {
		name                        string
		blockers                    [][]NodeID
		event                       string
		existing, overwrite, cancel bool
		charges                     int
	}{
		{name: "unknown Ready", blockers: [][]NodeID{{a}, nil}, event: "ready", charges: 1},
		{name: "existing Ready", blockers: [][]NodeID{{a}, nil}, event: "ready", existing: true, charges: 1},
		{name: "data", blockers: [][]NodeID{{a}, nil}, event: "data", charges: 1},
		{name: "overwritten data", blockers: [][]NodeID{{a}, nil}, event: "data", overwrite: true},
		{name: "overwritten unknown Ready", blockers: [][]NodeID{{a}, nil}, event: "ready", overwrite: true},
		{name: "existing Ready survives overwrite", blockers: [][]NodeID{{a}, nil}, event: "ready", existing: true, overwrite: true, charges: 1},
		{name: "several precise blockers", blockers: [][]NodeID{{a, b}, nil}, event: "ready", charges: 1},
		{name: "continuous unrelated feedback", blockers: [][]NodeID{{a, b}, {a, b}}, event: "unrelated"},
		{name: "reordered blockers", blockers: [][]NodeID{{a, b}, {b, a}}, event: "unrelated"},
		{name: "duplicate reordered blockers", blockers: [][]NodeID{{a, a, b}, {b, a, b}}, event: "unrelated"},
		{name: "duplicates cannot hide removal", blockers: [][]NodeID{{a, b}, {a, a}, {a, a}}, event: "unrelated"},
		{name: "replacement", blockers: [][]NodeID{{a, b}, {c, b}, {b, c}}, event: "unrelated"},
		{name: "success clears episode", blockers: [][]NodeID{{a}, nil}, event: "unrelated"},
		{name: "failed status is not progress", blockers: [][]NodeID{{a}}, event: "failed"},
		{name: "cancellation clears episode", blockers: [][]NodeID{{a}, {a}}, event: "unrelated", cancel: true},
	}
	for _, workers := range []int{2, 4} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("workers_%d/%s", workers, tc.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				victim, producer := id("consumer"), id("producer")
				blockerRounds := tc.blockers
				if tc.event == "data" {
					blockerRounds = make([][]NodeID, len(tc.blockers))
					for round, blockers := range tc.blockers {
						for _, dep := range blockers {
							dep.Kind = manifest.KindConfigMap
							blockerRounds[round] = append(blockerRounds[round], dep)
						}
					}
				}
				requested, applied := make(chan int), make(chan struct{})
				var runs, self, active atomic.Int64
				var s *Scheduler
				s = New(task.NewBounded(workers), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
					active.Add(1)
					defer active.Add(-1)
					if nid == producer {
						for round, blockers := range blockerRounds {
							select {
							case got := <-requested:
								assert.Equal(t, got, round)
							case <-ctx.Done():
								return OutcomeTerminal, nil
							}
							assert.Equal(t, active.Load(), int64(2))
							if round == 1 && tc.cancel {
								cancel()
								s.mu.Lock()
								for !s.canceled {
									s.cond.Wait()
								}
								s.mu.Unlock()
								close(applied)
								return OutcomeTerminal, nil
							}
							if len(blockers) > 0 {
								switch tc.event {
								case "unrelated":
									s.OnArrival(id("unrelated"), false)
								case "failed":
									s.OnStatusWake(blockers[0], false, true)
								case "ready", "data":
									for range 3 {
										for _, dep := range blockers {
											if tc.event == "data" {
												s.OnArrival(dep, false)
											} else {
												s.OnStatusWake(dep, true, false)
											}
										}
									}
								}
								if tc.overwrite {
									s.OnArrival(id("unrelated"), false)
								}
							}
							select {
							case applied <- struct{}{}:
							case <-ctx.Done():
								return OutcomeTerminal, nil
							}
						}
						return OutcomeTerminal, nil
					}
					assert.Equal(t, self.Add(1), int64(1))
					defer self.Add(-1)
					round := int(runs.Add(1)) - 1
					select {
					case requested <- round:
					case <-ctx.Done():
						return OutcomeTerminal, nil
					}
					if tc.cancel {
						// Hold the body until cancellation is observed by the scheduler.
						<-applied
					} else {
						awaitSignal(ctx, applied)
					}
					if round >= len(tc.blockers) {
						t.Error("unexpected retry")
						return OutcomeTerminal, nil
					}
					if blockers := blockerRounds[round]; len(blockers) > 0 {
						return OutcomeDependencyFailed, blockers
					}
					return OutcomeTerminal, nil
				}))
				if tc.existing {
					s.nodes[a] = &node{id: a, state: stateTerminal}
				}
				s.Seed([]NodeID{victim, producer})
				err := s.Run(ctx)
				if tc.cancel {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
				} else if err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, runs.Load(), int64(len(tc.blockers)))
				assert.Equal(t, s.nodes[victim].redispatches, tc.charges)
				assert.Equal(t, self.Load(), int64(0))
				if tc.cancel || len(tc.blockers[len(tc.blockers)-1]) == 0 {
					assert.Equal(t, len(s.failedIdx), 0)
					assert.Equal(t, s.nodes[victim].conservativeUsed, false)
				} else {
					for _, dep := range tc.blockers[len(tc.blockers)-1] {
						_, retained := s.failedIdx[dep][victim]
						assert.Equal(t, retained, true)
					}
				}
			})
		}
	}
}

func TestProgress_UnwatchedData(t *testing.T) {
	ids := progressCorpus("ConfigMap")
	for i := range ids {
		if i%2 != 0 {
			ids[i].Kind = manifest.KindSecret
		}
	}
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			consumer, producer := id("consumer"), id("producer")
			started, finished := make(chan struct{}), make(chan struct{})
			var s *Scheduler
			s = New(task.NewBounded(workers), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				if nid == consumer {
					close(started)
					awaitSignal(ctx, finished)
					return OutcomeTerminal, nil
				}
				if !awaitSignal(ctx, started) {
					return OutcomeTerminal, nil
				}
				// Warm-up and measured arrivals MUST use disjoint identities.
				kind := manifest.KindConfigMap
				allocations := testing.AllocsPerRun(1, func() {
					for _, dep := range ids {
						dep.Kind = kind
						s.OnArrival(dep, false)
					}
					kind = manifest.KindSecret
				})
				assert.Equal(t, allocations, float64(0))
				s.mu.Lock()
				assert.Equal(t, s.inFlight, 2)
				assert.Equal(t, s.untracked, ids[len(ids)-1])
				assert.Equal(t, s.untrackedAt, s.generation)
				assert.Equal(t, len(s.nodes), 2)
				assert.Equal(t, len(s.failedIdx), 0)
				assert.Equal(t, len(s.parkedIdx), 0)
				s.mu.Unlock()
				close(finished)
				return OutcomeTerminal, nil
			}))
			s.Seed([]NodeID{consumer, producer})
			if err := s.Run(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProgress_IdleInterest(t *testing.T) {
	s := New(task.NewBounded(2), nil)
	n := &node{id: id("consumer"), state: stateTerminal}
	dep := id("dependency")
	s.nodes[n.id] = n
	s.failedIdx[dep] = map[NodeID]struct{}{n.id: {}}
	s.OnArrival(id("unrelated"), false)
	assert.Equal(t, s.generation, uint64(0))
	s.OnStatusWake(dep, false, true)
	assert.Equal(t, s.generation, uint64(0))
	s.OnStatusWake(dep, true, false)
	assert.Equal(t, n.redispatches, 1)
	assert.Equal(t, n.state, stateRunnable)
}

func TestProgress_BlockerSlicesImmutable(t *testing.T) {
	s := New(task.NewBounded(2), nil)
	a, b := id("a-dependency"), id("b-dependency")
	blocked := []NodeID{a, a, b}
	want := []NodeID{a, a, b}
	consumer := id("consumer")
	n := &node{id: consumer, state: stateRunning}
	s.nodes[consumer], s.inFlight = n, 1
	s.complete(consumer, OutcomeDependencyFailed, blocked, false)
	assert.Diff(t, blocked, want)
	assert.Diff(t, n.failedOn, []NodeID{a, b})
	n.conservativeUsed, n.state, s.inFlight = true, stateRunning, 1
	s.complete(consumer, OutcomeDependencyFailed, []NodeID{b, a, b}, false)
	assert.Equal(t, n.conservativeUsed, true)
	assert.Diff(t, blocked, want)
}
