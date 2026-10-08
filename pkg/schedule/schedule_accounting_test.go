package schedule

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/task"
)

func TestRedispatch_SelectorChain(t *testing.T) {
	for _, depth := range []int{8, 32, 40} {
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tasks := task.NewBounded(2)
			var s *Scheduler
			var mu sync.Mutex
			wave, arrived := 0, 0
			barrier := make(chan struct{})
			runs := make([]int, depth)
			emitted := make([]bool, depth)
			ids := make([]NodeID, depth)
			indexes := make(map[NodeID]int, depth)
			for i := range depth {
				ids[i] = id(fmt.Sprintf("stage-%02d", i))
				indexes[ids[i]] = i
			}
			s = New(tasks, dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				i := indexes[nid]
				mu.Lock()
				round, gate := wave, barrier
				runs[i]++
				arrived++
				if arrived == depth {
					wave++
					arrived = 0
					barrier = make(chan struct{})
					close(gate)
				}
				mu.Unlock()
				// A shared input snapshot forces exactly one stage per sweep;
				// yielding slots permits genuinely overlapping bounded bodies.
				tasks.YieldSlot(func() {
					select {
					case <-gate:
					case <-ctx.Done():
					}
				})
				if round == i {
					emitted[i] = true
					s.OnArrival(id(fmt.Sprintf("provider-%02d", i)), false)
					return OutcomeTerminal, nil
				}
				if round == 0 {
					return OutcomeTerminal, nil // fresh empty artifact
				}
				return OutcomeTerminalNoop, nil
			}))
			s.SetRerunAtDrain(func(NodeID) bool { return true })
			s.Seed(ids)
			if err := s.Run(ctx); err != nil {
				t.Fatal(err)
			}
			for i, nid := range ids {
				if !emitted[i] || runs[i] != depth+1 || s.nodes[nid].redispatches > 2 {
					t.Fatalf("%s: emitted=%v runs=%d charges=%d; want emitted, %d runs, at most 2 charges", nid, emitted[i], runs[i], s.nodes[nid].redispatches, depth+1)
				}
			}
		})
	}
}

func TestRedispatch_QueueCauses(t *testing.T) {
	for _, transition := range []string{"immediate", "parked", "recheck", "sweep", "cascade", "force", "runnable"} {
		for _, content := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/content_%v", transition, content), func(t *testing.T) {
				s := New(task.NewBounded(2), nil)
				nid, dep := id("selector"), id("dependency")
				n := &node{id: nid, state: stateRunning, rerun: true, productive: true, contentRequested: content}
				s.nodes[nid] = n
				s.inFlight = 1
				switch transition {
				case "immediate":
					n.rerunRequested = true
					s.complete(nid, OutcomeBlocked, []NodeID{dep}, true)
				case "recheck":
					s.nodes[dep] = &node{id: dep, state: stateTerminal}
					s.complete(nid, OutcomeBlocked, []NodeID{dep}, true)
				case "parked", "cascade", "force":
					n.state, n.blockedOn = stateParked, []NodeID{dep}
					s.parkedIdx[dep] = map[NodeID]struct{}{nid: {}}
					if transition == "parked" {
						s.OnStatusWake(dep, true, false)
					} else {
						s.draining = DrainCascade
						if transition == "force" {
							s.draining = DrainForce
						}
						s.requeueAllParkedLocked()
					}
				case "sweep":
					n.state = stateTerminal
					s.requeueRerunLocked()
				case "runnable":
					n.state = stateRunnable
					s.runq = append(s.runq, nid)
					s.OnArrival(nid, true)
				}
				if n.state != stateRunnable || n.redispatches != 1 || !n.queuedCharged || n.productive || n.contentRequested || len(s.runq) != 1 || len(s.parkedIdx) != 0 {
					t.Fatalf("queue transition left stale or duplicate causes: node=%+v queue=%v parked=%v", n, s.runq, s.parkedIdx)
				}
				s.OnArrival(nid, true)
				s.OnArrival(nid, true)
				if n.redispatches != 1 || n.contentRequested || len(s.runq) != 1 {
					t.Fatalf("runnable content did not coalesce: %+v, queue=%v", n, s.runq)
				}
				n.state, n.queuedCharged = stateRunning, false
				s.inFlight = 1
				s.complete(nid, OutcomeTerminalNoop, nil, true)
				s.runq = nil
				s.requeueRerunLocked()
				if n.redispatches != 1 || n.productive || n.queuedCharged {
					t.Fatalf("no-op sweep charged a consumed cause: %+v", n)
				}
			})
		}
	}
}

func TestRedispatch_BlockedDependencyRetries(t *testing.T) {
	for _, status := range []bool{false, true} {
		t.Run(fmt.Sprintf("status_%v", status), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var s *Scheduler
			victim, producer, dep := id("a-consumer"), id("z-producer"), id("data")
			var runs atomic.Int64
			started := make(chan struct{})
			s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
				if nid == producer {
					select {
					case <-started:
					case <-ctx.Done():
						return OutcomeTerminal, nil
					}
					for range maxRedispatches + 8 {
						waitUntil(t, func() bool {
							s.mu.Lock()
							defer s.mu.Unlock()
							return s.nodes[victim].state == stateParked || ctx.Err() != nil
						})
						if status {
							s.OnStatusWake(dep, true, false)
						} else {
							s.OnArrival(dep, false)
						}
					}
					return OutcomeTerminal, nil
				}
				if runs.Add(1) == 1 {
					close(started)
				}
				if runs.Load() > maxRedispatches+8 {
					return OutcomeTerminal, nil
				}
				return OutcomeBlocked, []NodeID{dep}
			}))
			s.Seed([]NodeID{victim, producer})
			if err := s.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if runs.Load() != maxRedispatches+9 || s.nodes[victim].redispatches != 0 {
				t.Fatalf("runs=%d charges=%d; want 41 free executions", runs.Load(), s.nodes[victim].redispatches)
			}
		})
	}
}

func TestRedispatch_SlotWaitPreservesContent(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tasks := task.NewBounded(2)
	occupied := make(chan struct{}, 2)
	release := make(chan struct{})
	for range 2 {
		tasks.Go(ctx, "occupy", func(ctx context.Context) {
			occupied <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		<-occupied
	}
	victim := id("waiting")
	var runs atomic.Int64
	s := New(tasks, dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) {
		runs.Add(1)
		return OutcomeTerminalNoop, nil
	}))
	s.Seed([]NodeID{victim})
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitUntil(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.nodes[victim].state == stateRunning
	})
	s.OnArrival(victim, true)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 2 || s.nodes[victim].redispatches != 1 {
		t.Fatalf("slot-wait arrival lost: runs=%d node=%+v", runs.Load(), s.nodes[victim])
	}
}

func TestRedispatch_EmitBeforeBlockCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var s *Scheduler
	var active, runs atomic.Int64
	started, held := make(chan struct{}, 2), make(chan struct{}, 2)
	gate := make(chan struct{})
	x, y := id("x"), id("y")
	data := func(nid NodeID) NodeID { return NodeID{Kind: "ConfigMap", Name: nid.Name} }
	s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
		if ctx.Err() != nil {
			return OutcomeBlocked, nil
		}
		active.Add(1)
		defer active.Add(-1)
		count := runs.Add(1)
		if count <= 2 {
			started <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
			}
		}
		other := x
		if nid == x {
			other = y
		}
		if count <= 2 && nid == x {
			return OutcomeBlocked, []NodeID{data(other)}
		}
		if count >= 81 {
			held <- struct{}{}
			<-ctx.Done()
			return OutcomeBlocked, []NodeID{data(other)}
		}
		for ctx.Err() == nil {
			s.mu.Lock()
			parked := s.nodes[other].state == stateParked
			s.mu.Unlock()
			if parked {
				break
			}
			runtime.Gosched()
		}
		// Publish X then block on Y, and vice versa. Such custom publication
		// cannot be distinguished from free, unchanged dependency retries.
		s.OnArrival(data(nid), false)
		if count == 80 {
			held <- struct{}{}
			<-ctx.Done()
		}
		return OutcomeBlocked, []NodeID{data(other)}
	}))
	s.Seed([]NodeID{x, y})
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if active.Load() != 2 {
		t.Fatal("fixture did not overlap two bodies")
	}
	close(gate)
	for range 2 {
		select {
		case <-held:
		case <-ctx.Done():
			t.Fatal("feedback failed to reach finite cancellation handshake")
		}
	}
	if active.Load() != 2 {
		t.Fatal("cancellation must drain two active bodies")
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || active.Load() != 0 || strings.Contains(err.Error(), "did not converge") || runs.Load() != 81 {
		t.Fatalf("unsupported feedback must cancel and drain: runs=%d active=%d error=%v", runs.Load(), active.Load(), err)
	}
}

func TestRedispatch_TerminalEmissionFeedback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var s *Scheduler
	var active atomic.Int64
	var mu sync.Mutex
	counts := map[NodeID]int{}
	started := make(chan struct{}, 2)
	gate := make(chan struct{})
	x, y := id("x"), id("y")
	s = New(task.NewBounded(2), dispatchFunc(func(ctx context.Context, nid NodeID, _ int) (Outcome, []NodeID) {
		active.Add(1)
		defer active.Add(-1)
		mu.Lock()
		counts[nid]++
		run := counts[nid]
		mu.Unlock()
		if run == 1 {
			started <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
			}
		}
		other := x
		if nid == x {
			other = y
		}
		if nid == x && run%2 == 0 {
			return OutcomeBlocked, []NodeID{other}
		}
		s.OnArrival(NodeID{Kind: "ConfigMap", Name: nid.Name}, false)
		return OutcomeTerminal, nil
	}))
	s.SetRerunAtDrain(func(NodeID) bool { return true })
	s.Seed([]NodeID{x, y})
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if active.Load() != 2 {
		t.Fatal("fixture did not overlap two bodies")
	}
	close(gate)
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "exceeded 32 redispatches") || !strings.Contains(err.Error(), x.String()) && !strings.Contains(err.Error(), y.String()) || ctx.Err() != nil || active.Load() != 0 {
		t.Fatalf("terminal publication must be bounded and drain: active=%d context=%v error=%v", active.Load(), ctx.Err(), err)
	}
	for nid, n := range s.nodes {
		if n.redispatches > maxRedispatches {
			t.Fatalf("%s exceeded its charge cap: %+v", nid, n)
		}
	}
}

func TestRedispatch_RunnableUpgradeAtCap(t *testing.T) {
	for _, charged := range []bool{false, true} {
		t.Run(fmt.Sprintf("charged_%v", charged), func(t *testing.T) {
			s := New(task.NewBounded(2), nil)
			n := &node{id: id("queued"), state: stateRunnable, redispatches: maxRedispatches, queuedCharged: charged}
			s.nodes[n.id] = n
			s.runq = []NodeID{n.id}
			s.OnArrival(n.id, true)
			if charged {
				if s.err != nil || n.contentRequested {
					t.Fatalf("coalesced arrival charged twice: node=%+v error=%v", n, s.err)
				}
			} else if s.err == nil || !strings.Contains(s.err.Error(), n.id.String()) {
				t.Fatalf("free queued execution must reject an over-cap content upgrade: %v", s.err)
			}
			if n.redispatches != maxRedispatches || len(s.runq) != 1 {
				t.Fatalf("cap transition duplicated work: node=%+v queue=%v", n, s.runq)
			}
		})
	}
}

func TestOutcome_Compatibility(t *testing.T) {
	if OutcomeTerminal != 0 || OutcomeBlocked != 1 || OutcomeDependencyFailed != 2 || OutcomeTerminalNoop != 3 {
		t.Fatal("Outcome values must retain public numeric compatibility")
	}
}
