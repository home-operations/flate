package schedule

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkScheduler_Run(b *testing.B) {
	ids := make([]NodeID, 32)
	for i := range ids {
		ids[i] = id(fmt.Sprintf("node-%02d", i))
	}
	d := dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) { return OutcomeTerminal, nil })
	b.ReportAllocs()
	for b.Loop() {
		s := New(task.NewBounded(4), d)
		s.Seed(ids)
		if err := s.Run(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScheduler_ContentRedispatch(b *testing.B) {
	nid := id("content")
	b.ReportAllocs()
	for b.Loop() {
		var s *Scheduler
		runs := 0
		s = New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) {
			runs++
			if runs <= maxRedispatches {
				s.OnArrival(nid, true)
			}
			return OutcomeTerminal, nil
		}))
		s.Seed([]NodeID{nid})
		if err := s.Run(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOnStatusWake(b *testing.B) {
	s := New(task.NewBounded(2), nil)
	dep := id("dependency")
	s.OnStatusWake(dep, true, false)
	b.ReportAllocs()
	for b.Loop() {
		s.OnStatusWake(dep, true, false)
	}
}

const progressCorpusSize = 1_000_000

func progressCorpus(kind string) []NodeID {
	ids := make([]NodeID, progressCorpusSize)
	for i := range ids {
		ids[i] = NodeID{Kind: kind, Namespace: "default", Name: fmt.Sprintf("progress-%06d", i)}
	}
	return ids
}

func BenchmarkOnArrival_Data(b *testing.B) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		ids := progressCorpus(kind)
		for _, active := range []bool{false, true} {
			for _, distinct := range []bool{false, true} {
				mode := "Reused"
				if distinct {
					mode = "Distinct"
				}
				b.Run(fmt.Sprintf("%s/%s/Active=%v", mode, kind, active), func(b *testing.B) {
					benchmarkProgress(b, ids, progressWorkload{active: active, distinct: distinct})
				})
			}
		}
	}
}

func BenchmarkOnStatusWake_Progress(b *testing.B) {
	ids := progressCorpus("Kustomization")
	for _, existing := range []bool{false, true} {
		for _, active := range []bool{false, true} {
			for _, distinct := range []bool{false, true} {
				mode := "Reused"
				if distinct {
					mode = "Distinct"
				}
				b.Run(fmt.Sprintf("%s/Existing=%v/Active=%v", mode, existing, active), func(b *testing.B) {
					var nodes map[NodeID]*node
					if existing {
						count := 1
						if distinct {
							count = len(ids)
						}
						nodes = make(map[NodeID]*node, count)
						for _, nid := range ids[:count] {
							nodes[nid] = &node{id: nid, state: stateTerminal}
						}
					}
					benchmarkProgress(b, ids, progressWorkload{active: active, distinct: distinct, ready: true, nodes: nodes})
				})
			}
		}
	}
}

type progressWorkload struct {
	active, distinct, ready bool
	nodes                   map[NodeID]*node
}

func benchmarkProgress(b *testing.B, ids []NodeID, workload progressWorkload) {
	b.Helper()
	newScheduler := func() *Scheduler {
		s := New(task.NewBounded(2), nil)
		if workload.active {
			s.inFlight = 1
		}
		if workload.nodes != nil {
			s.nodes = workload.nodes
		}
		return s
	}
	s := newScheduler()
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		if workload.distinct && i == len(ids) {
			// Every distinct batch starts without warmed recovery history.
			b.StopTimer()
			s = newScheduler()
			i = 0
			b.StartTimer()
		}
		if workload.ready {
			s.OnStatusWake(ids[i], true, false)
		} else {
			s.OnArrival(ids[i], false)
		}
		if workload.distinct {
			i++
		}
	}
}

func BenchmarkComplete_Failed(b *testing.B) {
	for _, fanIn := range []int{2, 32} {
		previous := make([]NodeID, fanIn)
		for i := range previous {
			previous[i] = id(fmt.Sprintf("blocker-%02d", i))
		}
		for _, mode := range []string{"Unchanged", "Reordered", "Replaced"} {
			blocked := slices.Clone(previous)
			if mode == "Reordered" {
				slices.Reverse(blocked)
			}
			if mode == "Replaced" {
				blocked[0] = id("replacement")
			}
			b.Run(fmt.Sprintf("FanIn=%d/%s", fanIn, mode), func(b *testing.B) {
				next := blocked
				s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, int) (Outcome, []NodeID) {
					return OutcomeDependencyFailed, next
				}))
				nid := id("consumer")
				n := &node{id: nid}
				s.nodes[nid] = n
				n.failedOn = previous
				for _, dep := range append(slices.Clone(previous), blocked...) {
					s.failedIdx[dep] = map[NodeID]struct{}{nid: {}, id("other"): {}}
				}
				b.ReportAllocs()
				for b.Loop() {
					n.state = stateRunning
					s.inFlight = 1
					out, failed := s.disp.Dispatch(b.Context(), nid, DrainNone)
					s.complete(nid, out, failed, false)
					if mode != "Unchanged" {
						if &next[0] == &blocked[0] {
							next = previous
						} else {
							next = blocked
						}
					}
				}
			})
		}
	}
}
