package schedule

import (
	"context"
	"fmt"
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
