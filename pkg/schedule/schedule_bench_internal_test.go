package schedule

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

type benchmarkAssessment bool

func (a benchmarkAssessment) Valid() bool { return bool(a) }

func (benchmarkAssessment) Outcome() Kind { return Terminal }

func (benchmarkAssessment) Progressed() bool { return true }

func (benchmarkAssessment) Settle(context.Context, manifest.DrainLevel) {}

func (benchmarkAssessment) Retire() {}

func (benchmarkAssessment) Close() {}

func BenchmarkScheduler_Run(b *testing.B) {
	ids := make([]NodeID, 32)
	for i := range ids {
		ids[i] = testutil.KustomizationID("ns", fmt.Sprintf("node-%02d", i))
	}
	d := dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
		return Result{}
	})
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
	id := testutil.KustomizationID("ns", "content")
	b.ReportAllocs()
	for b.Loop() {
		runs := 0
		s := New(task.NewBounded(2), dispatchFunc(func(context.Context, NodeID, manifest.DrainLevel) Result {
			runs++
			return Result{Assessment: benchmarkAssessment(runs > maxRedispatches)}
		}))
		s.Seed([]NodeID{id})
		if err := s.Run(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOnPublication(b *testing.B) {
	s := New(task.NewBounded(2), nil)
	b.ReportAllocs()
	for b.Loop() {
		s.OnPublication()
	}
}

func BenchmarkOnArrival_Data(b *testing.B) {
	s := New(task.NewBounded(2), nil)
	id := NodeID{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "data"}
	b.ReportAllocs()
	for b.Loop() {
		s.OnArrival(id, false)
	}
}

func BenchmarkComplete_Assessment(b *testing.B) {
	for _, unresolved := range []bool{false, true} {
		b.Run(fmt.Sprintf("Unresolved=%v", unresolved), func(b *testing.B) {
			s := New(task.NewBounded(2), nil)
			id := testutil.KustomizationID("ns", "consumer")
			n := &node{id: id}
			s.nodes[id] = n
			a := &testAssessment{unresolved: unresolved}
			b.ReportAllocs()
			for b.Loop() {
				n.state = stateRunning
				s.inFlight = 1
				s.complete(n, Result{Assessment: a}, false)
			}
		})
	}
}
