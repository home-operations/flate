package task

import (
	"context"
	"testing"
)

func BenchmarkYieldSlot_Bounded(b *testing.B) {
	s := NewBounded(2)
	s.Go(b.Context(), "benchmark", func(context.Context) {
		b.ReportAllocs()
		for b.Loop() {
			s.YieldSlot(func() {})
		}
	})
	s.BlockTillDone()
}

func BenchmarkDispatch_SharedBudget(b *testing.B) {
	owner := NewBounded(2)
	left, right := owner.Fork(), owner.Fork()
	work := func(context.Context) {}
	b.ReportAllocs()
	for b.Loop() {
		left.Go(b.Context(), "left", work)
		right.Go(b.Context(), "right", work)
		left.BlockTillDone()
		right.BlockTillDone()
	}
}
