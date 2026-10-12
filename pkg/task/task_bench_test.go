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
