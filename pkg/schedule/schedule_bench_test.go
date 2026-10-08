package schedule

import (
	"testing"

	"github.com/home-operations/flate/pkg/task"
)

func BenchmarkOnStatusWake(b *testing.B) {
	s := New(task.NewBounded(2), nil)
	dep := id("dependency")
	s.OnStatusWake(dep, true, false)
	b.ReportAllocs()
	for b.Loop() {
		s.OnStatusWake(dep, true, false)
	}
}
