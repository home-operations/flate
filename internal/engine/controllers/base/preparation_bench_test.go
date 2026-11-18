package base_test

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/controllers/base"
)

func BenchmarkExecution_PreparationTransfer(b *testing.B) {
	var run base.Execution
	var previous, destination base.Preparation[string]
	from, to := &previous, &destination
	matches := func(value *string) bool { return *value != "" }
	*run.Prepare(from, nil, matches) = "retained"
	b.ReportAllocs()
	for b.Loop() {
		run.Prepare(to, nil, matches)
		from, to = to, from
	}
}
