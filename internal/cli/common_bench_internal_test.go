package cli

import (
	"testing"

	"github.com/home-operations/flate/pkg/selection"
)

func BenchmarkSelectedWarnings_Empty(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		selectedWarnings(&selection.Result{})
	}
}
