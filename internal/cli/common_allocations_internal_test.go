//go:build !race

package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
)

//nolint:paralleltest // allocation measurements require serial execution.
func TestSelectedWarnings_EmptyAllocations(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() { selectedWarnings(&selection.Selection{}) })
	assert.FatalIf(t, allocs != 0, "empty diagnostics allocated: %v", allocs)
}
