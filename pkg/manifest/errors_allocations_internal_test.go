//go:build !race

package manifest

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestInputf_Allocations(t *testing.T) {
	var err error
	allocations := testing.AllocsPerRun(100, func() {
		err = inputf("missing %s", "kind")
	})
	{
		assert.FatalIf(t, allocations != 2, "input error allocates %v objects, want 2", allocations)
	}
	{
		assert.FatalIf(t, !errors.Is(err, ErrInput) || err.Error() != "missing kind", "input error = %v", err)
	}
}
