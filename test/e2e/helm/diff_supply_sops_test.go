package helm_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI harness changes process-global logging and environment.
func TestE2E_DiffSupplySOPSConverges(t *testing.T) {
	for _, name := range []string{"reported", "no-supply", "unchanged", "no-parent", "build", "reversed"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "diff-supply-sops-hang", name)
			want := fixture.Want(t)
			for range 2 {
				if got := fixture.Run(t); !slices.Equal(got, want) {
					t.Fatalf("result: want %#v, got %#v", want, got)
				}
			}
		})
	}
}
