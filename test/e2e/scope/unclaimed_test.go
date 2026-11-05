package scope_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_UnclaimedPathWithoutHistory(t *testing.T) {
	fixture := harness.Open(t, "unclaimed-change", "unclaimed-path-without-history")
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) || !slices.Equal(second, want) || !slices.Equal(first, second) {
		t.Fatalf("unclaimed edit: want %#v, first %#v, second %#v", want, first, second)
	}
}
