package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_RootlessAggregate(t *testing.T) {
	for _, name := range []string{"plain-files", "compatible-packages-and-plain-files", "authored-builder-authority", "incompatible-packages", "generator-only-package", "generator-only-package-diff", "namespace-dependencies", "ancestor-bootstrap", "ancestor-bootstrap-indexed-producer"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "rootless-aggregate", name)
			want := fixture.Want(t)
			first, second := fixture.Run(t), fixture.Run(t)
			if !slices.Equal(first, want) {
				t.Fatalf("first sequence: want %#v, got %#v", want, first)
			}
			if !slices.Equal(second, want) {
				t.Fatalf("second sequence: want %#v, got %#v", want, second)
			}
			if !slices.Equal(first, second) {
				t.Fatalf("sequences differ: first %#v, second %#v", first, second)
			}
		})
	}
}
