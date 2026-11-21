package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_EntryUnknownProjection(t *testing.T) {
	for _, name := range []string{"rootless", "real-parent"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "entry-unknown-projection", name)
			want := fixture.Want(t)
			first, second := fixture.Run(t), fixture.Run(t)
			if !slices.Equal(first, want) || !slices.Equal(second, want) {
				t.Fatalf("want %#v\nfirst %#v\nsecond %#v", want, first, second)
			}
		})
	}
}
