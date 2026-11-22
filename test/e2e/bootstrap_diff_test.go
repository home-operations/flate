package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The harness changes process-wide logging and environment.
func TestE2E_BootstrapDiff(t *testing.T) {
	for _, tc := range []struct{ scenario, name string }{
		{"bootstrap-namespace-patch", "patched"},
		{"bootstrap-namespace-patch", "unpatched"},
		{"bootstrap-local-artifact", "local-generator"},
		{"bootstrap-source-declaration", "mirror-url"},
		{"bootstrap-sibling-sources", "ssh-origin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
			want := fixture.Want(t)
			for range 2 {
				if got := fixture.Run(t); !slices.Equal(got, want) {
					t.Fatalf("want %#v\ngot  %#v", want, got)
				}
			}
		})
	}
}
