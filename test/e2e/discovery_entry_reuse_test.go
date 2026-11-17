package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_DiscoveryEntryReuseNilSource(t *testing.T) {
	for _, name := range []string{"reported", "no-patches", "no-root-declarations", "missing-tag"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "discovery-entry-reuse-nil-source", name)
			want := fixture.Want(t)
			first, second := fixture.Run(t), fixture.Run(t)
			if !slices.Equal(first, want) {
				t.Errorf("first sequence:\nwant %#v\ngot %#v", want, first)
			}
			if !slices.Equal(second, want) {
				t.Errorf("second sequence:\nwant %#v\ngot %#v", want, second)
			}
			if !slices.Equal(first, second) {
				t.Errorf("sequences differ:\nfirst %#v\nsecond %#v", first, second)
			}
		})
	}
}
