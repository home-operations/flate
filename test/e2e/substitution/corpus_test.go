// Package substitution tests data reading, substitution and inherited patches.
package substitution_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func runSubstitutionCases(t *testing.T, scenario string, names ...string) {
	t.Helper()
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, scenario, name)
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
