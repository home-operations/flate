package sourceref_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_SourceFileMembership(t *testing.T) {
	for _, name := range []string{"specignore", "f7-include-ignored", "mini1", "templates", "sourceignore"} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "flux-source-membership", name)
			want := fixture.Want(t)
			for _, regime := range []string{"cold", "warm"} {
				t.Run(regime, func(t *testing.T) {
					if got := fixture.Run(t); !slices.Equal(want, got) {
						t.Errorf("CLI result:\nwant %#v\ngot %#v", want, got)
					}
				})
			}
		})
	}
}
