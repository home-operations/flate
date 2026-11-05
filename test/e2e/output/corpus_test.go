package output_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI invocations change process logging, environment, directories and transports.
func TestE2E_OutputCorpus(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		cases []string
	}{
		{name: "output-formats", cases: []string{
			"build-commands",
			"get-commands",
			"test-commands",
			"diff-commands",
			"diff-all-changes",
			"diff-ks-changes",
			"diff-hr-changes",
			"filter-defaults",
			"filter-empty-skip",
			"filter-replacement-skip",
			"filter-only-crds",
			"filter-only-secret",
			"filter-only-multiple",
			"filter-empty-only",
			"filter-exact-kind",
			"filter-exact-exclusion",
			"filter-env-empty-only",
			"filter-argv-overrides-empty-env-skip",
			"filter-explicit-skip-wins",
			"filter-env-skip-wins",
			"filter-env-only",
			"filter-env-empty-skip",
			"filter-argv-overrides-env-skip",
			"filter-argv-empty-only-overrides-env",
			"diff-images",
			"env-include-removed",
			"include-removed-precedence",
			"changed-only-build",
			"member-added",
			"member-removed",
			"suspended-current",
			"suspended-orig",
			"partial-failure",
			"failed-current",
			"failed-orig",
		}},
		{name: "cli-flags", cases: []string{
			"render-flags-1",
			"environment-bindings-2",
			"argv-precedence-3",
			"parser-errors-4",
			"help-and-version",
			"render-flags-5",
			"environment-bindings-6",
			"argv-precedence-7",
			"parser-errors-8",
			"environment-bindings-9",
			"argv-precedence-10",
			"render-flags-11",
			"environment-bindings-12",
			"argv-precedence-13",
			"render-flags-14",
			"environment-bindings-15",
			"argv-precedence-16",
			"omitted-kustomization-path",
			"restricted-local-source",
		}},
		{name: "cache-lifecycle", cases: []string{
			"empty-cache",
			"negative-age",
			"missing-cache",
			"invalid-env-age",
			"invalid-env-dry",
			"cache-command",
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			for _, name := range scenario.cases {
				t.Run(name, func(t *testing.T) {
					fixture := harness.Open(t, scenario.name, name)
					want := fixture.Want(t)
					first, second := fixture.Run(t), fixture.Run(t)
					if !slices.Equal(want, first) {
						t.Errorf("first sequence:\nwant %#v\ngot %#v", want, first)
					}
					if !slices.Equal(want, second) {
						t.Errorf("second sequence:\nwant %#v\ngot %#v", want, second)
					}
					if !slices.Equal(first, second) {
						t.Errorf("replay differs:\nfirst %#v\nsecond %#v", first, second)
					}
				})
			}
		})
	}
}
