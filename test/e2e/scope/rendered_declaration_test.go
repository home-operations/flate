package scope_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_ParentRenderedDeclaration(t *testing.T) {
	for _, scenario := range []string{"path", "identity", "site"} {
		t.Run(scenario, func(t *testing.T) {
			var witness []harness.Result
			for _, name := range []string{"witness", "control"} {
				t.Run(name, func(t *testing.T) {
					fixture := harness.Open(t, "rendered-declaration-"+scenario, name)
					want := fixture.Want(t)
					first, second := fixture.Run(t), fixture.Run(t)
					assertScopeResults(t, want, first)
					assertScopeResults(t, want, second)
					assertScopeResults(t, first, second)
					if name == "witness" {
						witness = first
					} else {
						assertScopeResults(t, witness, first)
					}
				})
			}
		})
	}
}

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_ParentRenderedDeclarationHelm(t *testing.T) {
	fixture := harness.Open(t, "rendered-declaration-helm", "witness")
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	assertScopeResults(t, want, first)
	assertScopeResults(t, want, second)
	assertScopeResults(t, first, second)
}

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_DeclarationReadiness(t *testing.T) {
	for _, name := range []string{"secret", "secret-allowed", "secret-supplied", "prediction", "prediction-allowed", "source", "frontier-emitted", "frontier-native"} {
		t.Run(name, func(t *testing.T) {
			scenario, _, _ := strings.Cut(name, "-")
			fixture := harness.Open(t, "declaration-readiness-"+scenario, name)
			want := fixture.Want(t)
			first, second := fixture.Run(t), fixture.Run(t)
			assertScopeResults(t, want, first)
			assertScopeResults(t, want, second)
			assertScopeResults(t, first, second)
		})
	}
}
