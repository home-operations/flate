package scope_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_EntryApplicationRendered(t *testing.T) {
	testScopeCases(t, "entry-applicator", []string{
		"entry-application-rendered-fluxinstance",
		"entry-application-rendered-kustomization",
	})
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_LiteralEntry(t *testing.T) {
	fixture := harness.Open(t, "entry-root", "literal-app")
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) {
		t.Fatalf("first result:\nwant %#v\ngot %#v", want, first)
	}
	if !slices.Equal(second, want) {
		t.Fatalf("second result:\nwant %#v\ngot %#v", want, second)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("results differ:\nfirst %#v\nsecond %#v", first, second)
	}
}
