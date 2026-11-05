package crds_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func runCRDScenario(t *testing.T, scenario, name string) {
	t.Helper()
	fixture := harness.Open(t, scenario, name)
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) {
		t.Fatalf("first sequence:\nwant %#v\ngot %#v", want, first)
	}
	if !slices.Equal(second, want) {
		t.Fatalf("second sequence:\nwant %#v\ngot %#v", want, second)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("sequences differ:\nfirst %#v\nsecond %#v", first, second)
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ResourceSetExpandsIntoChildKustomizations(t *testing.T) {
	t.Run("static", func(t *testing.T) {
		runCRDScenario(t, "resourceset", "resource-set-expands-into-child-kustomizations")
	})
	for _, name := range []string{
		"provider-warnings", "static-provider", "selector-provider", "flatten-provider", "permute-provider",
		"supplied-provider", "oci-tags", "invalid-template", "invalid-selector",
	} {
		t.Run(name, func(t *testing.T) {
			runCRDScenario(t, "resourceset-provider", name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ResourceSetGeneratedData(t *testing.T) {
	for _, name := range []string{"all", "only-explicit-skips", "only-implicit-skips", "defaults", "skip-data", "encrypted"} {
		t.Run(name, func(t *testing.T) {
			runCRDScenario(t, "resourceset", "resource-set-generated-data-"+name)
		})
	}
	t.Run("billimek-scalers", func(t *testing.T) {
		runCRDScenario(t, "resourceset-documents", "billimek-scalers")
	})
}

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ResourceSetDocumentBoundary(t *testing.T) {
	runCRDScenario(t, "resourceset-documents", "resource-set-document-boundary")
}
