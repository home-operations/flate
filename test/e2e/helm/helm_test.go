// Package helm tests Helm rendering, listing and diagnostics.
package helm_test

import (
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_HelmCorpus(t *testing.T) {
	for _, tc := range []struct{ scenario, name string }{
		{"helm-render", "buildhr-secret-excluded"},
		{"helm-render", "buildhr-secret-included"},
		{"helm-render", "malformed-chartcrd-omitted"},
		{"helm-render", "malformed-chartcrd-included"},
		{"helm-render", "malformed-chart-fails-producer-duplicate-keys"},
		{"helm-render", "malformed-chart-fails-producer-typed-child"},
		{"chart-inputs", "helm-source-root-values-files-reported-strict"},
		{"chart-inputs", "helm-source-root-values-files-reported-ignore-missing"},
		{"chart-inputs", "helm-source-root-values-files-outside-chart"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-template-edit"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-template-add"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-template-delete"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-template-strict-delete"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-chartref-edit"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-chartref-add"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-chartref-delete"},
		{"chart-inputs", "helm-source-root-values-files-changed-only-chartref-strict-delete"},
		{"helm-identity", "gethr-yaml-exposes-projected-fields-target-namespace"},
		{"helm-identity", "gethr-yaml-exposes-projected-fields-explicit-override"},
		{"helm-identity", "helm-release-target-namespace-identity"},
		{"helm-documents", "invalid-helm-list-included"},
		{"helm-documents", "invalid-helm-list-excluded"},
		{"helm-documents", "helm-list-member-diff-added"},
		{"helm-documents", "helm-list-member-diff-removed"},
		{"helm-values", "unreadable-parent-does-not-fill-required-values-full"},
		{"helm-values", "unreadable-parent-does-not-fill-required-values-changed-only"},
		{"helm-values", "unreadable-parent-does-not-fill-required-values-diff-current"},
		{"helm-values", "unreadable-parent-does-not-fill-required-values-diff-orig"},
		{"helm-values", "stale-values-warning"},
		{"helm-dependencies", "changed-onlyhr-depends-on-unchanged-dep"},
		{"helm-dependencies", "changed-only-emitted-kustomization-path-templated"},
		{"helm-dependencies", "changed-only-emitted-kustomization-path-templated-root"},
		{"helm-dependencies", "changed-only-emitted-kustomization-path-literal"},
		{"helm-dependencies", "changed-only-emitted-kustomization-path-literal-root"},
		{"helm-dependencies", "changed-only-render-emitted-dependency"},
		{"helm-dependencies", "ready-expr--helm-release"},
		{"helm-oci", "helm-missingoci-source"},
	} {
		t.Run(tc.scenario+"/"+tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
			want := fixture.Want(t)
			for range 2 {
				got := fixture.Run(t)
				if len(got) != len(want) {
					t.Fatalf("sequence length: want %d, got %d", len(want), len(got))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf(
							"step %s: code want %d, got %d\nstdout want:\n%s\ngot:\n%s\nstderr want:\n%s\ngot:\n%s",
							got[i].Step, want[i].Code, got[i].Code,
							want[i].Stdout, got[i].Stdout, want[i].Stderr, got[i].Stderr,
						)
					}
				}
			}
		})
	}
}
