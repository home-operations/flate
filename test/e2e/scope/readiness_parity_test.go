package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ReadinessParity(t *testing.T) {
	testScopeCases(t, "readiness-parity", []string{
		"source-republication",
		"kustomization-rename",
		"kustomization-rename-patch-reader",
		"helmrelease-rename",
		"helmrelease-rename-replacement",
		"helmrelease-rename-component",
		"helmrelease-rename-suspended",
		"helmrelease-rename-prefix",
		"helmrelease-rename-prefix-configured",
		"helmrelease-rename-patch",
		"helmrelease-rename-patch-configured",
		"helmrelease-rename-patch-dependency",
		"helmrelease-rename-patch-dependency-rename",
		"helmrelease-rename-patch-dependency-descendant",
		"helmrelease-rename-patch-dependency-selector",
		"helmrelease-rename-patch-dependency-strategic",
		"helmrelease-rename-patch-reader-kind",
		"helmrelease-rename-patch-target",
		"helmrelease-rename-substitute-inline",
		"helmrelease-rename-substitute-replacement",
		"helmrelease-rename-substitute-from",
		"helmrelease-rename-patch-target-and-producer",
		"helmrelease-rename-flux-patch",
		"helmrelease-rename-patched-configmap",
		"helmrelease-rename-expression",
		"helmrelease-required-base-failure",
		"ordering-name-prefix",
		"ordering-name-suffix",
		"ordering-target-namespace",
		"ordering-flux-patch",
		"ordering-nested-component",
		"ordering-replacement",
		"ordering-relative-component",
		"ordering-relative-unrelated",
		"ordering-parent-path",
		"ordering-parent-unrelated",
		"transitive-failure",
	})
}
