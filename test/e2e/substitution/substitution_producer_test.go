package substitution_test

import "testing"

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutedSelection(t *testing.T) {
	runSubstitutionCases(t, "substitution-selection",
		"case1-forward", "case1-reverse", "case2-forward", "case2-reverse", "case3-forward", "case3-reverse")
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionProducerRendering(t *testing.T) {
	runSubstitutionCases(t, "substitute-producers",
		"substitution-producer-rendering-configmap-full",
		"substitution-producer-rendering-configmap-direct",
		"substitution-producer-rendering-configmap-inherited",
		"substitution-producer-rendering-configmap-env",
		"substitution-producer-rendering-configmap-path-orig",
		"substitution-producer-rendering-configmap-diff",
		"substitution-producer-rendering-secret-full",
		"substitution-producer-rendering-secret-direct",
		"substitution-producer-rendering-secret-inherited",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutedLabelMembership(t *testing.T) {
	runSubstitutionCases(t, "substitution-selection", "label-forward", "label-reverse")
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionProducerCycle(t *testing.T) {
	runSubstitutionCases(t, "substitute-cycles",
		"substitution-producer-cycle-concurrency-1",
		"substitution-producer-cycle-concurrency-2",
		"substitution-producer-cycle-concurrency-8",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionProducerTransformedNamespace(t *testing.T) {
	runSubstitutionCases(t, "substitute-producers",
		"substitution-producer-transformed-namespace-configmap-full",
		"substitution-producer-transformed-namespace-configmap-changed",
		"substitution-producer-transformed-namespace-secret-full",
		"substitution-producer-transformed-namespace-secret-changed",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_SubstitutionCycleDownstream(t *testing.T) {
	runSubstitutionCases(t, "substitute-cycles",
		"substitution-cycle-downstream-concurrency-2",
		"substitution-cycle-downstream-concurrency-8",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_DependencyCycleWarnings(t *testing.T) {
	runSubstitutionCases(t, "substitute-cycles",
		"dependency-cycle-warnings-build",
		"dependency-cycle-warnings-test",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_LiteralKustomizationPath(t *testing.T) {
	runSubstitutionCases(t, "substitute-values",
		"literal-kustomization-path-full-build",
		"literal-kustomization-path-own-postbuild",
		"literal-kustomization-path-changed-only",
		"literal-kustomization-path-paired-diff",
		"literal-kustomization-path-reverse-diff",
	)
}

//nolint:paralleltest // The corpus harness changes process-global state.
func TestE2E_ComponentSecretProducerSelection(t *testing.T) {
	runSubstitutionCases(t, "component-secret-producer",
		"shared-file-ordered",
		"shared-file-unordered",
		"parent-context-ordered",
		"parent-context-unordered",
		"producer-absent",
		"producer-absent-allowed",
		"removed-producer",
		"removed-producer-reverse",
		"renamed-producer",
		"renamed-producer-reverse",
	)
}
