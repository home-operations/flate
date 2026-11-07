package sourceref_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI invocations change process logging, environment, directories and transports.
func TestE2E_SourceCorpus(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		cases []string
	}{
		{name: "source-pins", cases: []string{"nonhead-renders-preserve-checkout-tag", "nonhead-renders-preserve-checkout-semver", "nonhead-renders-preserve-checkout-head", "nonhead-renders-preserve-checkout-no-ref", "nonhead-renders-preserve-checkout-commit", "stable-pin-ignores-dirty-content", "source-ref--nonhead-tag", "pinned-diff--base-and-current-refs", "source-ref--pinned-followed-source-inherits-namespace-clusters-explicit", "source-ref--pinned-followed-source-inherits-namespace-clusters-omitted", "source-ref--pinned-followed-source-inherits-namespace-sub-explicit", "source-ref--pinned-followed-source-inherits-namespace-sub-omitted", "source-ref--pinned-tree-replaces-held-consumer-removed", "source-ref--pinned-tree-replaces-held-consumer-replaced", "source-ref--pinned-tree-replaces-held-consumer-different-name", "source-ref--pinned-tree-replaces-held-consumer-different-path", "source-ref--pinned-tree-replaces-held-consumer-different-source-ref", "source-ref--pinned-root-preserves-source", "source-ref--pinned-path-renders-deleted-kustomization", "source-ref--patched-stub-kustomization-keeps-parent-path", "source-ref--pinned-source-discovered-after-consumer-apps-first-flux-system", "source-ref--pinned-source-discovered-after-consumer-apps-first-cluster", "source-ref--pinned-source-discovered-after-consumer-sources-first-flux-system", "source-ref--pinned-source-discovered-after-consumer-sources-first-cluster", "source-ref--pinned-source-discovered-after-consumer-unpinned-no-ref", "source-ref--pinned-source-discovered-after-consumer-unpinned-non-matching", "source-ref--pinned-path-owns-working-tree-releases", "source-ref--pinned-path-ignores-added-discovery-objects"}},
		{name: "source-bootstrap", cases: []string{"source-ref--empty-path-matches-root-explicit", "source-ref--empty-path-matches-root-omitted", "source-ref--missing-non-git-ocirepository", "source-ref--missing-non-git-helmrepository", "source-ref--missing-non-git-bucket"}},
		{name: "source-ignore", cases: []string{"source-ref--ignore-negation-omitted", "source-ref--ignore-negation-explicit", "source-ref--ignore-negation-excluded"}},
		{name: "source-warnings", cases: []string{"source-warnings-aliases-build", "source-warnings-aliases-test", "source-warnings-aliases-get", "source-warnings--bootstrap-submodule-aliases", "source-warnings-unavailable-ref-build", "source-warnings-unavailable-ref-test", "source-warnings-unavailable-ref-get"}},
		{name: "source-diff-pins", cases: []string{"source-ref--path-orig-uses-own-object-store-different-spec", "source-ref--path-orig-uses-own-object-store-identical-spec", "pinned-diff--substitute-from-producer", "pinned-diff--deleted-substitute-from-producer", "pinned-diff--dirty-transitive-substitute-from", "pinned-diff--generator-inputs-secret-env", "pinned-diff--generator-inputs-configmap-file", "pinned-diff--escaped-patch-input", "supplied-git-repository", "pinned-diff--root-pin"}},
		{name: "substitute-sops", cases: []string{"bootstrap-generator-sops-substitution", "generator-sops-document-hash"}},
		{name: "source-membership", cases: []string{"discovery-index--scoped-raw-file-fallback", "discovery-index--changed-only-file-backed-producers-plain", "discovery-index--changed-only-file-backed-producers-list"}},
		{name: "artifact-reach", cases: []string{"discovery-index--changed-only-generator-inputs-secret-env", "discovery-index--changed-only-generator-inputs-configmap-file"}},
		{name: "manifest-failures", cases: []string{"loader-incidental-parse-failure-sibling", "loader-incidental-parse-failure-component", "kustomization-parse-failure"}},
		{name: "artifact-copy", cases: []string{"bootstrap-generator-failure-isolation-missing", "bootstrap-generator-failure-isolation-outside"}},
		{name: "chart-inputs", cases: []string{"source-ref--last-owner-local-chart"}},
		{name: "source-errors", cases: []string{"source-ref--held-error-across-namespaces-external-source", "source-ref--held-error-across-namespaces-working-tree-source", "source-ref--held-error-survives-namespace-inheritance-explicit-source-namespace", "source-ref--held-error-survives-namespace-inheritance-implicit-source-namespace", "source-ref--held-error-survives-namespace-inheritance-pinned-inherited-namespace", "source-ref--held-error-survives-namespace-inheritance-external-source-in-other-inherited-namespace", "source-ref--held-error-survives-namespace-inheritance-pinned-namespace-free-source", "source-ref--held-error-retained-by-empty-path", "source-ref--held-error-scope-oci", "source-ref--held-error-scope-bootstrap", "source-ref--held-error-scope-resolved-unavailable-tag", "source-ref--held-error-scope-non-matching-url", "source-ref--held-error-scope-two-held", "source-ref--held-error-scope-two-held-path-order"}},
		{name: "machine-auth", cases: []string{"docker-native-store", "helm-native-store", "anonymous-registry", "malformed-native-registry-store", "aws-native-profile", "missing-certificate", "missing-proxy"}},
		{name: "remote-inputs", cases: []string{"git-source"}},
		{name: "cache-lifecycle", cases: []string{"source-prime-and-warm"}},
	} {
		for _, name := range scenario.cases {
			t.Run(scenario.name+"/"+name, func(t *testing.T) {
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
	}
}
