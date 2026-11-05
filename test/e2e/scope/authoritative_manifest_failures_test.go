package scope_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_AuthoritativeManifestFailures(t *testing.T) {
	for _, name := range []string{"typed-release", "typed-kustomization", "generated-release", "patch-created-release", "required-syntax", "repaired-raw-release", "incidental-malformed-and-scalar"} {
		t.Run(name, func(t *testing.T) { testScopeCase(t, "scope-authority", name) })
	}
}
