package scope_test

import "testing"

//nolint:paralleltest // CLI invocations change process logging, environment, directories and transports.
func TestE2E_ExternalFileHelmRelease(t *testing.T) {
	for _, name := range []string{"test-cold", "test-warm", "build-cold", "build-warm", "diff-cold", "diff-warm"} {
		t.Run(name, func(t *testing.T) {
			testScopeCase(t, "external-file-helmrelease", name)
		})
	}
}
