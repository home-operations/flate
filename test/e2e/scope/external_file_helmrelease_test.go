package scope_test

import "testing"

//nolint:paralleltest // CLI invocations change process logging, environment, directories and transports.
func TestE2E_ExternalFileHelmRelease(t *testing.T) {
	testScopeCases(t, "external-file-helmrelease", []string{
		"test-cold", "test-build-warm", "build-cold", "diff-cold", "diff-warm",
	})
}
