package crds_test

import "testing"

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ControllerMetadataOwnership(t *testing.T) {
	runCRDScenario(t, "resourceset-documents", "controller-metadata-ownership")
}
