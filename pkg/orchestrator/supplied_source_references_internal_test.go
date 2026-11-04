package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedSourceReferences(t *testing.T) {
	t.Helper()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindBucket, manifest.KindHelmRepository, manifest.KindHelmChart} {
		for _, mode := range []string{"full", "changed", "selected all", "selected reference"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				testSuppliedSourceReference(t, kind, mode)
			})
		}
	}
}
