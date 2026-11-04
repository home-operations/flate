package orchestrator

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func testSuppliedApplications(t *testing.T) {
	t.Helper()
	for _, kind := range []string{manifest.KindKustomization, "FluxInstance"} {
		for _, restricted := range []bool{false, true} {
			for _, covering := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/restricted_%t/covering_%t", kind, restricted, covering), func(t *testing.T) {
					testSuppliedApplication(t, kind, restricted, covering)
				})
			}
		}
	}
}
