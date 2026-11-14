package helm_test

import (
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // The CLI harness changes process-global logging and environment.
func TestE2E_UnavailableValues(t *testing.T) {
	for _, name := range []string{
		"combined-secret", "combined-configmap", "strict-encryption", "wrong-kind-permission",
		"supply", "configmap-supply", "produced-allowed", "produced-strict", "missing-key-allowed", "missing-key-strict",
		"missing-key-supply", "requested-key-isolation",
	} {
		t.Run(name, func(t *testing.T) {
			fixture := harness.Open(t, "unavailable-values", name)
			want := fixture.Want(t)
			for range 2 {
				got := fixture.Run(t)
				if len(got) != len(want) {
					t.Fatalf("sequence length: want %d, got %d", len(want), len(got))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Errorf("result: want %#v, got %#v", want[i], got[i])
					}
				}
			}
		})
	}
}
