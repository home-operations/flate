//go:build !race

package manifest

import (
	"errors"
	"testing"
)

func TestInputf_Allocations(t *testing.T) {
	var err error
	allocations := testing.AllocsPerRun(100, func() {
		err = inputf("missing %s", "kind")
	})
	if allocations != 2 {
		t.Fatalf("input error allocates %v objects, want 2", allocations)
	}
	if !errors.Is(err, ErrInput) || err.Error() != "missing kind" {
		t.Fatalf("input error = %v", err)
	}
}
