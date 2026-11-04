package manifest

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func BenchmarkInputf(b *testing.B) {
	var err error
	b.ReportAllocs()
	for b.Loop() {
		err = inputf("missing %s", "kind")
	}
	assert.FatalIf(b, !errors.Is(err, ErrInput) || err.Error() != "missing kind", "input error = %v", err)
}
