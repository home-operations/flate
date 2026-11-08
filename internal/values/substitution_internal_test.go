package values

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSubstitutionNames_UnknownNode(t *testing.T) {
	t.Parallel()
	assert.FatalIf(
		t,
		collectSubstitutionNames(nil, make(map[string]struct{})),
		"%s",
		"unknown node proved no variable dependencies",
	)
}
