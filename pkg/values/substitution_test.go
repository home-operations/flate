package values

import "testing"

func TestSubstitutionNames_UnknownNode(t *testing.T) {
	t.Parallel()
	if collectSubstitutionNames(nil, make(map[string]struct{})) {
		t.Fatal("unknown node proved no variable dependencies")
	}
}
