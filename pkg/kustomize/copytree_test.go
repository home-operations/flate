package kustomize

import "testing"

// TestSkipStageDir locks the shared staging exclusion set. It is the
// single source of truth for both the staged copy (copyTreeInto) and the
// working-tree fingerprint that keys the persistent stage cache; the two
// MUST agree, so this guards the contract in one place.
func TestSkipStageDir(t *testing.T) {
	cases := []struct {
		base string
		want bool
	}{
		{"node_modules", true},
		{".git", true},
		{".flate-cache", true},
		{".vscode", true},
		{".", true}, // dot-prefixed; callers exclude the root before calling
		{"apps", false},
		{"node_modulesx", false}, // only an exact match is excluded
		{"my.app", false},        // dot must be the leading char
		{"", false},
	}
	for _, c := range cases {
		if got := SkipStageDir(c.base); got != c.want {
			t.Errorf("SkipStageDir(%q) = %v; want %v", c.base, got, c.want)
		}
	}
}
