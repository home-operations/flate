package safepath_test

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/source/safepath"
)

func TestContains(t *testing.T) {
	t.Parallel()
	root := filepath.Join(string(filepath.Separator), "base", "root")
	cases := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "file directly inside", target: filepath.Join(root, "a.yaml"), want: true},
		{name: "nested inside", target: filepath.Join(root, "sub", "a.yaml"), want: true},
		{name: "root itself", target: root, want: true},
		{name: "parent escape", target: filepath.Join(root, "..", "secret"), want: false},
		{name: "absolute outside", target: filepath.Join(string(filepath.Separator), "etc", "passwd"), want: false},
		// /base/rootx must NOT count as inside /base/root (prefix-boundary case).
		{name: "sibling prefix not contained", target: root + "x" + string(filepath.Separator) + "a.yaml", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := safepath.Contains(root, tc.target); got != tc.want {
				t.Errorf("Contains(%q, %q) = %v; want %v", root, tc.target, got, tc.want)
			}
		})
	}
}
