package safepath_test

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/source/safepath"
)

func TestSafeJoin_Paths(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	for _, tc := range []struct {
		name, rel, want string
	}{
		{name: "nested_relative", rel: "templates/cm.yaml", want: "templates/cm.yaml"},
		{name: "normalized_relative", rel: "foo/../bar.txt", want: "bar.txt"},
		{name: "absolute", rel: "/etc/passwd"},
		{name: "escape", rel: "foo/../../escape"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := safepath.SafeJoin(base, tc.rel)
			if tc.want == "" {
				if err == nil || !strings.Contains(err.Error(), tc.rel) {
					t.Fatalf("SafeJoin(%q) = %q, %v; want rejection naming the path", tc.rel, got, err)
				}
				return
			}
			if want := filepath.Join(base, tc.want); err != nil || got != want {
				t.Fatalf("SafeJoin(%q) = %q, %v; want %q", tc.rel, got, err, want)
			}
		})
	}
}

func TestArchiveName_Validation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, entry, want string
	}{
		{name: "empty", entry: ""},
		{name: "traversal_before_cleaning", entry: "a/../x"},
		{name: "absolute", entry: "/x"},
		{name: "drive_after_prefix", entry: "./C:/x"},
		{name: "backslash", entry: `a\x`},
		{name: "nul", entry: "a\x00x"},
		{name: "normalized", entry: "././dir/notes:@ space 雪.yaml", want: "dir/notes:@ space 雪.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := safepath.ArchiveName(tc.entry)
			if tc.want == "" {
				if !errors.Is(err, fs.ErrInvalid) {
					t.Fatalf("ArchiveName(%q) = %q, %v; want invalid name", tc.entry, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ArchiveName(%q) = %q, %v; want %q", tc.entry, got, err, tc.want)
			}
		})
	}
}

// TestContains exercises the pure containment check the kustomize remote-base
// copy uses to confine a symlink's resolved target to the base root. Both args
// are absolute, symlink-resolved paths; only the algebraic check is tested.
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
