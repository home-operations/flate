package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// TestLoadIgnore_GlobStarMatchesAcrossDirectories is the regression test for
// the original bug: filepath.Match does not expand ** across separators, so
// patterns like "apps/junk/**" silently never matched files inside
// apps/junk/sub/file.yaml.  After the fix, the gitignore-based matcher
// handles ** correctly.
func TestLoadIgnore_GlobStarMatchesAcrossDirectories(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, ".krmignore", "apps/junk/**\n")

	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore: %v", err)
	}

	cases := []struct {
		rel     string
		wantHit bool
	}{
		// Direct child — must match.
		{"apps/junk/file.yaml", true},
		// Nested several levels deep — the original bug: ** must cross /
		{"apps/junk/sub/file.yaml", true},
		{"apps/junk/a/b/c/deep.yaml", true},
		// Outside the ignored tree — must NOT match.
		{"apps/other/file.yaml", false},
		{"other/junk/file.yaml", false},
	}
	for _, tc := range cases {
		abs := filepath.Join(root, filepath.FromSlash(tc.rel))
		if got := ig.matches(abs, root); got != tc.wantHit {
			t.Errorf("matches(%q) = %v, want %v", tc.rel, got, tc.wantHit)
		}
	}
}

// TestLoadIgnore_SingleGlobPattern confirms that single-level wildcard
// patterns (e.g. "apps/*.yaml") still work after the matcher change.
func TestLoadIgnore_SingleGlobPattern(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, ".krmignore", "apps/*.yaml\n")

	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore: %v", err)
	}

	abs := func(rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }

	if !ig.matches(abs("apps/foo.yaml"), root) {
		t.Error("apps/foo.yaml should match apps/*.yaml")
	}
	if ig.matches(abs("apps/sub/foo.yaml"), root) {
		t.Error("apps/sub/foo.yaml must NOT match single-level apps/*.yaml")
	}
	if ig.matches(abs("other/foo.yaml"), root) {
		t.Error("other/foo.yaml must NOT match apps/*.yaml")
	}
}

// TestLoadIgnore_NoFile confirms that a missing .krmignore returns a
// no-op set without error.
func TestLoadIgnore_NoFile(t *testing.T) {
	root := t.TempDir()
	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore on missing file: %v", err)
	}
	// Should match nothing.
	if ig.matches(filepath.Join(root, "anything.yaml"), root) {
		t.Error("empty ignore set should not match anything")
	}
}

// TestLoadIgnore_CommentsAndBlankLines verifies the parser skips
// comment lines and blank lines without error.
func TestLoadIgnore_CommentsAndBlankLines(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, ".krmignore", `
# this is a comment
apps/junk/**

# another comment
`)

	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore: %v", err)
	}
	if !ig.matches(filepath.Join(root, "apps", "junk", "x.yaml"), root) {
		t.Error("apps/junk/x.yaml should match after blank lines and comments are stripped")
	}
}

// TestNormalizePrefix_LivesInParent is a compile-time + behavioural check
// that NormalizePrefix is accessible from the parent.go scope (i.e. the
// move didn't accidentally make it unexported or shadow it).
func TestNormalizePrefix_LivesInParent(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"./apps/plex", "apps/plex/"},
		{"apps/plex/", "apps/plex/"},
		{"apps/plex", "apps/plex/"},
	}
	for _, tc := range cases {
		if got := NormalizePrefix(tc.in); got != tc.want {
			t.Errorf("NormalizePrefix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
