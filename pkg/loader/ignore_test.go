package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
		if got := ig.matches(abs, root, false); got != tc.wantHit {
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

	if !ig.matches(abs("apps/foo.yaml"), root, false) {
		t.Error("apps/foo.yaml should match apps/*.yaml")
	}
	if ig.matches(abs("apps/sub/foo.yaml"), root, false) {
		t.Error("apps/sub/foo.yaml must NOT match single-level apps/*.yaml")
	}
	if ig.matches(abs("other/foo.yaml"), root, false) {
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
	if ig.matches(filepath.Join(root, "anything.yaml"), root, false) {
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
	if !ig.matches(filepath.Join(root, "apps", "junk", "x.yaml"), root, false) {
		t.Error("apps/junk/x.yaml should match after blank lines and comments are stripped")
	}
}

// TestLoadIgnore_DirOnlyPattern verifies that trailing-slash patterns in
// .krmignore (e.g. "tmp/") — which the gitignore parser marks as dirOnly —
// only fire when isDir=true for the directory itself. This is the regression
// test for the bug where ignoreSet.matches always passed false to the gitignore
// Matcher, causing dirOnly patterns to silently never fire against directory
// paths (shouldSkipDir never pruned those directories during walks).
//
// gitignore semantics for "tmp/":
//   - the directory "tmp" itself matches only when isDir=true
//   - files *inside* tmp/ (e.g. tmp/file.yaml) also match because the "tmp"
//     segment matches at a non-terminal position in the path — this is standard
//     gitignore behaviour (everything under an ignored dir is also ignored)
func TestLoadIgnore_DirOnlyPattern(t *testing.T) {
	root := t.TempDir()
	// "tmp/" — trailing slash means "only match directories named tmp"
	testutil.WriteFile(t, root, ".krmignore", "tmp/\n")

	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore: %v", err)
	}

	tmpDir := filepath.Join(root, "tmp")
	tmpFile := filepath.Join(root, "tmp", "file.yaml")
	tmpBare := filepath.Join(root, "tmp.yaml") // file at root named "tmp.yaml" — must NOT match
	otherDir := filepath.Join(root, "other")

	// isDir=true: the pattern is dir-only, must match the tmp directory itself.
	if !ig.matches(tmpDir, root, true) {
		t.Error("tmp/ pattern should match the 'tmp' directory (isDir=true)")
	}
	// isDir=false for the bare name "tmp" at the root level: the dir-only
	// guard fires here (segment is the terminal one), so a file named "tmp"
	// must NOT match.
	if ig.matches(tmpDir, root, false) {
		t.Error("tmp/ pattern must NOT match a file named 'tmp' at root level (isDir=false, terminal segment)")
	}
	// Files inside tmp/ also match — "tmp" appears as a non-terminal segment
	// so dirOnly does not block it; this is standard gitignore semantics.
	if !ig.matches(tmpFile, root, false) {
		t.Error("tmp/ pattern should match files inside 'tmp/' (non-terminal segment)")
	}
	// A file called "tmp.yaml" at root level must not match.
	if ig.matches(tmpBare, root, false) {
		t.Error("tmp/ pattern must NOT match 'tmp.yaml' (different name)")
	}
	// Unrelated directory must not match.
	if ig.matches(otherDir, root, true) {
		t.Error("tmp/ pattern must NOT match an unrelated directory")
	}
}

// TestWalkerIgnoreMatches_MemoMatchesUncached asserts the walker's
// memoized ignoreMatches returns identical results to the raw
// ignoreSet.matches across hit and miss. The contract is "same input
// → same output, but cheaper" — the cache must never flip a result.
func TestWalkerIgnoreMatches_MemoMatchesUncached(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, ".krmignore", "apps/junk/**\ntmp/\n")
	ig, err := loadIgnore(root)
	if err != nil {
		t.Fatalf("loadIgnore: %v", err)
	}
	w := walker{
		ignore:      ig,
		scanRoot:    root,
		visited:     map[string]struct{}{},
		ignoreCache: map[ignoreKey]bool{},
	}

	cases := []struct {
		rel   string
		isDir bool
	}{
		{"apps/junk/file.yaml", false},
		{"apps/junk/sub/file.yaml", false},
		{"apps/other/file.yaml", false},
		{"tmp", true},
		{"tmp", false}, // same path, different isDir → distinct cache entry
		{"unrelated/path.yaml", false},
	}
	for _, tc := range cases {
		abs := filepath.Join(root, filepath.FromSlash(tc.rel))
		want := ig.matches(abs, root, tc.isDir)
		// First call — miss.
		if got := w.ignoreMatches(abs, tc.isDir); got != want {
			t.Errorf("ignoreMatches(%q, isDir=%v) miss = %v, want %v", tc.rel, tc.isDir, got, want)
		}
		// Second call — hit. Result must match.
		if got := w.ignoreMatches(abs, tc.isDir); got != want {
			t.Errorf("ignoreMatches(%q, isDir=%v) hit = %v, want %v", tc.rel, tc.isDir, got, want)
		}
	}

	// Sanity: every input populated a cache entry.
	if got, want := len(w.ignoreCache), len(cases); got != want {
		t.Errorf("ignoreCache size after %d distinct lookups = %d, want %d", want, got, want)
	}
}

// TestWalkerIgnoreMatches_NilMatcherShortCircuits asserts the cache
// isn't populated when the ignore set is empty — every call returns
// false immediately and a cluster-sized walk shouldn't grow a map
// for free.
func TestWalkerIgnoreMatches_NilMatcherShortCircuits(t *testing.T) {
	w := walker{
		ignore:      &ignoreSet{}, // no matcher
		scanRoot:    "/tmp",
		ignoreCache: map[ignoreKey]bool{},
	}
	if got := w.ignoreMatches("/tmp/x.yaml", false); got {
		t.Errorf("nil matcher should always return false; got true")
	}
	if len(w.ignoreCache) != 0 {
		t.Errorf("nil matcher must not populate cache; got %d entries", len(w.ignoreCache))
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

// TestLoader_KrmignoreEnvironmentAllowlist mirrors the multi-environment
// layout from issue #816: environments/<env>/ trees carry same-named
// resources, so an unfiltered scan lets the alphabetically-last env
// shadow the others in the Store. A .krmignore using the exact
// allowlist patterns from the issue (the ones the reporter feeds to
// GitRepository spec.ignore) must scope the walk to common/ plus one
// environment, so the selected env's resources win.
func TestLoader_KrmignoreEnvironmentAllowlist(t *testing.T) {
	appID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}
	commonID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "common-settings"}

	writeTree := func(t *testing.T, ignore string) *store.Store {
		t.Helper()
		dir := t.TempDir()
		testutil.WriteFile(t, dir, ".krmignore", ignore)
		testutil.WriteFile(t, dir, "common/settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: common-settings, namespace: default}
`)
		for _, env := range []string{"production", "staging"} {
			testutil.WriteFile(t, dir, "environments/"+env+"/app.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: app-config, namespace: default}
data: {env: `+env+`}
`)
		}
		s := store.New()
		if _, err := New(s).Load(t.Context(), dir); err != nil {
			t.Fatalf("Load: %v", err)
		}
		return s
	}

	envOf := func(t *testing.T, s *store.Store) string {
		t.Helper()
		obj := s.GetObject(appID)
		if obj == nil {
			t.Fatal("app-config was not loaded at all")
		}
		cm, ok := obj.(*manifest.ConfigMap)
		if !ok {
			t.Fatalf("app-config is %T, want *manifest.ConfigMap", obj)
		}
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("no filtering: staging shadows production", func(t *testing.T) {
		s := writeTree(t, "")
		if got := envOf(t, s); got != "staging" {
			t.Errorf("unfiltered walk: app-config env = %q, want %q (alphabetically-last env wins)", got, "staging")
		}
	})

	t.Run("issue #816 allowlist patterns select production", func(t *testing.T) {
		s := writeTree(t, `/*
!/common/**/*.yaml
!/environments/production/**/*.yaml
`)
		if got := envOf(t, s); got != "production" {
			t.Errorf("allowlist walk: app-config env = %q, want %q", got, "production")
		}
		if s.GetObject(commonID) == nil {
			t.Error("common-settings must be re-included by !/common/**/*.yaml")
		}
	})

	t.Run("directory exclusion selects production", func(t *testing.T) {
		s := writeTree(t, "/environments/staging/\n")
		if got := envOf(t, s); got != "production" {
			t.Errorf("exclusion walk: app-config env = %q, want %q", got, "production")
		}
		if s.GetObject(commonID) == nil {
			t.Error("common-settings must still load")
		}
	})
}

// TestLoader_IgnoreFileOverride covers Loader.IgnoreFile: an explicitly
// named file replaces <root>/.krmignore, its patterns anchor at the scan
// root regardless of where the file lives, and naming a file that does
// not exist fails loud instead of silently scanning unfiltered.
func TestLoader_IgnoreFileOverride(t *testing.T) {
	appID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}

	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".krmignore", "/environments/staging/\n")
	testutil.WriteFile(t, dir, "ignores/production.krmignore", "/environments/production/\n")
	for _, env := range []string{"production", "staging"} {
		testutil.WriteFile(t, dir, "environments/"+env+"/app.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: app-config, namespace: default}
data: {env: `+env+`}
`)
	}

	envOf := func(t *testing.T, ignoreFile string) string {
		t.Helper()
		s := store.New()
		l := New(s)
		l.IgnoreFile = ignoreFile
		if _, err := l.Load(t.Context(), dir); err != nil {
			t.Fatalf("Load: %v", err)
		}
		cm, ok := s.GetObject(appID).(*manifest.ConfigMap)
		if !ok {
			t.Fatal("app-config was not loaded")
		}
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("empty falls back to root .krmignore", func(t *testing.T) {
		if got := envOf(t, ""); got != "production" {
			t.Errorf("env = %q, want production", got)
		}
	})

	t.Run("override anchors at the scan root", func(t *testing.T) {
		if got := envOf(t, filepath.Join(dir, "ignores", "production.krmignore")); got != "staging" {
			t.Errorf("env = %q, want staging", got)
		}
	})

	t.Run("missing override file errors", func(t *testing.T) {
		l := New(store.New())
		l.IgnoreFile = filepath.Join(dir, "nope.krmignore")
		if _, err := l.Load(t.Context(), dir); err == nil {
			t.Fatal("Load: expected error for missing IgnoreFile")
		}
	})
}
