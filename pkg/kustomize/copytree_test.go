package kustomize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSkipStageDir locks the directory exclusion set walkSourceFiles applies
// when copying a git base into a render's fs: node_modules and dot-prefixed
// dirs (.git, .flate-cache, IDE state).
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
		if got := skipStageDir(c.base); got != c.want {
			t.Errorf("skipStageDir(%q) = %v; want %v", c.base, got, c.want)
		}
	}
}

// collectWalk runs walkSourceFiles over root and returns rel -> contents.
func collectWalk(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	if err := walkSourceFiles(root, func(rel string, body []byte) error {
		got[rel] = string(body)
		return nil
	}); err != nil {
		t.Fatalf("walkSourceFiles: %v", err)
	}
	return got
}

// TestWalkSourceFiles_SkipsBrokenSymlink locks the fix for
// m00nwtchr/homelab-cluster's `.pre-commit-config.yaml` regression: a dangling
// symlink (editor lockfiles, gitignored CI configs, IDE caches pointing at
// machine-local paths) must not abort the walk used to copy a git base into the
// render fs. (Source trees aren't walked at all now — they're read lazily via
// the overlay — but git-base copies still use this path.)
func TestWalkSourceFiles_SkipsBrokenSymlink(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "kustomization.yaml"), []byte("resources: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/nonexistent/.pre-commit-config.yaml", filepath.Join(src, ".pre-commit-config.yaml")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	got := collectWalk(t, src)
	if _, ok := got["kustomization.yaml"]; !ok {
		t.Error("kustomization.yaml missing from walk")
	}
	if _, ok := got[".pre-commit-config.yaml"]; ok {
		t.Error("broken symlink should not appear in walk")
	}
}

// TestWalkSourceFiles_FollowsLiveSymlink confirms a symlink resolving to a real
// file is dereferenced — the skip applies only to the dangling case.
func TestWalkSourceFiles_FollowsLiveSymlink(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "real.yaml"), []byte("kind: ConfigMap\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(src, "real.yaml"), filepath.Join(src, "alias.yaml")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if got := collectWalk(t, src)["alias.yaml"]; got != "kind: ConfigMap\n" {
		t.Errorf("symlink target lost; got %q", got)
	}
}

// TestWalkSourceFiles_RejectsSymlinkEscape is the #741 regression: a remote git
// base is untrusted, so a symlink whose target escapes the base root (e.g.
// creds.yaml -> /proc/self/environ) must NOT be read through — its host bytes
// must never reach the render fs. In-root symlinks and regular files are
// unaffected.
func TestWalkSourceFiles_RejectsSymlinkEscape(t *testing.T) {
	// An outside-root "host file" with distinctive bytes.
	outside := t.TempDir()
	const sentinel = "SECRET-HOST-BYTES\n"
	hostFile := filepath.Join(outside, "host-secret.yaml")
	if err := os.WriteFile(hostFile, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "real.yaml"), []byte("kind: ConfigMap\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// In-tree symlink — must still be dereferenced (confine, not skip-all).
	if err := os.Symlink(filepath.Join(src, "real.yaml"), filepath.Join(src, "alias.yaml")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	// Escaping symlink — must be dropped.
	if err := os.Symlink(hostFile, filepath.Join(src, "creds.yaml")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	got := collectWalk(t, src)
	if got["real.yaml"] != "kind: ConfigMap\n" {
		t.Errorf("regular file missing/wrong: %q", got["real.yaml"])
	}
	if got["alias.yaml"] != "kind: ConfigMap\n" {
		t.Errorf("in-tree symlink should still be dereferenced; got %q", got["alias.yaml"])
	}
	if body, ok := got["creds.yaml"]; ok {
		t.Errorf("symlink escaping the base root must be skipped; got %q", body)
	}
	for rel, body := range got {
		if strings.Contains(body, sentinel) {
			t.Fatalf("host-file bytes exfiltrated via %q: %q", rel, body)
		}
	}
}
