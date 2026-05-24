package kustomize

import (
	"os"
	"path/filepath"
	"testing"
)

// TestStagingCache_CopyTree_SkipsBrokenSymlink locks the fix for
// m00nwtchr/homelab-cluster's `.pre-commit-config.yaml` regression: a
// dangling symlink in the user's working tree (common for editor
// lockfiles, gitignored CI configs, IDE caches that point at
// machine-local paths) used to abort the entire stage with
// "stat <path>: no such file or directory". Flux's reconcile would
// happily skip the same link in-cluster; flate now matches that.
func TestStagingCache_CopyTree_SkipsBrokenSymlink(t *testing.T) {
	src := t.TempDir()

	// One real file Flux cares about.
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	// One dangling symlink at the root — the exact shape m00nwtchr's
	// .pre-commit-config.yaml landed as in their local checkout.
	if err := os.Symlink("/nonexistent/.pre-commit-config.yaml",
		filepath.Join(src, ".pre-commit-config.yaml")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(src)
	if err != nil {
		t.Fatalf("Stage should ignore broken symlinks; got %v", err)
	}

	// The real file made it through.
	if _, err := os.Stat(filepath.Join(staged, "kustomization.yaml")); err != nil {
		t.Errorf("kustomization.yaml missing from stage: %v", err)
	}
	// The broken symlink did NOT get copied (good — we'd just propagate
	// the dangling reference into the stage).
	if _, err := os.Lstat(filepath.Join(staged, ".pre-commit-config.yaml")); err == nil {
		t.Error("broken symlink should not appear in stage")
	}
}

// TestStagingCache_CopyTree_FollowsLiveSymlink confirms we still
// follow symlinks that resolve to real files — the skip applies only
// to the "target doesn't exist" arm.
func TestStagingCache_CopyTree_FollowsLiveSymlink(t *testing.T) {
	src := t.TempDir()
	target := filepath.Join(src, "real.yaml")
	mustWrite(t, target, "kind: ConfigMap\n")
	if err := os.Symlink(target, filepath.Join(src, "alias.yaml")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(src)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(staged, "alias.yaml")) //nolint:gosec // staged is t.TempDir
	if err != nil {
		t.Fatalf("read alias: %v", err)
	}
	if string(got) != "kind: ConfigMap\n" {
		t.Errorf("symlink target lost; got %q", got)
	}
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
