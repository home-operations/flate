package kustomize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
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

// TestStagingCache_FetchRemote_CancelDoesNotPoisonCache pins the
// fix for ctx-capture poisoning: the previous implementation wrapped
// the fetch in sync.OnceValues with the first caller's ctx, so a
// cancel on caller A froze context.Canceled into the cached error for
// every subsequent FetchRemote of the same URL — even with healthy
// ctxs. The new implementation detaches the fetch's own ctx from
// callers; only the per-call select on rf.done vs ctx.Done() respects
// the caller's cancellation.
func TestStagingCache_FetchRemote_CancelDoesNotPoisonCache(t *testing.T) {
	// Slow server: holds requests until the harness signals release.
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("body: ok\n"))
	}))
	t.Cleanup(srv.Close)

	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	// Caller A starts a fetch with a ctx we'll cancel.
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() {
		_, err := cache.FetchRemote(ctxA, srv.URL+"/x.yaml")
		doneA <- err
	}()

	// Wait until the server has at least one in-flight request, then
	// cancel A. Give the goroutine a chance to actually call select.
	for hits.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancelA()
	if err := <-doneA; err == nil {
		t.Fatal("caller A should have seen ctx.Err()")
	}

	// Caller B with a healthy ctx must NOT see context.Canceled. The
	// fetch is still running (release not signaled); B should observe
	// it complete normally. Release the server now so B can finish.
	close(release)
	body, err := cache.FetchRemote(context.Background(), srv.URL+"/x.yaml")
	if err != nil {
		t.Errorf("caller B got poisoned error: %v", err)
	}
	if string(body) != "body: ok\n" {
		t.Errorf("caller B got wrong body: %q", body)
	}

	// And the dedup invariant: at most one server hit for the same URL
	// despite two callers.
	if got := hits.Load(); got != 1 {
		t.Errorf("server hit %d times; want 1 (singleflight broken)", got)
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

// TestNewStagingCache_SweepsStaleLeftovers pins the crash-cleanup
// sweep: any `flate-stage-*` directory under the parent older than
// staleStageAge is removed when a fresh StagingCache opens. Without
// this, a process killed mid-stage (SIGKILL, panic outside Close)
// leaks the staged tree forever.
func TestNewStagingCache_SweepsStaleLeftovers(t *testing.T) {
	parent := t.TempDir()

	// Lay down a "leftover" stage dir with a forced-old mtime.
	stale := filepath.Join(parent, "flate-stage-stale12345")
	if err := os.MkdirAll(stale, 0o750); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(stale, "marker"), "leftover")
	old := time.Now().Add(-2 * staleStageAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	// And a fresh-looking one that must SURVIVE the sweep.
	fresh := filepath.Join(parent, "flate-stage-fresh67890")
	if err := os.MkdirAll(fresh, 0o750); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(fresh, "marker"), "fresh")

	// And an unrelated dir with the same prefix but not ours.
	other := filepath.Join(parent, "some-other-tmp")
	if err := os.MkdirAll(other, 0o750); err != nil {
		t.Fatal(err)
	}
	oldOther := time.Now().Add(-2 * staleStageAge)
	if err := os.Chtimes(other, oldOther, oldOther); err != nil {
		t.Fatal(err)
	}

	if _, err := NewStagingCache(parent); err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale flate-stage dir survived the sweep: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh flate-stage dir was reaped (mtime cutoff broken): %v", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Errorf("non-flate dir was reaped (prefix check broken): %v", err)
	}
}

// TestStagingCache_HardlinksWhenSameFilesystem confirms the perf win:
// staged files share an inode with their source when the source and
// the cache tempdir sit on the same filesystem (the common case). A
// matching inode number on both sides proves we're not paying for a
// full-tree byte copy on every render.
func TestStagingCache_HardlinksWhenSameFilesystem(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	srcFile := filepath.Join(src, "kustomization.yaml")
	mustWrite(t, srcFile, "resources: []\n")

	cache, err := NewStagingCache(filepath.Join(root, "stage"))
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(src)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	stagedFile := filepath.Join(staged, "kustomization.yaml")
	si, err := os.Stat(srcFile)
	if err != nil {
		t.Fatal(err)
	}
	di, err := os.Stat(stagedFile)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(si, di) {
		t.Errorf("expected staged file to share an inode with source (hardlink); same-file check failed")
	}
}

// TestRestoreKustomization_DoesNotMutateSource is the safety net for
// hardlink staging: even though the stage's kustomization.yaml may
// share an inode with the source, restoreKustomizationFile must
// os.Remove the staged link before WriteFile so the source's bytes
// stay intact across renders.
func TestRestoreKustomization_DoesNotMutateSource(t *testing.T) {
	src := t.TempDir()
	srcKust := filepath.Join(src, "kustomization.yaml")
	mustWrite(t, srcKust, "original\n")

	cache, err := NewStagingCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(src)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	// Pretend the Generator wrote over the staged kustomization. If
	// the staged file is still hard-linked to the source, this writes
	// to the source's inode and corrupts it.
	if err := restoreKustomizationFile(src, staged, ""); err != nil {
		t.Fatalf("restoreKustomizationFile: %v", err)
	}
	// Overwrite the staged file simulating Generator output.
	if err := os.WriteFile(filepath.Join(staged, "kustomization.yaml"), []byte("rewritten\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(srcKust) //nolint:gosec // srcKust under t.TempDir
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original\n" {
		t.Errorf("source kustomization.yaml mutated by stage write: got %q", got)
	}
}

// TestIsHTTPClientError pins the 4xx-detection contract: only HTTP
// 4xx responses (definitive client errors) return true; 5xx, transport
// errors and nil all return false. Uses the httpStatusError sentinel
// directly (so the test is independent of the string format) and also
// verifies that wrapping via fmt.Errorf still classifies correctly —
// the whole point of the typed sentinel over string parsing.
func TestIsHTTPClientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"400", &httpStatusError{Code: 400}, true},
		{"404", &httpStatusError{Code: 404}, true},
		{"418", &httpStatusError{Code: 418}, true},
		{"499", &httpStatusError{Code: 499}, true},
		{"500 transient", &httpStatusError{Code: 500}, false},
		{"200 never-client-error", &httpStatusError{Code: 200}, false},
		{"wrapped 404", fmt.Errorf("preflight: %w", &httpStatusError{Code: 404}), true},
		{"wrapped 500", fmt.Errorf("preflight: %w", &httpStatusError{Code: 500}), false},
		{"transport error", fmt.Errorf("connection refused"), false},
		{"deadline", fmt.Errorf("context deadline exceeded"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		got := isHTTPClientError(tc.err)
		if got != tc.want {
			t.Errorf("isHTTPClientError(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
