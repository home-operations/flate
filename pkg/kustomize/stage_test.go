package kustomize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

	cache, err := NewStagingCache(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(context.Background(), src, "")
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

	cache, err := NewStagingCache(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(context.Background(), src, "")
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

	cache, err := NewStagingCache(t.TempDir(), 0)
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

	if _, err := NewStagingCache(parent, 0); err != nil {
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

	cache, err := NewStagingCache(filepath.Join(root, "stage"), 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(context.Background(), src, "")
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

	cache, err := NewStagingCache(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	staged, err := cache.Stage(context.Background(), src, "")
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

// TestStagingCache_PersistentSentinelWritten pins Phase 3.4b's
// staging contract: a Stage call with a non-empty fingerprint lands
// at <root>/<fp[:2]>/<fp>/ and writes the .flate-stage-complete
// sentinel atomically.
func TestStagingCache_PersistentSentinelWritten(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	root := t.TempDir()
	cache, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	const fp = "deadbeefcafef00d000000000000000000000000000000000000000000000000"
	staged, err := cache.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	want := filepath.Join(root, fp[:2], fp)
	if staged != want {
		t.Errorf("staged path = %q, want %q", staged, want)
	}
	if _, err := os.Stat(filepath.Join(staged, stageCompleteSentinel)); err != nil {
		t.Errorf("sentinel missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staged, "kustomization.yaml")); err != nil {
		t.Errorf("kustomization.yaml missing from stage: %v", err)
	}
}

// TestStagingCache_PartialStageRebuildsOnNextRun pins the
// post-rename-pre-sentinel crash recovery contract: if a process
// renames the staging tree into place but crashes before writing the
// .flate-stage-complete sentinel, the next Stage call for the same
// fingerprint MUST rebuild (or otherwise re-emit the sentinel) rather
// than adopt the incomplete tree.
//
// Pre-fix, the sentinel was written into the tmp dir BEFORE rename, so
// any partial-rename or crash-after-sentinel-write would leave the
// final dir with a sentinel inside it — making subsequent runs
// short-circuit on a tree they couldn't trust. Post-fix, the sentinel
// is only written after the rename succeeds, so a crash window leaves
// the dir sentinel-free and the next Stage call rebuilds.
func TestStagingCache_PartialStageRebuildsOnNextRun(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	root := t.TempDir()
	cache1, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache 1: %v", err)
	}

	const fp = "deadbeef11111111000000000000000000000000000000000000000000000000"
	staged, err := cache1.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 1: %v", err)
	}
	sentinel := filepath.Join(staged, stageCompleteSentinel)
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("sentinel missing after initial Stage: %v", err)
	}
	if err := cache1.Close(); err != nil {
		t.Fatalf("cache1.Close: %v", err)
	}

	// Simulate the post-rename-pre-sentinel crash window: the tree is
	// fully renamed into the final slot, but the sentinel was never
	// written (or was lost). A correct implementation must observe
	// "no sentinel" and rebuild on the next Stage call.
	if err := os.Remove(sentinel); err != nil {
		t.Fatalf("simulate crash (remove sentinel): %v", err)
	}

	// Fresh cache instance — the in-process promise from cache1 is gone,
	// so the reattempt has to take the on-disk path.
	cache2, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache 2: %v", err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	staged2, err := cache2.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 2 (after simulated crash): %v", err)
	}
	if staged2 != staged {
		t.Errorf("Stage 2 landed at %q, want %q", staged2, staged)
	}
	// The sentinel MUST be present again — either rebuilt or rewritten.
	if _, err := os.Stat(sentinel); err != nil {
		t.Errorf("sentinel not restored after rebuild: %v", err)
	}
	// And the tree contents survived.
	if _, err := os.Stat(filepath.Join(staged2, "kustomization.yaml")); err != nil {
		t.Errorf("kustomization.yaml missing after rebuild: %v", err)
	}
}

// TestStagingCache_SentinelWrittenAfterRename is the regression fence
// for the post-fix ordering: stage a fingerprint, then assert that the
// final dir exists, contains the sentinel, and has no `.tmp.*`
// siblings lingering in the prefix dir. Together with the partial-stage
// recovery test above, this pins the rename-then-sentinel invariant.
func TestStagingCache_SentinelWrittenAfterRename(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	root := t.TempDir()
	cache, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	const fp = "abcdef9876543210000000000000000000000000000000000000000000000000"
	staged, err := cache.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	// Final dir exists and carries the sentinel.
	if _, err := os.Stat(staged); err != nil {
		t.Fatalf("final dir missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staged, stageCompleteSentinel)); err != nil {
		t.Errorf("sentinel missing from final dir: %v", err)
	}

	// No `.tmp.*` siblings remain in the prefix shard.
	prefixDir := filepath.Dir(staged)
	entries, err := os.ReadDir(prefixDir)
	if err != nil {
		t.Fatalf("read prefix dir: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp.") {
			t.Errorf("leftover staging tmpdir in prefix: %q", e.Name())
		}
	}
}

// TestStagingCache_PersistentSameFingerprintSkipsRebuild proves that
// a second Stage call for the same fingerprint is a no-op: no
// additional copyTree pass fires, and the existing sentinel mtime is
// preserved (a rebuild would re-write the sentinel via atomic
// rename and bump the mtime).
func TestStagingCache_PersistentSameFingerprintSkipsRebuild(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	root := t.TempDir()
	cache, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	const fp = "1111111111111111111111111111111111111111111111111111111111111111"
	staged1, err := cache.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 1: %v", err)
	}
	sentinel := filepath.Join(staged1, stageCompleteSentinel)
	info1, err := os.Stat(sentinel)
	if err != nil {
		t.Fatalf("sentinel stat: %v", err)
	}

	// Force a measurable mtime delta if the second Stage rewrites.
	time.Sleep(20 * time.Millisecond)

	staged2, err := cache.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 2: %v", err)
	}
	if staged1 != staged2 {
		t.Errorf("Stage 2 path = %q, want %q", staged2, staged1)
	}
	info2, err := os.Stat(sentinel)
	if err != nil {
		t.Fatalf("sentinel stat 2: %v", err)
	}
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Errorf("sentinel mtime changed across Stage calls (rebuild happened) — was %v, now %v",
			info1.ModTime(), info2.ModTime())
	}
}

// TestStagingCache_PersistentDistinctFingerprintsDistinctDirs proves
// the namespace partitioning: two source trees hashed to different
// fingerprints get independent staged copies.
func TestStagingCache_PersistentDistinctFingerprintsDistinctDirs(t *testing.T) {
	srcA := t.TempDir()
	mustWrite(t, filepath.Join(srcA, "a.yaml"), "kind: A\n")
	srcB := t.TempDir()
	mustWrite(t, filepath.Join(srcB, "b.yaml"), "kind: B\n")

	root := t.TempDir()
	cache, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	const fpA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const fpB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	stagedA, err := cache.Stage(context.Background(), srcA, fpA)
	if err != nil {
		t.Fatalf("Stage A: %v", err)
	}
	stagedB, err := cache.Stage(context.Background(), srcB, fpB)
	if err != nil {
		t.Fatalf("Stage B: %v", err)
	}
	if stagedA == stagedB {
		t.Fatalf("distinct fingerprints landed at same dir: %q", stagedA)
	}
	if _, err := os.Stat(filepath.Join(stagedA, "a.yaml")); err != nil {
		t.Errorf("A.yaml missing from staged A: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stagedB, "b.yaml")); err != nil {
		t.Errorf("B.yaml missing from staged B: %v", err)
	}
}

// TestStagingCache_CrossProcessReuse stands in for a real
// cross-process scenario: build the stage with one cache instance,
// close it, point a fresh cache at the same root, and observe that
// the second Stage call reuses the existing dir via the sentinel
// without recopying. The proof is the original sentinel mtime
// surviving the second Stage call (a rebuild would atomically
// rename a fresh sibling into place and the mtime would change).
func TestStagingCache_CrossProcessReuse(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "kustomization.yaml"), "resources: []\n")

	root := t.TempDir()
	const fp = "cafebabe00000000000000000000000000000000000000000000000000000000"

	// "Process 1": fresh cache, builds the persistent stage.
	cache1, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache 1: %v", err)
	}
	staged1, err := cache1.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 1: %v", err)
	}
	sentinel := filepath.Join(staged1, stageCompleteSentinel)
	info1, err := os.Stat(sentinel)
	if err != nil {
		t.Fatalf("sentinel stat: %v", err)
	}
	if err := cache1.Close(); err != nil {
		t.Fatalf("cache1.Close: %v", err)
	}
	// Persistent stages MUST survive Close.
	if _, err := os.Stat(staged1); err != nil {
		t.Fatalf("persistent stage removed on Close: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	// "Process 2": fresh cache at the same root; Stage should
	// short-circuit on the sentinel.
	cache2, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache 2: %v", err)
	}
	t.Cleanup(func() { _ = cache2.Close() })

	staged2, err := cache2.Stage(context.Background(), src, fp)
	if err != nil {
		t.Fatalf("Stage 2: %v", err)
	}
	if staged1 != staged2 {
		t.Errorf("process 2 Stage path = %q, want %q", staged2, staged1)
	}
	info2, err := os.Stat(sentinel)
	if err != nil {
		t.Fatalf("sentinel stat 2: %v", err)
	}
	// We allow chtimes to bump the dir mtime (LRU touch), but the
	// SENTINEL file mtime is the rebuild signal — it stays put.
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Errorf("sentinel mtime changed across processes (rebuild happened) — was %v, now %v",
			info1.ModTime(), info2.ModTime())
	}
}

// TestStagingCache_PerProcessFallbackForEmptyFingerprint pins the
// fallback contract: when no fingerprint is supplied (local-path
// sources, the working-tree alias), Stage falls back to per-process
// scratch staging in a `flate-stage-*` tempdir and that tempdir is
// removed on Close.
func TestStagingCache_PerProcessFallbackForEmptyFingerprint(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "k.yaml"), "x: 1\n")

	root := t.TempDir()
	cache, err := NewStagingCache(root, 0)
	if err != nil {
		t.Fatalf("NewStagingCache: %v", err)
	}

	staged, err := cache.Stage(context.Background(), src, "")
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if !strings.Contains(staged, "flate-stage-") {
		t.Errorf("expected per-process flate-stage-* dir; got %q", staged)
	}
	if _, err := os.Stat(filepath.Join(staged, "k.yaml")); err != nil {
		t.Errorf("staged file missing: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Errorf("per-process stage survived Close: %v", err)
	}
}

// TestSweepStageBySize_EvictsOldestUnderLimit pins the LRU contract:
// when total cache size exceeds maxBytes, the sweep removes the
// oldest entries first until size is at or below the cap.
func TestSweepStageBySize_EvictsOldestUnderLimit(t *testing.T) {
	root := t.TempDir()

	// Three fingerprint stages of increasing mtime, each ~1KiB.
	stamps := []time.Time{
		time.Now().Add(-3 * time.Hour),
		time.Now().Add(-2 * time.Hour),
		time.Now().Add(-1 * time.Hour),
	}
	fps := []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	for i, fp := range fps {
		dir := filepath.Join(root, fp[:2], fp)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		// 1 KiB payload so total reliably crosses our 2 KiB cap.
		body := make([]byte, 1024)
		if err := os.WriteFile(filepath.Join(dir, "data"), body, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, stageCompleteSentinel), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(dir, stamps[i], stamps[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := sweepStageBySize(root, 2*1024); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// Oldest (fps[0]) MUST be gone; the two newer ones MUST survive.
	if _, err := os.Stat(filepath.Join(root, fps[0][:2], fps[0])); !os.IsNotExist(err) {
		t.Errorf("oldest stage not evicted: %v", err)
	}
	for _, fp := range fps[1:] {
		if _, err := os.Stat(filepath.Join(root, fp[:2], fp)); err != nil {
			t.Errorf("newer stage %s reaped: %v", fp[:8], err)
		}
	}
}

// BenchmarkStagingCache_PersistentRerun measures the warm-rerun cost
// of staging the same source tree N times with the same fingerprint.
// The persistent CAS path skips copyTree after the first Stage call;
// every subsequent call is essentially a stat + chtimes pair. Before
// Phase 3.4b every iteration paid the full os.Link per file walk.
func BenchmarkStagingCache_PersistentRerun(b *testing.B) {
	src := b.TempDir()
	// Synthesize 200 small files under a handful of subdirs so the
	// copyTree pass has measurable depth + breadth.
	for d := range 10 {
		dir := filepath.Join(src, fmt.Sprintf("d%02d", d))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			b.Fatal(err)
		}
		for f := range 20 {
			path := filepath.Join(dir, fmt.Sprintf("f%02d.yaml", f))
			if err := os.WriteFile(path, []byte("kind: ConfigMap\n"), 0o600); err != nil {
				b.Fatal(err)
			}
		}
	}
	const fp = "feedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface"

	cache, err := NewStagingCache(b.TempDir(), 0)
	if err != nil {
		b.Fatalf("NewStagingCache: %v", err)
	}
	b.Cleanup(func() { _ = cache.Close() })

	// Warm: first call populates the persistent stage.
	if _, err := cache.Stage(context.Background(), src, fp); err != nil {
		b.Fatalf("warm Stage: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cache.Stage(context.Background(), src, fp); err != nil {
			b.Fatalf("Stage: %v", err)
		}
	}
}
