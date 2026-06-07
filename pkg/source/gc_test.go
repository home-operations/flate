package source

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/source/blob"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestSweep_AgePrunesStaleSlots: a slot whose mtime is older than
// MaxAge is removed; a fresh one is preserved.
func TestSweep_AgePrunesStaleSlots(t *testing.T) {
	root := t.TempDir()
	stale := filepath.Join(root, "sources", "old-repo", "deadbeef")
	fresh := filepath.Join(root, "sources", "new-repo", "cafefeed")
	for _, d := range []string{stale, fresh} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "marker"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-7 * 24 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != stale {
		t.Errorf("Removed = %v, want [%q]", res.Removed, stale)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale slot still exists: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh slot removed: %v", err)
	}
}

func TestSweep_IgnoresSourceSlotLockFiles(t *testing.T) {
	root := t.TempDir()
	lockFile := filepath.Join(root, "sources", "repo", "deadbeef.lock")
	if err := os.MkdirAll(filepath.Dir(lockFile), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockFile, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-7 * 24 * time.Hour)
	if err := os.Chtimes(lockFile, old, old); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if slices.Contains(res.Removed, lockFile) {
		t.Errorf("lock file should not be swept as a source slot: %v", res.Removed)
	}
	if _, err := os.Stat(lockFile); err != nil {
		t.Errorf("lock file removed from disk: %v", err)
	}
}

// TestSweepStageCacheByAge: the stage-cache age sweep (via the shared
// kustomize.EachStageDir walker with requireSentinel=false) reaps OLD
// fingerprint dirs regardless of the stage-complete sentinel — including
// abandoned crash debris with no sentinel — while skipping the per-process
// flate-stage-* scratch dirs and .tmp.* in-flight staging dirs at both levels.
// TestSweep_BaselinesAndBlobs: age applies to baselines/<sha>/ and
// blobs/sha256/<digest>/ exactly the same way as sources.
func TestSweep_BaselinesAndBlobs(t *testing.T) {
	root := t.TempDir()
	baseline := filepath.Join(root, "baselines", "abc123")
	blob := filepath.Join(root, "blobs", "sha256", "def456")
	for _, d := range []string{baseline, blob} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, d := range []string{baseline, blob} {
		if err := os.Chtimes(d, old, old); err != nil {
			t.Fatal(err)
		}
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if len(res.Removed) != 2 {
		t.Errorf("Removed = %v, want 2 entries", res.Removed)
	}
}

// TestSweep_MirrorsPreservedByDefault: bare git mirrors are kept
// across sweeps unless IncludeMirrors is set — they're expensive to
// rebuild.
func TestSweep_MirrorsPreservedByDefault(t *testing.T) {
	root := t.TempDir()
	mirror := filepath.Join(root, "git-mirrors", "abc123")
	if err := os.MkdirAll(mirror, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(mirror, old, old); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if slices.Contains(res.Removed, mirror) {
		t.Error("mirror swept without IncludeMirrors")
	}

	res, _ = Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour, IncludeMirrors: true})
	if !slices.Contains(res.Removed, mirror) {
		t.Errorf("mirror not swept with IncludeMirrors: %v", res.Removed)
	}
}

// TestSweep_DanglingRefsCleaned: a ref pointing at a digest that
// doesn't exist in blobs/ is removed regardless of age.
func TestSweep_DanglingRefsCleaned(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "refs", "charts"), 0o750); err != nil {
		t.Fatal(err)
	}
	const (
		missingDigest = "0000000000000000000000000000000000000000000000000000000000000000"
		liveDigest    = "1111111111111111111111111111111111111111111111111111111111111111"
	)
	dangling := filepath.Join(root, "refs", "charts", "missing-chart")
	if err := os.WriteFile(dangling, []byte(missingDigest), 0o600); err != nil {
		t.Fatal(err)
	}

	// A second ref points at a real blob — must survive.
	live := filepath.Join(root, "refs", "charts", "real-chart")
	if err := os.WriteFile(live, []byte(liveDigest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "blobs", "sha256", liveDigest), 0o750); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{})
	if !slices.Contains(res.Removed, dangling) {
		t.Errorf("dangling ref not removed: %v", res.Removed)
	}
	if slices.Contains(res.Removed, live) {
		t.Errorf("live ref removed: %v", res.Removed)
	}
}

// TestSweep_LiveRefPreservesOldBlob is the mark-sweep contract: a
// blob whose digest is referenced by a live ref must survive the age
// sweep, even when its mtime is older than MaxAge. Without mark, the
// fresh ref would silently point at a deleted blob and the next
// render would hit ENOENT.
func TestSweep_LiveRefPreservesOldBlob(t *testing.T) {
	root := t.TempDir()
	const digest = "2222222222222222222222222222222222222222222222222222222222222222"

	blob := filepath.Join(root, "blobs", "sha256", digest)
	if err := os.MkdirAll(blob, 0o750); err != nil {
		t.Fatal(err)
	}
	// Stamp the blob old so the age sweep would otherwise grab it.
	old := time.Now().Add(-365 * 24 * time.Hour)
	if err := os.Chtimes(blob, old, old); err != nil {
		t.Fatal(err)
	}

	// A fresh ref points at this digest — the chart we just resolved.
	if err := os.MkdirAll(filepath.Join(root, "refs", "charts"), 0o750); err != nil {
		t.Fatal(err)
	}
	ref := filepath.Join(root, "refs", "charts", "live-chart")
	if err := os.WriteFile(ref, []byte(digest), 0o600); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if slices.Contains(res.Removed, blob) {
		t.Error("live-referenced blob was swept by age — mark phase broken")
	}
	if _, err := os.Stat(blob); err != nil {
		t.Errorf("live blob removed from disk: %v", err)
	}
	if slices.Contains(res.Removed, ref) {
		t.Error("live ref removed; should have survived (blob exists)")
	}
}

// TestSweep_UnreferencedOldBlobIsPruned proves the inverse — an old
// blob with NO ref still pointing at it gets swept.
func TestSweep_UnreferencedOldBlobIsPruned(t *testing.T) {
	root := t.TempDir()
	const digest = "3333333333333333333333333333333333333333333333333333333333333333"
	blob := filepath.Join(root, "blobs", "sha256", digest)
	if err := os.MkdirAll(blob, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(blob, old, old); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour})
	if !slices.Contains(res.Removed, blob) {
		t.Errorf("orphan blob survived: %v", res.Removed)
	}
}

// TestSweep_PutInFlightPreservesBlob is the regression test for the
// mark↔sweep race fix in gclock.go. The caller's PutBytes + Refs.Put
// pair runs concurrently with Sweep; the invariant is "if the ref
// landed, its blob still resolves" — i.e., no ref points at a freshly-
// purged blob.
//
// Pre-fix: Sweep marks refs before PutBytes refreshes the (old) blob's
// mtime, then ages-out the blob; Refs.Put then writes a ref pointing
// at a deleted blob. Post-fix: PutBytes holds rLockGC and refreshes
// mtime, Refs.Put holds rLockGC, Sweep holds the exclusive lock via
// blob.WithSweepLock — the three operations serialize cleanly so the
// invariant holds in every interleaving.
func TestSweep_PutInFlightPreservesBlob(t *testing.T) {
	layout := cacheroot.New(t.TempDir())
	store := blob.NewStore(layout)
	refs := blob.NewRefs(layout, "test")

	// Pre-seed an OLD blob so Sweep would age-prune it on the next pass.
	content := []byte("payload")
	dir, digest, err := store.PutBytes(context.Background(), content, "f.bin")
	if err != nil {
		t.Fatalf("seed PutBytes: %v", err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}

	// Race the (PutBytes → Refs.Put) pair against Sweep with a tight
	// MaxAge that would otherwise reap the seeded blob.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, _, perr := store.PutBytes(context.Background(), content, "f.bin"); perr != nil {
			t.Errorf("racing PutBytes: %v", perr)
			return
		}
		if perr := refs.Put("k", digest); perr != nil {
			t.Errorf("racing Refs.Put: %v", perr)
		}
	}()
	go func() {
		defer wg.Done()
		if _, serr := Sweep(layout, SweepOpts{MaxAge: 1 * time.Nanosecond}); serr != nil {
			t.Errorf("Sweep: %v", serr)
		}
	}()
	wg.Wait()

	// Invariant: if the ref landed, its blob must resolve.
	if got, ok := refs.Get("k"); ok {
		if got != digest {
			t.Fatalf("ref points at unexpected digest %q (want %q)", got, digest)
		}
		if _, err := os.Stat(layout.Blob(got)); err != nil {
			t.Errorf("ref → blob invariant broken: ref exists but blob is gone: %v", err)
		}
	}
}

// TestSweep_DryRunReports: DryRun emits the would-be-removed list
// without touching disk.
func TestSweep_DryRunReports(t *testing.T) {
	root := t.TempDir()
	slot := filepath.Join(root, "sources", "repo", "hash")
	if err := os.MkdirAll(slot, 0o750); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(slot, old, old); err != nil {
		t.Fatal(err)
	}

	res, _ := Sweep(cacheroot.New(root), SweepOpts{MaxAge: 24 * time.Hour, DryRun: true})
	if !slices.Contains(res.Removed, slot) {
		t.Errorf("DryRun didn't report stale slot: %v", res.Removed)
	}
	if _, err := os.Stat(slot); err != nil {
		t.Errorf("DryRun removed slot on disk: %v", err)
	}
}
