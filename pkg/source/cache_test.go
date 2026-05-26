package source

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestCache_ResetSerializesAgainstSlot exercises the per-slot mutex
// that Slot/Release acquire. A goroutine race-detector run with many
// parallel Slot/Commit/Reset cycles targeting the same path must
// complete without -race tripping. A regression that drops the lock
// from Slot would fail under `go test -race`.
func TestCache_ResetSerializesAgainstSlot(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))
	const goroutines = 16
	const iterations = 32
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				slot, err := c.Slot(context.Background(), "https://shared.example/repo", "main", "")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				slot.Release()
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				slot, err := c.Slot(context.Background(), "https://shared.example/repo", "main", "")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				if slot.Exists {
					if err := slot.Reset(); err != nil {
						t.Errorf("Reset: %v", err)
						slot.Release()
						return
					}
				}
				slot.Release()
			}
		}()
	}
	wg.Wait()
}

// TestCache_SlotSerializesSameKey: two goroutines competing for the
// same (url, ref) must execute their critical sections serially — the
// second caller's Exists=true observation must follow the first
// caller's Commit, not race it. Reproduces the PR-137 cross-CR slot
// collision the previous Cache mutex (which guarded only allocation)
// allowed.
func TestCache_SlotSerializesSameKey(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))
	var firstReleased, secondAcquired atomic.Bool

	// g1Entered closes when G1 has acquired the slot lock; g2Start
	// closes after G2 may begin attempting acquisition. Deterministic
	// — no sleeps — so the test pins the serialization invariant
	// without depending on scheduler timing.
	g1Entered := make(chan struct{})
	g2Start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		slot, err := c.Slot(context.Background(), "https://shared.example/repo", "main", "")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		close(g1Entered)
		// Hold until the harness has launched G2 and confirmed it is
		// blocked on acquisition.
		<-g2Start
		_ = os.WriteFile(filepath.Join(slot.Path, ".inprogress"), []byte("x"), 0o600)
		if err := slot.Commit(); err != nil {
			t.Errorf("Commit: %v", err)
		}
		firstReleased.Store(true)
		slot.Release()
		done <- struct{}{}
	}()
	<-g1Entered // G1 holds the lock.
	go func() {
		slot, err := c.Slot(context.Background(), "https://shared.example/repo", "main", "")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		// G1 must have released by the time we got the lock.
		if !firstReleased.Load() {
			t.Errorf("G2 acquired before G1 released — serialization failed")
		}
		secondAcquired.Store(true)
		if !slot.Exists {
			t.Errorf("expected Exists=true on second acquisition after G1 committed a file")
		}
		slot.Release()
		done <- struct{}{}
	}()
	// G2 has been launched and is now blocking on the slot lock. Tell
	// G1 to finish and release.
	close(g2Start)
	<-done
	<-done
	if !secondAcquired.Load() {
		t.Errorf("G2 never acquired the slot")
	}
}

// TestCache_SlotCommitAtomicRename validates the central atomic-rename
// guarantee: a fetcher that writes into Path and returns without
// calling Commit leaves the final slot absent. The NEXT Slot call
// must observe Exists=false. Without atomic rename, a torn fetch would
// leak its partial directory and the next call would see it as a hit.
func TestCache_SlotCommitAtomicRename(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	// First fetcher aborts after writing partial state.
	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	if slot.Exists {
		t.Fatal("fresh slot should be miss")
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "partial"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	staging := slot.Path
	slot.Release() // no Commit — staging must be wiped, final must stay absent

	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Errorf("staging dir leaked after Release without Commit: stat err = %v", err)
	}

	// Next caller must see Exists=false.
	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot 2: %v", err)
	}
	if slot.Exists {
		t.Error("second Slot observed Exists=true after first fetcher aborted; atomic-rename guarantee broken")
	}
	slot.Release()
}

// TestCache_SlotCommitPersists validates the success path: write
// staging, Commit, Release, next Slot observes Exists=true with the
// committed contents at Path.
func TestCache_SlotCommitPersists(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "ok"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	committed := slot.Path
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot 2: %v", err)
	}
	if !slot.Exists {
		t.Error("expected Exists=true after Commit")
	}
	if slot.Path != committed {
		t.Errorf("Path drift: committed %q, second-acquire %q", committed, slot.Path)
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "ok")); err != nil {
		t.Errorf("committed file missing: %v", err)
	}
	slot.Release()
}

func TestCache_CommitAdoptsExistingFinalSlot(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	staging := slot.Path
	if err := os.WriteFile(filepath.Join(staging, "ours"), []byte("ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(slot.final, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(slot.final, "theirs"), []byte("theirs"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := slot.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.final, "theirs")); err != nil {
		t.Errorf("existing final slot was not preserved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.final, "ours")); !os.IsNotExist(err) {
		t.Errorf("staged contents should not overwrite adopted final slot; stat err=%v", err)
	}
	if _, err := os.Stat(staging); !os.IsNotExist(err) {
		t.Errorf("adopted staging dir leaked: %v", err)
	}
	slot.Release()
}

// TestCache_AuthIDIsolatesSlots locks in that two source CRs with the
// same (URL, ref) but different auth IDs do not collide on disk —
// otherwise the first fetch's clone is silently reused for the second
// auth context, bypassing the access check.
func TestCache_AuthIDIsolatesSlots(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))
	a, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "team-a/git-creds")
	if err != nil {
		t.Fatalf("Slot a: %v", err)
	}
	defer a.Release()
	b, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "team-b/git-creds")
	if err != nil {
		t.Fatalf("Slot b: %v", err)
	}
	defer b.Release()
	if a.Path == b.Path {
		t.Errorf("auth-keyed slots should differ, both got %q", a.Path)
	}
	// Empty authID must still collide with itself.
	c2 := NewCache(cacheroot.New(t.TempDir()))
	x, err := c2.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot x: %v", err)
	}
	y := filepath.Dir(x.Path)
	x.Release()
	z, err := c2.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot z: %v", err)
	}
	if filepath.Dir(z.Path) != y {
		t.Errorf("same (url, ref, \"\") must produce same slot dir")
	}
	z.Release()
}

// TestCache_SlotResetThenStage validates the reset-while-holding path
// fetchers use when a cache-hit slot is detected as stale (e.g. cosign
// rejected the cached digest). Reset wipes the final, Stage allocates
// a fresh staging dir, write + Commit publishes the new contents.
func TestCache_SlotResetThenStage(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	// Seed a committed slot.
	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "old"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatal(err)
	}
	slot.Release()

	// Acquire again, detect stale, reset-and-restage.
	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot 2: %v", err)
	}
	if !slot.Exists {
		t.Fatal("expected Exists=true after seed Commit")
	}
	if err := slot.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := slot.Stage(); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "new"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot 3: %v", err)
	}
	if !slot.Exists {
		t.Error("expected Exists=true after reset+stage+Commit")
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "old")); err == nil {
		t.Error("old contents survived Reset+Commit; expected wipe")
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "new")); err != nil {
		t.Errorf("new contents missing: %v", err)
	}
	slot.Release()
}

func TestCache_StageRefreshPreservesOldSlotOnAbort(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "old"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatal(err)
	}
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot refresh: %v", err)
	}
	if err := slot.StageRefresh(); err != nil {
		t.Fatalf("StageRefresh: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "new"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot after abort: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "old")); err != nil {
		t.Errorf("old slot should survive aborted refresh: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "new")); !os.IsNotExist(err) {
		t.Errorf("aborted refresh staging leaked into final slot: %v", err)
	}
	slot.Release()
}

func TestCache_StageRefreshReplacesOldSlotOnCommit(t *testing.T) {
	c := NewCache(cacheroot.New(t.TempDir()))

	slot, err := c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "old"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatal(err)
	}
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot refresh: %v", err)
	}
	if err := slot.StageRefresh(); err != nil {
		t.Fatalf("StageRefresh: %v", err)
	}
	if err := os.WriteFile(filepath.Join(slot.Path, "new"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := slot.Commit(); err != nil {
		t.Fatalf("Commit refresh: %v", err)
	}
	slot.Release()

	slot, err = c.Slot(context.Background(), "https://example.com/repo", "v1", "")
	if err != nil {
		t.Fatalf("Slot after refresh: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "old")); !os.IsNotExist(err) {
		t.Errorf("old slot survived committed refresh: %v", err)
	}
	if _, err := os.Stat(filepath.Join(slot.Path, "new")); err != nil {
		t.Errorf("new slot missing after committed refresh: %v", err)
	}
	slot.Release()
}

func TestSlugifyRepo(t *testing.T) {
	cases := map[string]string{
		"https://github.com/owner/cluster.git":                "cluster",
		"git@github.com:owner/cluster.git":                    "cluster",
		"https://example.com/long-path/with/slashes/repo.git": "repo",
		"oci://ghcr.io/stefanprodan/charts/podinfo":           "podinfo",
		"": "repo",

		// Tag suffix: versionedURL passes URL:tag into slugify
		// for OCI fetches. Slug should be the chart name, NOT
		// the tag — otherwise the cache layout collapses
		// every release of every chart into the same `&lt;tag&gt;/`
		// directory.
		"oci://ghcr.io/bjw-s-labs/helm/app-template:5.0.1":     "app-template",
		"oci://ghcr.io/bjw-s-labs/helm/app-template:1.2.3-rc4": "app-template",
		"oci://registry.local:5000/charts/mychart:1.2.3":       "mychart",
		"oci://registry.local:5000/charts/mychart":             "mychart",
		// Digest suffix: same concern as tags.
		"oci://ghcr.io/foo/bar@sha256:1111111111111111111111111111111111111111111111111111111111111111": "bar",
		// SCP-style git URL with `@` userinfo must NOT be
		// mistaken for a digest suffix.
		"git@github.com:owner/repo": "repo",
	}
	for in, want := range cases {
		if got := slugifyRepo(in); got != want {
			t.Errorf("slugifyRepo(%q) = %q want %q", in, got, want)
		}
	}
}

func TestMutableCacheKeyIsUnique(t *testing.T) {
	base := "branch:main#opts:abc123"
	a := MutableCacheKey(base)
	b := MutableCacheKey(base)
	if a == b {
		t.Fatalf("MutableCacheKey returned the same key twice: %q", a)
	}
	if !strings.HasPrefix(a, base+"#mutable:") {
		t.Fatalf("MutableCacheKey(%q) = %q", base, a)
	}
	if !strings.HasPrefix(b, base+"#mutable:") {
		t.Fatalf("MutableCacheKey(%q) = %q", base, b)
	}
}
