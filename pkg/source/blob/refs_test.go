package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestRefs_RoundTrip(t *testing.T) {
	r := NewRefs(cacheroot.New(t.TempDir()), "test")
	if _, ok := r.Get("missing"); ok {
		t.Error("unset key should miss")
	}
	if err := r.Put("repo/chart@1.2.3", "deadbeef"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := r.Get("repo/chart@1.2.3")
	if !ok || got != "deadbeef" {
		t.Errorf("Get = (%q, %v), want (deadbeef, true)", got, ok)
	}
}

func TestRefs_OverwritePicksUpNewDigest(t *testing.T) {
	r := NewRefs(cacheroot.New(t.TempDir()), "test")
	_ = r.Put("k", "old")
	_ = r.Put("k", "new")
	got, _ := r.Get("k")
	if got != "new" {
		t.Errorf("Get after overwrite = %q, want new", got)
	}
}

// TestRefs_GetServesFromMemory: after Put the in-memory cache is
// populated, so a subsequent Get must not hit disk even if the
// backing file is deleted. Proves the hot-path optimization is
// load-bearing.
func TestRefs_GetServesFromMemory(t *testing.T) {
	layout := cacheroot.New(t.TempDir())
	r := NewRefs(layout, "test")
	if err := r.Put("k", "abc"); err != nil {
		t.Fatal(err)
	}
	// Wipe the on-disk file behind Put's back; only the in-memory
	// entry remains.
	_ = os.RemoveAll(layout.RefsCategory("test"))
	got, ok := r.Get("k")
	if !ok || got != "abc" {
		t.Errorf("Get after disk wipe = (%q, %v), expected (abc, true) from cache", got, ok)
	}
}

// TestRefs_GetMissDoesNotClobberConcurrentPut: a Get whose disk
// ReadFile observed the OLD ref file must not overwrite the in-memory
// entry a concurrent Put just landed with the NEW digest. The
// pre-fix Get used mem.Store unconditionally, which poisoned the
// cache for the rest of the run.
//
// We simulate the race deterministically by pre-seeding the on-disk
// file with OLD, then in two goroutines (a) calling Put("k", "new")
// and (b) calling Get("k") repeatedly. After both finish, the
// in-memory cache must hold "new" — Put is authoritative.
func TestRefs_GetMissDoesNotClobberConcurrentPut(t *testing.T) {
	layout := cacheroot.New(t.TempDir())
	r := NewRefs(layout, "test")
	dir := layout.RefsCategory("test")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "k"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			_, _ = r.Get("k")
		}
	}()
	go func() {
		defer wg.Done()
		_ = r.Put("k", "new")
	}()
	wg.Wait()
	got, ok := r.Get("k")
	if !ok || got != "new" {
		t.Errorf("after concurrent Put, Get = (%q, %v), want (new, true) — Get clobbered Put's mem entry", got, ok)
	}
}

// TestStore_PutBytesRefreshesMtimeOnReuse: when PutBytes returns the
// existing-blob early path, the blob's mtime is bumped to "now" so a
// concurrent gc.Sweep with a tight MaxAge can't purge a blob the
// caller is about to reference. Without the chtimes, an old blob
// reused across runs would have a stale mtime and could be swept
// between the caller's PutBytes and Refs.Put.
func TestStore_PutBytesRefreshesMtimeOnReuse(t *testing.T) {
	layout := cacheroot.New(t.TempDir())
	s := NewStore(layout)
	content := []byte("hello world")
	dir, digest, err := s.PutBytes(context.Background(), content, "f.bin")
	if err != nil {
		t.Fatalf("PutBytes initial: %v", err)
	}
	sum := sha256.Sum256(content)
	if digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest mismatch: got %s", digest)
	}
	// Artificially age the blob directory.
	old := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
	// Reuse the same content — should hit the Exists=true early return
	// and refresh mtime.
	before := time.Now().Add(-1 * time.Second)
	if _, _, err := s.PutBytes(context.Background(), content, "f.bin"); err != nil {
		t.Fatalf("PutBytes reuse: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Before(before) {
		t.Errorf("blob mtime not refreshed on reuse: got %v, want > %v", info.ModTime(), before)
	}
}

// TestRefs_GetSkipsCorruptFile: a torn ref file (empty or whitespace)
// surfaces as a miss, not as a sentinel digest.
func TestRefs_GetSkipsCorruptFile(t *testing.T) {
	layout := cacheroot.New(t.TempDir())
	r := NewRefs(layout, "test")
	dir := layout.RefsCategory("test")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	// Drop a file with just whitespace under the escaped key path.
	corruptPath := filepath.Join(dir, "corrupt")
	if err := os.WriteFile(corruptPath, []byte("   \n   "), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("corrupt"); ok {
		t.Error("corrupt ref should miss, not surface whitespace as digest")
	}
}
