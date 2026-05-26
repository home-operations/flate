package blob

import (
	"os"
	"path/filepath"
	"testing"

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
