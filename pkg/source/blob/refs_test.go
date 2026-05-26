package blob

import (
	"path/filepath"
	"testing"
)

func TestRefs_RoundTrip(t *testing.T) {
	r := NewRefs(filepath.Join(t.TempDir(), "refs"))
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
	r := NewRefs(filepath.Join(t.TempDir(), "refs"))
	_ = r.Put("k", "old")
	_ = r.Put("k", "new")
	got, _ := r.Get("k")
	if got != "new" {
		t.Errorf("Get after overwrite = %q, want new", got)
	}
}
