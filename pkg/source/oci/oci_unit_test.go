package oci

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/source"
)

// fullDigest is a sha256-shaped digest used across the cached-
// digest tests. Real OCI digests are sha256:<64-hex-chars>; the
// readCachedDigest regex requires at least 32 hex chars after the
// algorithm prefix, so test inputs must match the shape that real
// fetches produce.
const fullDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// TestCachedDigest_Roundtrip covers writeCachedDigest + readCachedDigest:
// the digest written by one survives a re-read on cache hit.
func TestCachedDigest_Roundtrip(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := readCachedDigest(slot)
	if got != fullDigest {
		t.Errorf("readCachedDigest = %q, want %q", got, fullDigest)
	}
}

// TestReadCachedDigest_MissingReturnsEmpty pins the cache-miss path:
// no .flate-digest file → empty string (signals "no cached digest").
func TestReadCachedDigest_MissingReturnsEmpty(t *testing.T) {
	if got := readCachedDigest(t.TempDir()); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

// TestReadCachedDigest_MalformedTreatedAsMissing pins the
// format-validation contract: a malformed digest in the sidecar (partial,
// garbage, or shorter than the OCI spec minimum) must read as "" so the
// fetcher's cache-hit path resets the slot instead of passing the bad digest
// to cosign (which would produce a misleading "signature not found" failure).
func TestReadCachedDigest_MalformedTreatedAsMissing(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"empty", ""},
		{"no_colon", "abc123"},
		{"too_short_hex", "sha256:abc"},
		{"non_hex", "sha256:Z" + strings.Repeat("Z", 64)},
		{"only_algorithm", "sha256:"},
		{"random_junk", "this is not a digest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slot := t.TempDir()
			if err := source.WriteSlotMeta(slot, source.SlotMeta{Digest: tc.content}); err != nil {
				t.Fatal(err)
			}
			if got := readCachedDigest(slot); got != "" {
				t.Errorf("malformed digest %q read as %q; want empty", tc.content, got)
			}
		})
	}
}

// TestWriteCachedDigest_AtomicNoPartial pins the atomic-write
// contract: writeCachedDigest must not leave the destination file
// in a partial state at any point. We can't trigger a real crash
// mid-write, but we CAN assert that no .flate-digest-* temp files
// linger after a successful write (those would be created by the
// atomic path and renamed away).
func TestWriteCachedDigest_AtomicNoPartial(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, fullDigest); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(slot)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == source.SlotMetaFile {
			continue
		}
		t.Errorf("unexpected leftover entry in slot after writeCachedDigest: %q (atomic write should have cleaned up the temp)", e.Name())
	}
}

// TestLoadCredentials_ValidJSONLoads covers the happy path with a
// minimal docker config.
func TestLoadCredentials_ValidJSONLoads(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadCredentials(config)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if store == nil {
		t.Error("expected non-nil store for valid config")
	}
}

// TestLoadCredentials_EmptyPathFallsBackToDocker covers the
// docker-default lookup arm. Either it succeeds or it gracefully
// returns (nil, nil) when no default is configured — never errors.
func TestLoadCredentials_EmptyPathFallsBackToDocker(t *testing.T) {
	_, err := loadCredentials("")
	if err != nil {
		t.Errorf("empty path should never error; got %v", err)
	}
}
