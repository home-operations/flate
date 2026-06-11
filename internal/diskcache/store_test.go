package diskcache

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sweepBlocking forces a synchronous eviction pass. Test affordance — production
// callers use the async trigger inside Put. Lets eviction-ordering assertions
// run without flake.
func (s *Store) sweepBlocking() {
	if s == nil {
		return
	}
	// Wait for any in-flight async sweep to drain before kicking off our own so
	// the synchronous call sees a stable view.
	for !s.sweepGate.TryAcquire() {
		time.Sleep(time.Millisecond)
	}
	s.sweep()
}

// TestStore_RoundTrip pins the basic put-then-get path: payload bytes survive
// gzip + atomic rename + read intact. The load-bearing assertion is
// byte-identity — a single corrupted byte would be silently observed by every
// downstream consumer.
func TestStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, 1<<20)
	if s == nil {
		t.Fatalf("NewStore returned nil for valid inputs")
	}

	key := strings.Repeat("a", 64) // 64-hex digest shape
	payload := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n")
	s.Put(key, payload)

	got, ok := s.Get(key)
	if !ok {
		t.Fatalf("Get after Put should hit")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get returned different bytes than Put:\nwant: %q\ngot:  %q", payload, got)
	}
}

// TestStore_PutShardsByHexPrefix pins the on-disk layout: entries land under
// <root>/<key[:2]>/<key> so no single directory holds the entire keyspace. A
// regression that flattens the layout would surface here.
func TestStore_PutShardsByHexPrefix(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, 1<<20)
	key := strings.Repeat("b", 64)
	s.Put(key, []byte("payload"))

	want := filepath.Join(dir, key[:2], key)
	info, err := os.Stat(want)
	if err != nil {
		t.Fatalf("expected file at %s, got %v", want, err)
	}
	if info.Size() == 0 {
		t.Fatalf("file at %s is empty; Put should have written gzipped bytes", want)
	}
}

// TestStore_MissReturnsFalse covers the cold-cache path: a key never Put returns
// (nil, false), and a nil receiver does the same (the disabled-cache sentinel
// contract callers rely on).
func TestStore_MissReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, 1<<20)
	if got, ok := s.Get(strings.Repeat("c", 64)); ok || got != nil {
		t.Fatalf("miss should return (nil, false); got (%v, %v)", got, ok)
	}

	var nilStore *Store
	if got, ok := nilStore.Get("anything"); ok || got != nil {
		t.Fatalf("nil receiver miss should return (nil, false); got (%v, %v)", got, ok)
	}
	nilStore.Put("anything", []byte("nope")) // must not panic
}

// TestStore_DisabledOnEmptyRoot pins the constructor's disabled-sentinel
// contract for the two disable inputs (empty root, non-positive limit). Both
// return nil so callers' `if store == nil` short-circuit fires.
func TestStore_DisabledOnEmptyRoot(t *testing.T) {
	if NewStore("", 1<<20) != nil {
		t.Errorf("empty root must return nil (caching disabled)")
	}
	if NewStore(t.TempDir(), 0) != nil {
		t.Errorf("zero limit must return nil")
	}
	if NewStore(t.TempDir(), -1) != nil {
		t.Errorf("negative limit must return nil")
	}
}

// TestStore_SweepEvictsOldestByMtime pins the LRU-by-mtime eviction policy: with
// three entries totaling more than the limit, the sweep removes the oldest until
// total ≤ limit. The fixed per-entry mtimes (manually Chtimes-d after the write)
// make the expected eviction order deterministic across filesystems.
func TestStore_SweepEvictsOldestByMtime(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, 50) // 50 bytes total
	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
	}
	for i, k := range keys {
		s.Put(k, []byte(strings.Repeat("x", 1024)))
		// Stagger mtimes so the sort key is unambiguous. Older entries get
		// earlier timestamps.
		p := s.pathFor(k)
		t0 := time.Now().Add(-time.Duration(len(keys)-i) * time.Hour)
		if err := os.Chtimes(p, t0, t0); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}

	s.sweepBlocking()

	// The oldest (index 0) must be gone; the youngest (index 2) must survive.
	// The middle is a tie-breaker the sweep may keep or drop depending on
	// cumulative size, so we only pin the extremes.
	if _, ok := s.Get(keys[0]); ok {
		t.Errorf("oldest entry %s must be evicted, still present", keys[0])
	}
	if _, ok := s.Get(keys[2]); !ok {
		t.Errorf("newest entry %s must survive, was evicted", keys[2])
	}
}

// TestStore_CrossProcessReuse pins the cross-invocation contract: a fresh Store
// pointing at the same root reads keys the previous instance wrote. This is the
// entire reason the disk layer exists.
func TestStore_CrossProcessReuse(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	payload := []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\n")

	first := NewStore(dir, 1<<20)
	first.Put(key, payload)

	second := NewStore(dir, 1<<20)
	got, ok := second.Get(key)
	if !ok {
		t.Fatalf("fresh Store pointing at same root must read the previous Put")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("cross-process read returned different bytes:\nwant: %q\ngot:  %q", payload, got)
	}
}

// TestStore_GetBumpsMtime pins the LRU freshness bump: a successful Get chtimes
// the entry to nowFn() so the next sweep sees it as recently used. Rebinds nowFn
// for a deterministic clock.
func TestStore_GetBumpsMtime(t *testing.T) {
	orig := nowFn
	t.Cleanup(func() { nowFn = orig })

	dir := t.TempDir()
	s := NewStore(dir, 1<<20)
	key := strings.Repeat("e", 64)
	s.Put(key, []byte("payload"))

	// Backdate the entry, then Get with a pinned "now" far in the future and
	// assert the file's mtime advanced to it.
	p := s.pathFor(key)
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	bump := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	nowFn = func() time.Time { return bump }

	if _, ok := s.Get(key); !ok {
		t.Fatalf("Get should hit")
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !info.ModTime().Equal(bump) {
		t.Errorf("Get did not bump mtime to nowFn(); want %v, got %v", bump, info.ModTime())
	}
}

// TestStore_ReadsExternallyWrittenGzip pins the on-disk wire format: a file that
// is simply gzip(payload) at the sharded path is a valid Store entry. This is
// what guarantees a warm cache written by a prior binary stays readable across
// an upgrade — the format is gzip(payload), nothing more.
func TestStore_ReadsExternallyWrittenGzip(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, 1<<20)
	key := strings.Repeat("f", 64)
	payload := []byte("hello: world\n")

	// Write gzip(payload) by hand at the sharded path — no Store.Put involved.
	p := s.pathFor(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	got, ok := s.Get(key)
	if !ok {
		t.Fatalf("Store must read an externally-written gzip(payload) entry")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("wire-format read mismatch:\nwant: %q\ngot:  %q", payload, got)
	}
}
