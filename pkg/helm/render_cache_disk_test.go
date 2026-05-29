package helm

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestDiskRenderCache_RoundTrip pins the basic put-then-get path:
// payload bytes survive gzip + atomic rename + read intact. The
// load-bearing assertion is byte-identity — a single corrupted byte
// in the rendered manifest would be silently observed by every
// downstream consumer.
func TestDiskRenderCache_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := newDiskRenderCache(dir, 1<<20)
	if c == nil {
		t.Fatalf("newDiskRenderCache returned nil for valid inputs")
	}

	key := strings.Repeat("a", 64) // 64-hex digest shape
	payload := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n")
	c.Put(key, payload)

	got, ok := c.Get(key)
	if !ok {
		t.Fatalf("Get after Put should hit")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get returned different bytes than Put:\nwant: %q\ngot:  %q", payload, got)
	}
}

// TestDiskRenderCache_PutShardsByHexPrefix pins the on-disk layout:
// entries land under <root>/<key[:2]>/<key> so no single directory
// holds the entire keyspace. A regression that flattens the layout
// would surface here.
func TestDiskRenderCache_PutShardsByHexPrefix(t *testing.T) {
	dir := t.TempDir()
	c := newDiskRenderCache(dir, 1<<20)
	key := strings.Repeat("b", 64)
	c.Put(key, []byte("payload"))

	want := filepath.Join(dir, key[:2], key)
	info, err := os.Stat(want)
	if err != nil {
		t.Fatalf("expected file at %s, got %v", want, err)
	}
	if info.Size() == 0 {
		t.Fatalf("file at %s is empty; Put should have written gzipped bytes", want)
	}
}

// TestDiskRenderCache_MissReturnsFalse covers the cold cache path:
// a key never Put returns (nil, false), and a nil receiver does the
// same (the disabled-cache sentinel contract callers rely on at the
// in-memory layer's disk fall-through).
func TestDiskRenderCache_MissReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	c := newDiskRenderCache(dir, 1<<20)
	if got, ok := c.Get(strings.Repeat("c", 64)); ok || got != nil {
		t.Fatalf("miss should return (nil, false); got (%v, %v)", got, ok)
	}

	var nilCache *diskRenderCache
	if got, ok := nilCache.Get("anything"); ok || got != nil {
		t.Fatalf("nil receiver miss should return (nil, false); got (%v, %v)", got, ok)
	}
	nilCache.Put("anything", []byte("nope")) // must not panic
}

// TestDiskRenderCache_DisabledOnEmptyRoot pins the constructor's
// disabled-sentinel contract for the two disable inputs (empty root,
// non-positive limit). Both return nil so the in-memory layer's
// `if c.disk == nil` short-circuit fires.
func TestDiskRenderCache_DisabledOnEmptyRoot(t *testing.T) {
	if newDiskRenderCache("", 1<<20) != nil {
		t.Errorf("empty root must return nil (caching disabled)")
	}
	if newDiskRenderCache(t.TempDir(), 0) != nil {
		t.Errorf("zero limit must return nil")
	}
	if newDiskRenderCache(t.TempDir(), -1) != nil {
		t.Errorf("negative limit must return nil")
	}
}

// TestDiskRenderCache_SweepEvictsOldestByMtime pins the LRU-by-mtime
// eviction policy: with three entries totaling more than the limit,
// the sweep removes the oldest until total ≤ limit. The fixed
// per-entry mtimes (manually Chtimes-d after the write) make the
// expected eviction order deterministic across filesystems.
func TestDiskRenderCache_SweepEvictsOldestByMtime(t *testing.T) {
	dir := t.TempDir()
	// 3 entries, ~300 bytes compressed each — pick a tiny limit so
	// SweepBlocking has to drop at least one entry. The compressed
	// size is content-dependent; we keep entries large enough that
	// the per-shard ~32-byte tmp/finalize cost stays a rounding error.
	c := newDiskRenderCache(dir, 50) // 50 bytes total
	keys := []string{
		strings.Repeat("1", 64),
		strings.Repeat("2", 64),
		strings.Repeat("3", 64),
	}
	for i, k := range keys {
		c.Put(k, []byte(strings.Repeat("x", 1024)))
		// Stagger mtimes so the sort key is unambiguous. Older
		// entries get earlier timestamps.
		p := c.pathFor(k)
		t0 := time.Now().Add(-time.Duration(len(keys)-i) * time.Hour)
		if err := os.Chtimes(p, t0, t0); err != nil {
			t.Fatalf("Chtimes %s: %v", p, err)
		}
	}

	c.SweepBlocking()

	// The oldest (index 0) must be gone; the youngest (index 2)
	// must survive. The middle is a tie-breaker the sweep may keep
	// or drop depending on cumulative size, so we only pin the
	// extremes.
	if _, ok := c.Get(keys[0]); ok {
		t.Errorf("oldest entry %s must be evicted, still present", keys[0])
	}
	if _, ok := c.Get(keys[2]); !ok {
		t.Errorf("newest entry %s must survive, was evicted", keys[2])
	}
}

// TestDiskRenderCache_CrossProcessReuse pins the cross-invocation
// contract: a fresh cache instance pointing at the same root reads
// keys the previous instance wrote. This is the entire reason the
// disk layer exists; if this test breaks, Phase 3.4a's premise is
// broken too.
func TestDiskRenderCache_CrossProcessReuse(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("d", 64)
	payload := []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\n")

	first := newDiskRenderCache(dir, 1<<20)
	first.Put(key, payload)

	second := newDiskRenderCache(dir, 1<<20)
	got, ok := second.Get(key)
	if !ok {
		t.Fatalf("fresh cache pointing at same root must read the previous Put")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("cross-process read returned different bytes:\nwant: %q\ngot:  %q", payload, got)
	}
}

// TestTemplateCache_PromotesFromDisk pins the in-memory layer's
// fall-through to disk: a Get that misses memory but hits disk
// returns the disk payload AND populates the LRU so the second
// same-process Get stays a memory hit. The cache.Len() probe before
// and after verifies the promotion happened.
func TestTemplateCache_PromotesFromDisk(t *testing.T) {
	dir := t.TempDir()
	disk := newDiskRenderCache(dir, 1<<20)
	key := strings.Repeat("e", 64)
	disk.Put(key, []byte("from-disk"))

	mem := newTemplateCache(1<<20, disk)
	if got := mem.Len(); got != 0 {
		t.Fatalf("fresh cache should be empty, has %d entries", got)
	}

	v, ok := mem.Get(key)
	if !ok {
		t.Fatalf("Get should hit via disk fall-through")
	}
	if v != "from-disk" {
		t.Fatalf("disk fall-through returned wrong value: %q", v)
	}
	if got := mem.Len(); got != 1 {
		t.Fatalf("disk hit must promote to memory; Len=%d, want 1", got)
	}
	// Second Get serves from memory; correctness is already pinned
	// by the first assertion, but the Len invariant should still
	// hold (no double-insert).
	if _, ok := mem.Get(key); !ok {
		t.Fatalf("second Get on a promoted key must hit")
	}
	if got := mem.Len(); got != 1 {
		t.Fatalf("Len after second Get = %d, want 1", got)
	}
}

// TestTemplateCache_PutWritesThroughToDisk pins the write-through
// contract: a Put against the in-memory layer must persist to disk
// so a subsequent fresh cache instance reads the same value. Without
// this, the in-memory hit rate would be fine but cross-process hit
// rate would stay at zero — defeating Phase 3.4a's entire purpose.
func TestTemplateCache_PutWritesThroughToDisk(t *testing.T) {
	dir := t.TempDir()
	disk1 := newDiskRenderCache(dir, 1<<20)
	mem1 := newTemplateCache(1<<20, disk1)
	key := strings.Repeat("f", 64)
	mem1.Put(key, "persistent")

	// Fresh stack — no shared state with mem1/disk1 — but pointing
	// at the same on-disk root.
	disk2 := newDiskRenderCache(dir, 1<<20)
	mem2 := newTemplateCache(1<<20, disk2)
	got, ok := mem2.Get(key)
	if !ok {
		t.Fatalf("fresh cache must read the disk-write-through value")
	}
	if got != "persistent" {
		t.Fatalf("fresh cache returned wrong value: %q", got)
	}
}

// TestTemplateCache_TemplateIntegrationCrossProcess covers the on-
// the-render-path behavior end-to-end: render with cache instance A,
// instantiate cache instance B pointing at the same disk root,
// render again — second render returns byte-identical output via the
// disk cache without re-running action.Install. The byte-identity
// assertion catches any divergence between cached and uncached output
// (the failure mode disk-layer correctness bugs would surface as).
func TestTemplateCache_TemplateIntegrationCrossProcess(t *testing.T) {
	// Chart fixture mirrors the in-process integration test so
	// values churn meaningfully on each render.
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, chartDir, "mychart/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, chartDir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)

	cacheRoot := t.TempDir()
	layout := cacheroot.New(cacheRoot)

	// First client: render and persist to disk.
	first, err := NewClientWithOptions(layout, ClientOptions{
		TemplateCacheBytes: 1 << 20,
		RenderCacheBytes:   1 << 20,
		RenderCacheRoot:    layout.RenderHelmCache(),
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions first: %v", err)
	}
	first.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))

	hr := newHR()
	ctx := context.Background()
	expected, err := first.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("first Template: %v", err)
	}

	// Second client: fresh in-memory cache, same disk root. The
	// render must hit disk and return byte-identical output.
	second, err := NewClientWithOptions(layout, ClientOptions{
		TemplateCacheBytes: 1 << 20,
		RenderCacheBytes:   1 << 20,
		RenderCacheRoot:    layout.RenderHelmCache(),
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions second: %v", err)
	}
	second.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", chartDir))

	got, err := second.Template(ctx, hr, map[string]any{"greeting": "hi"}, Options{})
	if err != nil {
		t.Fatalf("second Template: %v", err)
	}
	if got != expected {
		t.Fatalf("cross-process disk-cached render diverged from initial render:\nfirst:\n%s\nsecond:\n%s", expected, got)
	}
	// Second client's in-memory cache should have exactly one entry
	// — the disk fall-through promotion.
	if got := second.templateCache.Len(); got != 1 {
		t.Errorf("expected 1 in-memory entry after disk-cached render, got %d", got)
	}
}
