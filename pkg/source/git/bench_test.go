package git

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/source"
)

// BenchmarkCachedRevision_HitMiss measures readCachedRevision against
// a fresh marker (hit) and a missing marker (miss) — the two
// canonical outcomes of the per-fetch cache-hit probe in fetcher.go.
//
// The hit path is the hot post-clone read; the miss path is what every
// new slot lookup pays before falling through to clone.
func BenchmarkCachedRevision_HitMiss(b *testing.B) {
	hitSlot := b.TempDir()
	if err := writeCachedRevision(hitSlot, "abc123def456"); err != nil {
		b.Fatalf("seed marker: %v", err)
	}
	missSlot := b.TempDir() // no marker written

	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if rev := readCachedRevision(hitSlot); rev == "" {
				b.Fatalf("expected hit, got empty")
			}
		}
	})

	b.Run("Miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if rev := readCachedRevision(missSlot); rev != "" {
				b.Fatalf("expected miss, got %q", rev)
			}
		}
	})

	// Stale-marker check via cachedRevisionFresh — the post-hit
	// freshness gate the fetcher consults on mutable refs.
	b.Run("FreshHit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			rev, ok := cachedRevisionFresh(hitSlot, time.Hour)
			if !ok || rev == "" {
				b.Fatalf("expected fresh hit, got %q ok=%v", rev, ok)
			}
		}
	})

	b.Run("FreshMiss_Stale", func(b *testing.B) {
		// Backdate the marker so cachedRevisionFresh treats it as stale.
		past := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filepath.Join(hitSlot, source.SlotMetaFile), past, past); err != nil {
			b.Fatalf("chtimes: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			rev, ok := cachedRevisionFresh(hitSlot, time.Hour)
			if ok {
				b.Fatalf("expected stale miss, got %q ok=%v", rev, ok)
			}
		}
		// Restore for any later sub-benchmark; keep the slot useful.
		now := time.Now()
		_ = os.Chtimes(filepath.Join(hitSlot, source.SlotMetaFile), now, now)
	})
}
