package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestStore_PutBytesDigestPath(t *testing.T) {
	s := NewStore(cacheroot.New(t.TempDir()))
	content := []byte("hello world")
	dir, digest, err := s.PutBytes(context.Background(), content, "data.txt")
	if err != nil {
		t.Fatalf("PutBytes: %v", err)
	}
	sum := sha256.Sum256(content)
	wantDigest := hex.EncodeToString(sum[:])
	if digest != wantDigest {
		t.Errorf("digest = %q, want %q", digest, wantDigest)
	}
	if !s.Exists(digest) {
		t.Error("Exists should be true after PutBytes")
	}
	got, err := os.ReadFile(filepath.Join(dir, "data.txt")) //nolint:gosec // dir is under t.TempDir()
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content drift: got %q, want %q", got, content)
	}
}

func TestStore_SameContentSharesSlot(t *testing.T) {
	s := NewStore(cacheroot.New(t.TempDir()))
	content := []byte("dedup me")
	dir1, d1, err := s.PutBytes(context.Background(), content, "a.txt")
	if err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	dir2, d2, err := s.PutBytes(context.Background(), content, "a.txt")
	if err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if d1 != d2 {
		t.Errorf("digest drift across identical-content puts: %q vs %q", d1, d2)
	}
	if dir1 != dir2 {
		t.Errorf("path drift across identical-content puts: %q vs %q", dir1, dir2)
	}
}

func TestStore_ConcurrentPutsCoalesce(t *testing.T) {
	s := NewStore(cacheroot.New(t.TempDir()))
	content := []byte("racy")
	const goroutines = 16
	var wg sync.WaitGroup
	var errs atomic.Int32
	digests := make([]string, goroutines)
	for i := range goroutines {
		wg.Go(func() {
			_, d, err := s.PutBytes(context.Background(), content, "data")
			if err != nil {
				errs.Add(1)
				return
			}
			digests[i] = d
		})
	}
	wg.Wait()
	if got := errs.Load(); got != 0 {
		t.Errorf("expected no errors, got %d", got)
	}
	first := digests[0]
	for i, d := range digests {
		if d != first {
			t.Errorf("goroutine %d digest %q differs from %q", i, d, first)
		}
	}
}
