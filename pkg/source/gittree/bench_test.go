package gittree

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// benchRepo seeds a source repo with n blobs (a 90/10 small/large mix —
// small ≤16KB land as in-memory pack objects, large ≥64KB take go-git's
// FSObject reopen+inflate path), commits them, then CLONES it. A fresh
// clone stores objects only in a packfile, so Materialize exercises the
// same packfile-backed read path as a production source fetch (a loose
// PlainInit repo would shadow the pack with unpacked objects and
// understate the read cost).
func benchRepo(b *testing.B, n int) (*git.Repository, plumbing.Hash) {
	b.Helper()
	src := b.TempDir()
	repo, err := git.PlainInit(src, false)
	if err != nil {
		b.Fatalf("PlainInit: %v", err)
	}
	for i := range n {
		size := 2 << 10 // 2KB
		if i%10 == 0 {
			size = 96 << 10 // 96KB → FSObject path
		}
		content := make([]byte, size)
		for j := range content {
			content[j] = byte((i*131 + j*7) % 251) // poorly-compressible fill
		}
		full := filepath.Join(src, fmt.Sprintf("d%02d", i%50), fmt.Sprintf("f%05d.bin", i))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(full, content, 0o600); err != nil {
			b.Fatal(err)
		}
	}
	wt, err := repo.Worktree()
	if err != nil {
		b.Fatalf("Worktree: %v", err)
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		b.Fatalf("Add: %v", err)
	}
	if _, err := wt.Commit("seed", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	}); err != nil {
		b.Fatalf("Commit: %v", err)
	}

	clone := b.TempDir()
	cloned, err := git.PlainClone(clone, false, &git.CloneOptions{URL: src})
	if err != nil {
		b.Fatalf("PlainClone: %v", err)
	}
	head, err := cloned.Head()
	if err != nil {
		b.Fatalf("Head: %v", err)
	}
	return cloned, head.Hash() // Materialize resolves the commit's tree itself
}

// BenchmarkMaterialize is the gate for the blob-write lock-narrowing.
// Workers=1 has no lock contention (control); Workers=NumCPU is where
// moving the per-blob disk write outside the object-read lock can pay
// off. Compare current-vs-narrowed with benchstat over -count=10; ship
// the narrowing only on a significant Workers=N improvement.
func BenchmarkMaterialize(b *testing.B) {
	repo, commit := benchRepo(b, 5000)
	for _, workers := range []int{1, runtime.NumCPU()} {
		b.Run(fmt.Sprintf("Workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				dst, err := os.MkdirTemp("", "mat-bench-*")
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := Materialize(context.Background(), repo, commit, dst, Options{Workers: workers}); err != nil {
					b.Fatalf("Materialize: %v", err)
				}

				b.StopTimer()
				_ = os.RemoveAll(dst)
				b.StartTimer()
			}
		})
	}
}
