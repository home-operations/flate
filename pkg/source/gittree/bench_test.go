package gittree

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/internal/testutil"
)

// benchRepo seeds a source repo with n blobs (a 90/10 small/large mix —
// small ≤16KB land as in-memory pack objects, large ≥64KB take go-git's
// FSObject reopen+inflate path), commits them, then CLONES it. A fresh
// clone stores objects only in a packfile, so the views exercise the
// same packfile-backed read path as a production source fetch (a loose
// PlainInit repo would shadow the pack with unpacked objects and
// understate the read cost).
func benchRepo(b *testing.B, n int) (*git.Repository, plumbing.Hash) {
	b.Helper()
	src := b.TempDir()
	repo := testutil.GitInit(b, src)
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
	testutil.GitCommitAll(b, repo, "seed", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})

	clone := b.TempDir()
	cloned, err := git.PlainClone(clone, false, &git.CloneOptions{URL: src})
	if err != nil {
		b.Fatalf("PlainClone: %v", err)
	}
	head, err := cloned.Head()
	if err != nil {
		b.Fatalf("Head: %v", err)
	}
	return cloned, head.Hash()
}

func BenchmarkView_Packed(b *testing.B) {
	repo, commit := benchRepo(b, 5000)
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	wt, err := repo.Worktree()
	if err != nil {
		b.Fatal(err)
	}
	current, _, err := New(b.Context(), backing, plumbing.ZeroHash, ViewOptions{Mode: Direct, Worktree: wt.Filesystem})
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"Source", "LazyBase", "SelectedBody", "ReadAll"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				opts := ViewOptions{Mode: Source}
				if name == "LazyBase" || name == "SelectedBody" {
					opts = ViewOptions{Mode: Direct, Current: current, Changes: []string{"d00/f00000.bin"}}
				}
				input, _, err := New(b.Context(), backing, commit, opts)
				if err != nil {
					b.Fatal(err)
				}
				if name == "SelectedBody" {
					f, err := input.Open("d00/f00000.bin")
					if err != nil {
						b.Fatal(err)
					}
					if _, err := io.Copy(io.Discard, f); err != nil {
						b.Fatal(err)
					}
					if err := f.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if name == "ReadAll" {
					if err := fs.WalkDir(input, ".", func(path string, entry fs.DirEntry, err error) error {
						if err != nil || entry.IsDir() {
							return err
						}
						f, err := input.Open(path)
						if err != nil {
							return err
						}
						_, err = io.Copy(io.Discard, f)
						closeErr := f.Close()
						if err != nil {
							return err
						}
						return closeErr
					}); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
