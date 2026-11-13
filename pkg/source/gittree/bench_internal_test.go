package gittree

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

// benchRepo seeds a source repo with n blobs (a 90/10 small/large mix —
// small ≤16KB land as in-memory pack objects, large ≥64KB take go-git's
// FSObject reopen+inflate path), commits them, then CLONES it. A fresh
// clone stores objects only in a packfile, so the views exercise the
// same packfile-backed read path as a production source fetch (a loose
// PlainInit repo would shadow the pack with unpacked objects and
// understate the read cost).
//
//nolint:gocritic // The benchmark MUST retain the repository owner with its packed commit.
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
		{
			err := os.MkdirAll(filepath.Dir(full), 0o750)
			assert.NoError(b, err)
		}
		{
			err := os.WriteFile(full, content, 0o600)
			assert.NoError(b, err)
		}
	}
	testutil.GitCommitAll(b, repo, "seed", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})

	clone := b.TempDir()
	cloned, err := git.PlainClone(clone, &git.CloneOptions{URL: src})
	assert.FatalIf(b, err != nil, "PlainClone: %v", err)
	head, err := cloned.Head()
	assert.FatalIf(b, err != nil, "Head: %v", err)
	return cloned, head.Hash()
}

func BenchmarkView_Packed(b *testing.B) {
	repo, commit := benchRepo(b, 5000)
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	wt, err := repo.Worktree()
	assert.NoError(b, err)

	current, _, err := New(
		b.Context(),
		backing,
		plumbing.ZeroHash,
		ViewOptions{Mode: Direct, Worktree: wt.Filesystem()},
	)
	assert.NoError(b, err)

	for _, name := range []string{"Source", "LazyBase", "SelectedBody", "ReadAll"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				opts := ViewOptions{Mode: Source}
				if name == "LazyBase" || name == "SelectedBody" {
					opts = ViewOptions{Mode: Direct, Current: current, Changes: []string{"d00/f00000.bin"}}
				}
				input, _, err := New(b.Context(), backing, commit, opts)
				assert.NoError(b, err)

				if name == "SelectedBody" {
					f, err := input.Open("d00/f00000.bin")
					assert.NoError(b, err)
					{
						_, err := io.Copy(io.Discard, f)
						assert.NoError(b, err)
					}
					{
						err := f.Close()
						assert.NoError(b, err)
					}
				}
				if name == "ReadAll" {
					benchReadAll(b, input)
				}
			}
		})
	}
}

func benchReadAll(b *testing.B, input *FS) {
	b.Helper()
	err := fs.WalkDir(input, ".", func(path string, entry fs.DirEntry, err error) error {
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
	})
	assert.NoError(b, err)
}

func BenchmarkFreshInputs_FileSize(b *testing.B) {
	for _, size := range []int{1 << 10, 128 << 10, 8 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			root := b.TempDir()
			data := make([]byte, size)
			for i := range data {
				data[i] = byte((i*131 + i/251) % 253)
			}
			testutil.WriteFile(b, root, "file", string(data))
			location, err := gitrepo.Locate(b.Context(), root, root)
			assert.NoError(b, err)
			directory, err := OpenDirectory(b.Context(), location)
			assert.NoError(b, err)
			input := directory.Artifact()
			assert.NoError(b, directory.Close())
			selected := input.FS.(*FS)
			kinds := map[string]fs.FileMode{"file": 0}
			requested := []string{"file"}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				_, err := selected.FreshInputs(b.Context(), requested, kinds)
				assert.NoError(b, err)
			}
		})
	}
}
