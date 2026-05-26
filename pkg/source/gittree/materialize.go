// Package gittree materializes a git commit's tree to disk, writing
// every blob as a regular file (or real symlink, where applicable),
// in parallel. Both the baseline-CAS slot and the git fetcher's
// worktree slot route through this single implementation; the two
// callers used to have ~identical sequential variants.
package gittree

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"golang.org/x/sync/errgroup"
)

// Options tunes Materialize.
type Options struct {
	// Workers caps the number of concurrent blob writes. Zero defaults
	// to runtime.NumCPU. Set 1 for deterministic sequential writes
	// (the legacy behavior — useful for narrowing down a
	// concurrency-sensitive bug).
	Workers int
	// OnSubmodule, when non-nil, is invoked for each submodule entry
	// the walker encounters. flate skips submodules in both source-
	// fetcher and baseline contexts (the submodule's state rarely
	// matches what flate is rendering); the callback exists so the
	// caller can log at the right level / structured shape. Nil
	// substitutes a slog.Warn with key "gittree.submodule".
	OnSubmodule func(path string)
}

// Materialize walks the tree at hash and writes every blob into root.
// Symlinks land as real OS symlinks (not collapsed to text files);
// non-file modes (tree entries, etc.) are silently skipped — the
// per-blob write MkdirAll's parents on demand. Submodule entries are
// reported via opts.OnSubmodule and skipped.
//
// Writes run concurrently across opts.Workers goroutines; the walker
// streams entries through a buffered channel so memory stays bounded
// even on monorepos with 50k+ blobs. ctx cancellation propagates to
// every in-flight worker.
func Materialize(ctx context.Context, repo *git.Repository, hash plumbing.Hash, root string, opts Options) error {
	if opts.Workers <= 0 {
		opts.Workers = runtime.NumCPU()
	}
	if opts.OnSubmodule == nil {
		opts.OnSubmodule = func(path string) {
			slog.Warn("gittree: skipping submodule", "path", path)
		}
	}

	commit, err := repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("load commit %s: %w", hash, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("load tree for %s: %w", hash, err)
	}

	type item struct {
		name  string
		entry object.TreeEntry
	}
	entries := make(chan item, opts.Workers*4)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(entries)
		walker := object.NewTreeWalker(tree, true, nil)
		defer walker.Close()
		for {
			name, entry, werr := walker.Next()
			if errors.Is(werr, io.EOF) {
				return nil
			}
			if werr != nil {
				return fmt.Errorf("walk tree: %w", werr)
			}
			if entry.Mode == filemode.Submodule {
				opts.OnSubmodule(name)
				continue
			}
			if entry.Mode == filemode.Dir {
				// Pre-create the directory once on the walker. Without
				// this, every worker that writes a blob into the dir
				// would re-call MkdirAll for the same parent — on a
				// 50k-file monorepo with 5k unique dirs, that's 10×
				// the syscalls. Walker is single-threaded so each
				// unique dir is created once.
				dir := filepath.Join(root, filepath.FromSlash(name))
				if err := os.MkdirAll(dir, 0o750); err != nil {
					return fmt.Errorf("mkdir %s: %w", dir, err)
				}
				continue
			}
			if entry.Mode != filemode.Symlink && !entry.Mode.IsFile() {
				continue
			}
			select {
			case entries <- item{name, entry}:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})
	for range opts.Workers {
		g.Go(func() error {
			for it := range entries {
				if err := writeEntry(repo, it.entry, root, it.name); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// writeEntry materializes one tree entry — a symlink or a blob — at
// root/name. The walker pre-created the parent dir, so we don't
// MkdirAll here.
func writeEntry(repo *git.Repository, entry object.TreeEntry, root, name string) error {
	dst := filepath.Join(root, filepath.FromSlash(name))
	if entry.Mode == filemode.Symlink {
		blob, err := repo.BlobObject(entry.Hash)
		if err != nil {
			return fmt.Errorf("load symlink blob %s for %q: %w", entry.Hash, name, err)
		}
		target, err := readBlobBytes(blob)
		if err != nil {
			return fmt.Errorf("read symlink target for %q: %w", name, err)
		}
		if err := os.Symlink(string(target), dst); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", dst, target, err)
		}
		return nil
	}
	blob, err := repo.BlobObject(entry.Hash)
	if err != nil {
		return fmt.Errorf("load blob %s for %q: %w", entry.Hash, name, err)
	}
	return writeBlobTo(dst, blob, entry.Mode)
}

// readBlobBytes returns the full contents of a blob. Used for symlink
// targets which are bounded by PATH_MAX.
func readBlobBytes(blob *object.Blob) ([]byte, error) {
	r, err := blob.Reader()
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

// writeBlobTo streams blob's content to dst with mode-derived
// permissions (executable bit preserved from filemode.Executable).
// Caller (Materialize's walker) has already created the parent dir.
func writeBlobTo(dst string, blob *object.Blob, mode filemode.FileMode) error {
	perm := os.FileMode(0o600)
	if mode == filemode.Executable {
		perm = 0o700
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm) //nolint:gosec // dst is built from the tree's commit object under the caller's root
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer func() { _ = out.Close() }()
	r, err := blob.Reader()
	if err != nil {
		return fmt.Errorf("blob reader %s: %w", dst, err)
	}
	defer func() { _ = r.Close() }()
	if _, err := io.Copy(out, r); err != nil {
		return fmt.Errorf("copy %s: %w", dst, err)
	}
	return nil
}
