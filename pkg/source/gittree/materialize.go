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
	"strings"
	"sync"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/source/safepath"
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
	// substitutes a slog.Warn carrying the submodule path.
	OnSubmodule func(path string)
}

// Materialize walks the tree at hash and writes every blob into root.
// Symlinks land as real OS symlinks; all writes are confined to root.
// Malformed tree entries fail. Submodule entries are
// reported via opts.OnSubmodule and skipped.
//
// Writes run concurrently across opts.Workers goroutines; the walker
// streams entries through a buffered channel so memory stays bounded
// even on monorepos with 50k+ blobs. ctx cancellation propagates to
// every in-flight worker.
func Materialize(ctx context.Context, repo *git.Repository, hash plumbing.Hash, root string, opts Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve materialization root: %w", err)
	}
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
	// go-git's packfile scanner and object cache are not safe for
	// concurrent decodes, so each blob READ serializes on
	// serializedObjectReader. The decode returns an independent
	// in-memory reader (see blobBytes), so the per-blob disk WRITE runs
	// OUTSIDE the lock and workers write in parallel.
	objects := &serializedObjectReader{repo: repo}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(entries)
		walker := object.NewTreeWalker(tree, true, nil)
		defer walker.Close()
		for {
			name, entry, werr := objects.nextTreeEntry(walker)
			if errors.Is(werr, io.EOF) {
				return nil
			}
			if werr != nil {
				return fmt.Errorf("walk tree: %w", werr)
			}
			if entry.Name == "" || entry.Name == "." || entry.Name == ".." ||
				strings.ContainsAny(entry.Name, "/\\\x00") {
				return fmt.Errorf("malformed tree entry name %q", entry.Name)
			}
			if entry.Mode == filemode.Submodule {
				opts.OnSubmodule(name)
				continue
			}
			if entry.Mode == filemode.Dir {
				dir, err := safepath.SafeJoin(root, name, true)
				if err != nil {
					return err
				}
				// Parents are visited first. Directories must be real, and
				// exclusive blob creation prevents workers replacing them.
				info, err := os.Lstat(dir)
				if err == nil {
					if !info.IsDir() {
						return fmt.Errorf("unsafe directory destination %q", name)
					}
				} else if os.IsNotExist(err) {
					if err := os.Mkdir(dir, 0o750); err != nil {
						return fmt.Errorf("mkdir %s: %w", name, err)
					}
				} else {
					return fmt.Errorf("stat directory %s: %w", name, err)
				}
				continue
			}

			if !entry.Mode.IsFile() {
				return fmt.Errorf("malformed tree entry mode %s for %q", entry.Mode, name)
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
				if err := gctx.Err(); err != nil {
					return err
				}
				if err := writeEntry(objects, it.entry, root, it.name); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

type serializedObjectReader struct {
	repo *git.Repository
	mu   sync.Mutex
}

func (r *serializedObjectReader) nextTreeEntry(walker *object.TreeWalker) (string, object.TreeEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name, entry, err := walker.Next()
	// TreeWalker reports an unreadable directory object as EOF. It must
	// fail materialization instead of publishing a truncated artifact.
	if errors.Is(err, io.EOF) && entry.Mode == filemode.Dir {
		_, err := r.repo.TreeObject(entry.Hash)
		return name, entry, fmt.Errorf("load directory %q: %w", entry.Name, err)
	}
	return name, entry, err
}

// blobBytes returns a blob's full contents. The go-git read (BlobObject
// + decode + Reader) is serialized on r.mu because the packfile scanner
// and object cache are not concurrency-safe; the returned slice is an
// independent copy, so callers write it to disk after the lock is
// released and concurrent workers write in parallel. Both regular files
// and symlink targets route through here.
//
// The buffer is sized exactly from blob.Size and filled with
// io.ReadFull rather than io.ReadAll, which would re-grow (and so
// roughly double the bytes allocated for) every blob over its initial
// capacity.
func (r *serializedObjectReader) blobBytes(hash plumbing.Hash, name string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	blob, err := r.repo.BlobObject(hash)
	if err != nil {
		return nil, fmt.Errorf("load blob %s for %q: %w", hash, name, err)
	}
	reader, err := blob.Reader()
	if err != nil {
		return nil, fmt.Errorf("blob reader %q: %w", name, err)
	}
	defer func() { _ = reader.Close() }()
	buf := make([]byte, blob.Size)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, fmt.Errorf("read blob %q: %w", name, err)
	}
	return buf, nil
}

// writeEntry materializes one tree entry — a symlink or a blob — at
// root/name. Both kinds read their content under the object-read lock
// (blobBytes) and write outside it, so concurrent workers write to disk
// in parallel. The walker pre-created the parent dir, so we don't
// MkdirAll here. The executable bit is preserved from
// filemode.Executable.
func writeEntry(objects *serializedObjectReader, entry object.TreeEntry, root, name string) error {
	dst, err := safepath.SafeJoin(root, name, true)
	if err != nil {
		return err
	}
	if entry.Mode == filemode.Symlink {
		target, err := objects.blobBytes(entry.Hash, name)
		if err != nil {
			return fmt.Errorf("read symlink target for %q: %w", name, err)
		}
		if err := os.Symlink(string(target), dst); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", dst, target, err)
		}
		return nil
	}

	perm := os.FileMode(0o600)
	if entry.Mode == filemode.Executable {
		perm = 0o700
	}
	data, err := objects.blobBytes(entry.Hash, name)
	if err != nil {
		return err
	}
	// The walker validated every parent. O_EXCL refuses symlink leaves and
	// duplicate tree destinations instead of following or replacing them.
	file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm) //nolint:gosec // SafeJoin confines dst; validated directories and O_EXCL reject symlink writes.
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}
