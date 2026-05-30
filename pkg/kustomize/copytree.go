package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/sync/errgroup"
)

// copyTreeInto materializes every regular file from src into dst.
// Symlinks are dereferenced, dotfiles are skipped to keep stages
// clean. Caller owns dst — copyTreeInto neither creates nor removes
// it on failure (so the persistent path can stage into a sibling
// tempdir and atomically rename).
//
// The walk collects file-copy tasks serially (cheap, also creates
// the destination directory skeleton) and then fans them out across
// a worker pool. Each task is independent — hardlinks are atomic;
// byte copies operate on distinct dst paths — so concurrency is
// safe. The pool is capped at runtime.NumCPU because the cost per
// task is I/O, not CPU, and over-fanning would just thrash the page
// cache.
func (c *StagingCache) copyTreeInto(src, dst string) error {
	type task struct {
		srcPath, dstPath string
		mode             os.FileMode
	}
	var tasks []task

	walkErr := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		base := d.Name()
		if d.IsDir() {
			// Skip anything that isn't user content: .git / node_modules
			// and every dot-prefixed dir (which captures .flate-cache).
			if base == "node_modules" || strings.HasPrefix(base, ".") {
				return fs.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		// Only stat when we need to follow a symlink — DirEntry already
		// carries the file-type bits for regular entries. Skipping the
		// stat on 50k regular files in a monorepo eliminates the same
		// number of syscalls; hardlinks inherit mode from source so the
		// fallback-copy's mode field is only consulted on cross-FS
		// stages (EXDEV), where 0o600 is acceptable for kustomize input.
		if d.Type()&fs.ModeSymlink == 0 {
			if !d.Type().IsRegular() {
				return nil
			}
			tasks = append(tasks, task{path, filepath.Join(dst, rel), 0o600})
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			// A dangling symlink in the user's working tree (a common
			// local-only state for editor lockfiles, gitignored
			// .pre-commit-config.yaml, IDE caches) used to abort the
			// entire stage. flate doesn't need the link target — Flux's
			// reconcile wouldn't either — so skip silently when the
			// target is missing. Other Stat errors (permissions, I/O)
			// still surface.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		tasks = append(tasks, task{path, filepath.Join(dst, rel), info.Mode()})
		return nil
	})
	if walkErr != nil {
		return fmt.Errorf("stage %s: %w", src, walkErr)
	}

	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.NumCPU())
	for _, t := range tasks {
		g.Go(func() error { return copyFile(t.srcPath, t.dstPath, t.mode) })
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("stage %s: %w", src, err)
	}
	return nil
}

// copyFile materializes srcPath at dstPath. Hardlinks when source and
// destination sit on the same filesystem — a stage of a monorepo would
// otherwise duplicate gigabytes of bytes on every render. Falls back to
// a stream copy on cross-device (EXDEV) failures so the cache continues
// to work when a user points --cache-dir at a different volume than
// their working tree.
//
// Callers that mutate the staged file MUST first os.Remove it so the
// hardlink is broken before write — otherwise an O_TRUNC|O_WRONLY open
// on the staged path would modify the source's underlying inode.
// flux.go's restoreKustomizationFile follows that protocol; new write
// sites must too.
func copyFile(srcPath, dstPath string, mode os.FileMode) error {
	// Hardlink fast path. Any Link failure (cross-device EXDEV,
	// permissions, source missing) falls through to the copy path so
	// unusual filesystems still work.
	if os.Link(srcPath, dstPath) == nil {
		return nil
	}
	src, err := os.Open(srcPath) //nolint:gosec // srcPath is a tree-walk result under our source root
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.Perm()) //nolint:gosec // dstPath is inside our staging tempdir
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}
