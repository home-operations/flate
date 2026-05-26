package git

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// materializeTree walks the tree at hash and writes every blob into
// root, preserving the on-disk layout the upstream commit produced.
// Submodule entries are warn-and-skipped: the bare mirror has no
// nested object stores, so resolving them would require a separate
// fetch that defeats the point of the mirror cache. Callers that need
// submodule support fall back to the legacy PlainClone path
// (Fetcher.fetch decides on Spec.RecurseSubmodules).
//
// Symlinks materialize as real OS symlinks rather than being collapsed
// to text files, so the rendered tree matches what a `git checkout`
// would produce — important for kustomize bases that follow symlinked
// component directories.
func materializeTree(repo *git.Repository, hash plumbing.Hash, root string) error {
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("load commit %s: %w", hash, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("load tree for %s: %w", hash, err)
	}

	walker := object.NewTreeWalker(tree, true, nil)
	defer walker.Close()
	for {
		name, entry, werr := walker.Next()
		if errors.Is(werr, io.EOF) {
			break
		}
		if werr != nil {
			return fmt.Errorf("walk tree: %w", werr)
		}
		if entry.Mode == filemode.Submodule {
			slog.Warn("git mirror: skipping submodule (mirror path does not recurse)", "path", name)
			continue
		}
		dst := filepath.Join(root, filepath.FromSlash(name))
		switch entry.Mode {
		case filemode.Symlink:
			blob, berr := repo.BlobObject(entry.Hash)
			if berr != nil {
				return fmt.Errorf("load symlink blob %s for %q: %w", entry.Hash, name, berr)
			}
			target, terr := readBlobBytes(blob)
			if terr != nil {
				return fmt.Errorf("read symlink target for %q: %w", name, terr)
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
				return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
			}
			if err := os.Symlink(string(target), dst); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", dst, target, err)
			}
		default:
			if !entry.Mode.IsFile() {
				continue
			}
			blob, berr := repo.BlobObject(entry.Hash)
			if berr != nil {
				return fmt.Errorf("load blob %s for %q: %w", entry.Hash, name, berr)
			}
			if err := writeBlobTo(dst, blob, entry.Mode); err != nil {
				return err
			}
		}
	}
	return nil
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

// writeBlobTo writes a blob to dst with mode-derived permissions
// (executable bit preserved from filemode.Executable). Creates the
// parent dir as needed.
func writeBlobTo(dst string, blob *object.Blob, mode filemode.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	perm := os.FileMode(0o600)
	if mode == filemode.Executable {
		perm = 0o700
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm) //nolint:gosec // dst is built from the mirror's commit tree under the cache slot
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
