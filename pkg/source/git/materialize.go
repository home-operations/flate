package git

import (
	"context"
	"log/slog"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/source/gittree"
)

// materializeTree walks the tree at hash and writes every blob into
// root via the shared gittree.Materialize helper. Submodule entries
// are warn-and-skipped: the bare mirror has no nested object stores,
// so resolving them would require a separate fetch that defeats the
// point of the mirror cache. Callers that need submodule support fall
// back to the legacy PlainClone path (Fetcher.fetch decides on
// Spec.RecurseSubmodules).
//
// Symlinks materialize as real OS symlinks rather than being collapsed
// to text files, so the rendered tree matches what a `git checkout`
// would produce — important for kustomize bases that follow symlinked
// component directories.
func materializeTree(ctx context.Context, repo *git.Repository, hash plumbing.Hash, root string) error {
	return gittree.Materialize(ctx, repo, hash, root, gittree.Options{
		OnSubmodule: func(path string) {
			slog.Warn("git mirror: skipping submodule (mirror path does not recurse)", "path", path)
		},
	})
}
