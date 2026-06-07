package kustomize

// tree.go holds the per-run render cache. With the memory-over-disk overlay
// (see flux.go RenderFlux), source trees are read straight from disk — no
// per-run snapshot or copy — so this is now just the small amount of shared
// state a run's renders need: the HTTP remote-resource fetch dedup table and
// the injected git-base fetcher.

import (
	"context"
	"sync"
)

// GitBaseFetcher materializes a remote kustomize git base — a repo URL at a
// bare, undifferentiated ref (tag, branch, commit, or "" for the default
// branch) — into an on-disk worktree, returning its absolute path and the
// resolved revision. The orchestrator supplies a closure over its
// *git.Fetcher; this package only ever sees the function value, keeping it free
// of the pkg/source → pkg/kustomize import cycle.
type GitBaseFetcher func(ctx context.Context, repoURL, ref string) (localPath, revision string, err error)

// TreeCache carries the state shared across one run's renders.
type TreeCache struct {
	// remoteFetches dedupes HTTP(S) resource fetches across every preflight
	// pass in one run: a kustomization URL reached via multiple Flux
	// Kustomizations is fetched once. See remotefetch.go.
	remoteFetches sync.Map // url string -> *remoteFetch

	// gitBase resolves a remote kustomize git base (a repo URL at a bare ref)
	// to an on-disk worktree. The one injected seam: pkg/kustomize cannot import
	// pkg/source/git (import cycle). nil ⇒ a git-base resource fails with a
	// clear error. See gitbase.go.
	gitBase GitBaseFetcher
}

// NewTreeCache constructs an empty render cache.
func NewTreeCache() *TreeCache { return &TreeCache{} }

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
