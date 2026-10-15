package kustomize

import (
	"context"
	"sync"
	"time"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// GitBaseRequest carries a base identity and the referencing Flux interval.
type GitBaseRequest struct {
	URL    string
	Ref    string
	MaxAge time.Duration
}

// GitBaseFetcher acquires an immutable repository view for private render mounts.
type GitBaseFetcher func(context.Context, GitBaseRequest, cache.RetryConfig) (*store.SourceArtifact, error)

// TreeCache carries the state shared across one run's renders.
type TreeCache struct {
	// remoteFetches dedupes HTTP(S) resource fetches across every preflight
	// pass in one run: a kustomization URL reached via multiple Flux
	// Kustomizations is fetched once. See remotefetch.go.
	remoteFetches sync.Map // remoteKey -> *remoteFetch

	// sourceRoots owns standalone source admission and shaping for this run.
	sourceRoots sync.Map

	// gitBase resolves a remote kustomize git base (a repo URL at a bare ref)
	// to an on-disk worktree. The one injected seam: pkg/kustomize cannot import
	// pkg/source/git (import cycle). nil ⇒ a git-base resource fails with a
	// clear error. See gitbase.go.
	gitBase GitBaseFetcher

	// owner is borrowed; the caller closes it after all renders finish.
	owner *cache.Cache
}

// NewTreeCache borrows the nonnil owner for source and completed-render storage.
// The caller MUST close owner after every render and acquired reader finishes.
func NewTreeCache(owner *cache.Cache) *TreeCache { return &TreeCache{owner: owner} }

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
