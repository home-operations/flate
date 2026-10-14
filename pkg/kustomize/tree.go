package kustomize

import (
	"context"
	"sync"
	"time"

	"github.com/home-operations/flate/internal/diskcache"
	"github.com/home-operations/flate/pkg/store"
)

// GitBaseRequest carries a base identity and the referencing Flux interval.
type GitBaseRequest struct {
	URL    string
	Ref    string
	MaxAge time.Duration
}

// GitBaseFetcher acquires an immutable repository view for private render mounts.
type GitBaseFetcher func(context.Context, GitBaseRequest) (*store.SourceArtifact, error)

// TreeCache carries the state shared across one run's renders.
type TreeCache struct {
	// remoteFetches dedupes HTTP(S) resource fetches across every preflight
	// pass in one run: a kustomization URL reached via multiple Flux
	// Kustomizations is fetched once. See remotefetch.go.
	remoteFetches sync.Map // url string -> *remoteFetch

	// sourceRoots owns standalone source admission and shaping for this run.
	sourceRoots sync.Map

	// gitBase resolves a remote kustomize git base (a repo URL at a bare ref)
	// to an on-disk worktree. The one injected seam: pkg/kustomize cannot import
	// pkg/source/git (import cycle). nil ⇒ a git-base resource fails with a
	// clear error. See gitbase.go.
	gitBase GitBaseFetcher

	// render is the cross-run, content-validated kustomize render cache (see
	// render_cache.go). nil ⇒ disabled; every render rebuilds. Set once at
	// construction via SetRenderCache.
	render *renderCache
}

// NewTreeCache constructs an empty render cache.
func NewTreeCache() *TreeCache { return &TreeCache{} }

// SetRenderCache enables the cross-run render cache, persisting under dir with a
// capBytes disk cap. Empty dir, non-positive cap or unsafe build metadata
// disables persistence. The orchestrator calls this once at construction;
// embedders/tests may leave it unset for an always-render
// TreeCache.
func (c *TreeCache) SetRenderCache(dir string, capBytes int64) {
	s, identity := diskcache.NewStore(dir, capBytes)
	c.render = newRenderCache(s, identity)
}

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
