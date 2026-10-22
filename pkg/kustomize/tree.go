package kustomize

import (
	"context"
	"sync"
	"time"

	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// GitBaseRequest carries a base identity and the referencing Flux interval.
type GitBaseRequest struct {
	URL    string
	Ref    string
	MaxAge time.Duration
}

// GitBaseFetcher acquires an immutable repository view for private render mounts.
type GitBaseFetcher func(context.Context, GitBaseRequest, cache.RetryConfig) (*store.SourceArtifact, error)

// TreeCache borrows backing storage and HTTP inputs for one run's render state.
// Callers MUST join renders and Close before releasing borrowed dependencies.
type TreeCache struct {
	sourceRoots sync.Map
	// pkg/kustomize cannot import pkg/source/git (import cycle).
	gitBase      GitBaseFetcher
	tasks        *task.Service
	owner        *cache.Cache
	remoteInputs *source.HTTPInputs
}

// NewTreeCache borrows the nonnil storage owner, HTTP inputs and task service.
func NewTreeCache(owner *cache.Cache, remoteInputs *source.HTTPInputs, tasks *task.Service) *TreeCache {
	return &TreeCache{owner: owner, remoteInputs: remoteInputs, tasks: tasks}
}

// Close clears render state after callers join renders; borrowed inputs stay open.
func (c *TreeCache) Close() { c.sourceRoots.Clear() }

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
