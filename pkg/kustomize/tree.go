package kustomize

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/home-operations/flate/pkg/source"
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

// TreeCache owns one run's shared remote workers and borrows its backing Cache
// and Network. Callers MUST join renders and close TreeCache before the backing.
type TreeCache struct {
	// remoteFetches dedupes HTTP(S) resource fetches across every preflight
	// pass in one run: a kustomization URL reached via multiple Flux
	// Kustomizations is fetched once. See remotefetch.go.
	remoteFetches sync.Map // remoteKey -> *remoteFetch
	workers       atomic.Pointer[remoteWorkers]
	closeOnce     sync.Once

	// sourceRoots owns standalone source admission and shaping for this run.
	sourceRoots sync.Map

	// gitBase resolves a remote kustomize git base (a repo URL at a bare ref)
	// to an on-disk worktree. The one injected seam: pkg/kustomize cannot import
	// pkg/source/git (import cycle). nil ⇒ a git-base resource fails with a
	// clear error. See gitbase.go.
	gitBase GitBaseFetcher

	// owner is borrowed; the caller closes it after renders, workers and readers finish.
	owner      *cache.Cache
	network    *source.Network
	httpClient *http.Client
	transport  *http.Transport
}

const remoteClosed uint64 = 1 << 63

var closedRemoteWorkers = &remoteWorkers{}

type remoteWorkers struct {
	state atomic.Uint64 // closed bit and admitted worker count share one admission gate.
	drain chan struct{}
}

// NewTreeCache borrows the nonnil owner for source and completed-render storage.
// The caller MUST join renders, Close the TreeCache, and finish acquired readers
// before closing owner. Close never releases the borrowed owner or Network.
func NewTreeCache(owner *cache.Cache, network *source.Network) *TreeCache {
	tr, _ := source.NewHTTPTransport(nil, nil)
	return &TreeCache{owner: owner, network: network, transport: tr,
		httpClient: &http.Client{Timeout: remoteFetchTimeout, Transport: network.HTTPTransport(tr)}}
}

// Close freezes remote admission, cancels shared work and joins its complete
// acquisition and cleanup. It is terminal, idempotent and safe for concurrent
// callers. Callers MUST separately join renders and finish acquired readers
// before releasing backing storage. Close does not aggregate fetch errors.
func (c *TreeCache) Close() {
	c.closeOnce.Do(func() {
		workers := c.workers.Swap(closedRemoteWorkers)
		if workers != nil {
			if workers.state.Or(remoteClosed) == 0 {
				close(workers.drain)
			}
			c.remoteFetches.Range(func(_, value any) bool {
				if cancel := value.(*remoteFetch).cancel.Load(); cancel != nil {
					(*cancel)()
				}
				return true
			})
			<-workers.drain
		}
		c.transport.CloseIdleConnections()
		c.remoteFetches.Clear()
		c.sourceRoots.Clear()
	})
}

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
