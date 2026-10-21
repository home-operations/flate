package kustomize

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
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

	// pkg/kustomize cannot import pkg/source/git (import cycle), so gitBase
	// acquires immutable repository views through the injected fetcher.
	gitBase GitBaseFetcher

	// owner is borrowed; the caller closes it after renders, workers and readers finish.
	tasks      *task.Service
	owner      *cache.Cache
	network    *source.Network
	httpClient *http.Client
	ownNetwork bool
}

const remoteClosed uint64 = 1 << 63

var closedRemoteWorkers = &remoteWorkers{}

type remoteWorkers struct {
	state atomic.Uint64 // closed bit and admitted worker count share one admission gate.
	drain chan struct{}
}

// NewTreeCache borrows the nonnil storage owner and task service for this run.
// A nil Network creates a private Network released by Close.
// The caller MUST join renders, Close the TreeCache, and finish acquired readers
// before closing owner. Close never releases the borrowed owner or Network.
func NewTreeCache(owner *cache.Cache, network *source.Network, tasks *task.Service) *TreeCache {
	ownNetwork := network == nil
	if ownNetwork {
		network = new(source.Network)
	}
	client := source.NewHTTPClient(network, source.HTTPResource)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		req.Header.Del("If-None-Match")
		req.Header.Del("If-Modified-Since")
		return nil
	}
	return &TreeCache{owner: owner, network: network, tasks: tasks,
		ownNetwork: ownNetwork, httpClient: client}
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
		if c.ownNetwork {
			c.network.Close()
		}
		c.remoteFetches.Clear()
		c.sourceRoots.Clear()
	})
}

// SetGitBaseFetcher wires the git-clone capability used to resolve remote git
// bases referenced from a kustomization's resources:. The orchestrator calls
// this once at construction; library/test embedders may leave it unset.
func (c *TreeCache) SetGitBaseFetcher(fn GitBaseFetcher) { c.gitBase = fn }
