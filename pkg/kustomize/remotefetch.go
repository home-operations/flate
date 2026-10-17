package kustomize

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type remoteOperation uint8

const (
	remoteHTTP remoteOperation = iota
	remoteGit
)

type remoteInput struct {
	Operation remoteOperation `json:"op"`
	URL       string          `json:"url"`
	Ref       string          `json:"ref,omitempty"`
}

type remoteIdentity struct {
	HTTP     string `json:"http,omitempty"`
	Key      string `json:"key,omitempty"`
	Revision string `json:"revision,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

func (r remoteInput) acquire(ctx context.Context, c *TreeCache, input *treeFS) (fs.FS, remoteIdentity, error) {
	switch r.Operation {
	case remoteHTTP:
		acquired, err := c.FetchRemote(ctx, r.URL, input.retry)
		if err != nil {
			return nil, remoteIdentity{}, err
		}
		return acquired, remoteIdentity{HTTP: cache.AcquisitionIdentity(acquired)}, nil
	case remoteGit:
		if c.gitBase == nil {
			return nil, remoteIdentity{}, fmt.Errorf("%w: kustomization references remote git base %q but no git fetcher is wired", manifest.ErrInput, r.URL)
		}
		artifact, err := c.gitBase(ctx, GitBaseRequest{URL: r.URL, Ref: r.Ref, MaxAge: input.maxAge}, input.retry)
		if err != nil {
			return nil, remoteIdentity{}, err
		}
		if artifact == nil || artifact.FS == nil || artifact.Key == "" || (artifact.Revision == "" && artifact.Digest == "") {
			return nil, remoteIdentity{}, fmt.Errorf("%w: remote git base has no immutable content", manifest.ErrInput)
		}
		return artifact.FS, remoteIdentity{Key: artifact.Key, Revision: artifact.Revision, Digest: artifact.Digest}, nil
	default:
		return nil, remoteIdentity{}, fmt.Errorf("%w: unknown remote operation %d", manifest.ErrInput, r.Operation)
	}
}

// remoteFetch owns one complete acquisition independently of waiter cancellation.
type remoteFetch struct {
	start  sync.Once
	cancel atomic.Pointer[context.CancelFunc]
	done   chan struct{}
	input  fs.FS
	err    error
}

type remoteKey struct {
	url   string
	retry cache.RetryConfig
}

// FetchRemote returns an immutable filesystem containing content and shares
// complete acquisitions with equal policies. Successes
// and definitive client errors remain available for this TreeCache's lifetime.
// Shared work outlives individual waiters and uses bounded HTTP requests. Only
// Close guarantees worker cleanup; a FetchRemote return does not join the owner.
func (c *TreeCache) FetchRemote(ctx context.Context, urlStr string, retry cache.RetryConfig) (fs.FS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workers := c.workers.Load()
	if workers == nil {
		workers = &remoteWorkers{drain: make(chan struct{})}
		if !c.workers.CompareAndSwap(nil, workers) {
			workers = c.workers.Load()
		}
	}
	if workers == closedRemoteWorkers {
		return nil, fmt.Errorf("%w: remote fetch: %w", manifest.ErrFlux, fs.ErrClosed)
	}
	key := remoteKey{url: urlStr, retry: retry}
	// NaN cannot be a comparable dictionary key value.
	if math.IsNaN(retry.Jitter) || math.IsInf(retry.Jitter, 0) {
		key.retry.Jitter = 0
	} else {
		key.retry.Jitter = max(0, min(1, retry.Jitter))
	}
	loaded, _ := c.remoteFetches.LoadOrStore(key, &remoteFetch{done: make(chan struct{})})
	rf := loaded.(*remoteFetch)
	rf.start.Do(func() {
		// Shared acquisition MUST outlive any one waiter's cancellation.
		workerCtx, cancel := context.WithCancel(context.Background())
		rf.cancel.Store(&cancel)
		for {
			state := workers.state.Load()
			if state&remoteClosed != 0 {
				cancel()
				rf.err = fmt.Errorf("%w: remote fetch: %w", manifest.ErrFlux, fs.ErrClosed)
				c.remoteFetches.CompareAndDelete(key, rf)
				rf.cancel.Store(nil)
				close(rf.done)
				return
			}
			if workers.state.CompareAndSwap(state, state+1) {
				break
			}
		}
		go func() {
			defer func() {
				if workers.state.Add(^uint64(0)) == remoteClosed {
					close(workers.drain)
				}
			}()
			reader, _, _, err := c.owner.Blob(workerCtx, cache.BlobKey{Content: struct{ URL string }{urlStr}}, cache.Policy{Retry: retry}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
				return c.httpGetURL(ctx, urlStr, w)
			})
			if err == nil {
				rf.input = cache.FileFS("content", reader)
				err = reader.Close()
			}
			rf.err = err
			if err != nil && fetcherr.Classes(err)&fetcherr.Client == 0 {
				c.remoteFetches.CompareAndDelete(key, rf)
			}
			cancel()
			rf.cancel.Store(nil)
			close(rf.done)
		}()
	})
	select {
	case <-rf.done:
		if rf.err != nil {
			return nil, rf.err
		}
		return rf.input, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
