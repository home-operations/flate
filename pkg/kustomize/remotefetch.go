package kustomize

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
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
		acquired, err := c.FetchRemote(ctx, r.URL, input.retry, input.slot)
		if err != nil {
			return nil, remoteIdentity{}, err
		}
		return acquired, remoteIdentity{HTTP: cache.AcquisitionIdentity(acquired)}, nil
	case remoteGit:
		if c.gitBase == nil {
			return nil, remoteIdentity{}, fmt.Errorf("%w: kustomization references remote git base %q but no git fetcher is wired", manifest.ErrInput, r.URL)
		}
		var artifact *store.SourceArtifact
		var err error
		fetch := func() {
			if err = ctx.Err(); err == nil {
				artifact, err = c.gitBase(ctx, GitBaseRequest{URL: r.URL, Ref: r.Ref, MaxAge: input.maxAge}, input.retry)
			}
		}
		if input.slot == nil {
			done := make(chan struct{})
			c.tasks.Go(context.WithoutCancel(ctx), "kustomize/git-base", func(context.Context) {
				defer close(done)
				fetch()
			})
			<-done
		} else {
			fetch()
		}
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
// heldSlot is the borrowed service when the caller holds its slot, or nil.
func (c *TreeCache) FetchRemote(ctx context.Context, urlStr string, retry cache.RetryConfig, heldSlot *task.Service) (fs.FS, error) {
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
		c.tasks.Go(context.WithoutCancel(workerCtx), "kustomize/http-input", func(context.Context) {
			defer func() {
				if workers.state.Add(^uint64(0)) == remoteClosed {
					close(workers.drain)
				}
			}()
			defer func() {
				cancel()
				rf.cancel.Store(nil)
				close(rf.done)
			}()
			if err := workerCtx.Err(); err != nil {
				rf.err = fmt.Errorf("%w: remote fetch: %w", manifest.ErrFlux, err)
				c.remoteFetches.CompareAndDelete(key, rf)
				return
			}
			reader, _, _, err := c.owner.Blob(workerCtx, cache.BlobKey{Content: struct{ URL string }{urlStr}}, cache.Policy{Retry: retry}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
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
		})
	})
	var input fs.FS
	var err error
	wait := func() {
		select {
		case <-rf.done:
			input, err = rf.input, rf.err
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if heldSlot == nil {
		wait()
	} else {
		heldSlot.YieldSlot(wait)
	}
	return input, err
}
