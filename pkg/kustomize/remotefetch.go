package kustomize

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"sync"

	"github.com/home-operations/flate/pkg/source/cache"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// remoteFetch owns one complete acquisition independently of waiter cancellation.
type remoteFetch struct {
	start sync.Once
	done  chan struct{}
	input fs.FS
	err   error
}

type remoteKey struct {
	url   string
	retry cache.RetryConfig
}

// FetchRemote shares complete acquisitions with equal policies. Successes
// and definitive client errors remain available for this TreeCache's lifetime.
// Shared work outlives individual waiters and uses bounded HTTP requests.
func (c *TreeCache) FetchRemote(ctx context.Context, urlStr string, retry cache.RetryConfig) ([]byte, error) {
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
		go func() {
			// A canceled waiter MUST not cancel another render's acquisition.
			reader, _, err := c.owner.Blob(context.Background(), cache.BlobKey{ID: "remote-http-" + urlHash(urlStr)}, cache.Policy{Retry: retry}, func(ctx context.Context, w io.Writer) (ocispec.Descriptor, error) {
				return httpGetURL(ctx, urlStr, w)
			})
			if err == nil {
				rf.input = cache.FileFS("content", reader)
				err = reader.Close()
			}
			rf.err = err
			if err != nil && !isHTTPClientError(err) {
				c.remoteFetches.CompareAndDelete(key, rf)
			}
			close(rf.done)
		}()
	})
	select {
	case <-rf.done:
		if rf.err != nil {
			return nil, rf.err
		}
		return fs.ReadFile(rf.input, "content")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func isHTTPClientError(err error) bool {
	status, ok := errors.AsType[*httpStatusError](err)
	return ok && status.Code >= 400 && status.Code < 500 && status.Code != 429
}
