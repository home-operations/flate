package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

// HTTPInputs owns one run's shared HTTP acquisitions and borrows its dependencies.
// It MUST NOT be copied after use; Close joins workers before backing is released.
type HTTPInputs struct {
	remoteFetches sync.Map
	workers       atomic.Pointer[remoteWorkers]
	closeOnce     sync.Once
	tasks         *task.Service
	owner         *cache.Cache
	httpClient    *http.Client
}

const remoteClosed uint64 = 1 << 63

var closedRemoteWorkers = &remoteWorkers{}

type remoteWorkers struct {
	state atomic.Uint64 // closed bit and admitted worker count share one admission gate.
	drain chan struct{}
}

// remoteFetchMaxBytes caps each HTTP input response body. A
// kustomization resource is almost always under a megabyte of YAML;
// 64 MiB is several orders of magnitude of headroom that still
// bounds the OOM blast radius from a malicious or accidentally-huge
// URL endpoint.
const remoteFetchMaxBytes = 64 << 20 // 64 MiB

const (
	httpOriginalURLAnnotation  = "flate.http-original-url"
	httpETagAnnotation         = "flate.http-etag"
	httpLastModifiedAnnotation = "flate.http-last-modified"
)

// NewHTTPInputs borrows the nonnil storage owner, Network and task service.
func NewHTTPInputs(owner *cache.Cache, network *Network, tasks *task.Service) HTTPInputs {
	client := NewHTTPClient(network, HTTPResource)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		req.Header.Del("If-None-Match")
		req.Header.Del("If-Modified-Since")
		return nil
	}
	return HTTPInputs{owner: owner, tasks: tasks, httpClient: client}
}

// Close freezes remote admission, cancels shared work and joins its complete
// acquisition and cleanup. It is terminal, idempotent and safe for concurrent
// callers. Callers MUST separately join renders and finish acquired readers
// before releasing backing storage. Close does not aggregate fetch errors.
func (h *HTTPInputs) Close() {
	h.closeOnce.Do(func() {
		workers := h.workers.Swap(closedRemoteWorkers)
		if workers != nil {
			if workers.state.Or(remoteClosed) == 0 {
				close(workers.drain)
			}
			h.remoteFetches.Range(func(_, value any) bool {
				if cancel := value.(*remoteFetch).cancel.Load(); cancel != nil {
					(*cancel)()
				}
				return true
			})
			<-workers.drain
		}
		h.remoteFetches.Clear()
	})
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

// Acquire returns an immutable filesystem containing content and shares
// complete acquisitions with equal policies. Successes
// and definitive client errors remain available for this owner's lifetime.
// Shared work outlives individual waiters and uses bounded HTTP requests. Only
// Close guarantees worker cleanup; an Acquire return does not join the owner.
// heldSlot is the borrowed service when the caller holds its slot, or nil.
func (h *HTTPInputs) Acquire(ctx context.Context, urlStr string, retry cache.RetryConfig, heldSlot *task.Service) (fs.FS, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	workers := h.workers.Load()
	if workers == nil {
		workers = &remoteWorkers{drain: make(chan struct{})}
		if !h.workers.CompareAndSwap(nil, workers) {
			workers = h.workers.Load()
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
	loaded, _ := h.remoteFetches.LoadOrStore(key, &remoteFetch{done: make(chan struct{})})
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
				h.remoteFetches.CompareAndDelete(key, rf)
				rf.cancel.Store(nil)
				close(rf.done)
				return
			}
			if workers.state.CompareAndSwap(state, state+1) {
				break
			}
		}
		h.tasks.Go(context.WithoutCancel(workerCtx), "source/http-input", func(context.Context) {
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
				h.remoteFetches.CompareAndDelete(key, rf)
				return
			}
			reader, _, _, err := h.owner.Blob(workerCtx, cache.BlobKey{Content: struct{ URL string }{urlStr}}, cache.Policy{Retry: retry}, func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
				return h.httpGetURL(ctx, urlStr, w)
			})
			if err == nil {
				rf.input = cache.FileFS("content", reader)
				err = reader.Close()
			}
			rf.err = err
			if err != nil && fetcherr.Classes(err)&fetcherr.Client == 0 {
				h.remoteFetches.CompareAndDelete(key, rf)
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
func (h *HTTPInputs) httpGetURL(ctx context.Context, urlStr string, w cache.ProducerInput) (result cache.ProducerResult, resultErr error) {
	fetchCtx, cancel := context.WithTimeout(ctx, HTTPResource.Timeout())
	defer cancel()
	previous, err := w.Previous(fetchCtx)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, urlStr, nil)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	if previous != nil && previous.Annotations[httpOriginalURLAnnotation] == "true" {
		if etag := previous.Annotations[httpETagAnnotation]; etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		if modified := previous.Annotations[httpLastModifiedAnnotation]; modified != "" {
			req.Header.Set("If-Modified-Since", modified)
		}
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, resp.Body.Close()) }()
	if resp.StatusCode == http.StatusNotModified && previous != nil && (resp.Request.Header.Get("If-None-Match") != "" || resp.Request.Header.Get("If-Modified-Since") != "") {
		result.Descriptor, result.Unchanged = *previous, true
	} else if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return cache.ProducerResult{}, &fetcherr.ResponseError{StatusCode: resp.StatusCode, Op: http.MethodGet}
	} else {
		result.Annotations = make(map[string]string)
		requestURL, responseURL := *req.URL, *resp.Request.URL
		if requestURL.User != nil && responseURL.User != nil && *requestURL.User == *responseURL.User {
			responseURL.User = requestURL.User
		}
		if requestURL == responseURL {
			result.Annotations[httpOriginalURLAnnotation] = "true"
		}
	}
	for _, validator := range [...]struct{ header, annotation string }{{"ETag", httpETagAnnotation}, {"Last-Modified", httpLastModifiedAnnotation}} {
		if _, supplied := resp.Header[http.CanonicalHeaderKey(validator.header)]; supplied {
			result.Annotations[validator.annotation] = resp.Header.Get(validator.header)
		}
	}
	if result.Unchanged {
		return result, nil
	}
	// The extra byte detects overflow without buffering an unbounded response.
	size, err := io.Copy(w, io.LimitReader(resp.Body, remoteFetchMaxBytes+1))
	if err != nil {
		return cache.ProducerResult{}, err
	}
	if size > remoteFetchMaxBytes {
		return cache.ProducerResult{}, fmt.Errorf("response body exceeds %d-byte cap", remoteFetchMaxBytes)
	}
	result.Size = size
	return result, nil
}
