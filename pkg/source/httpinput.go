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
	"github.com/home-operations/flate/internal/httpconditional"
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

//nolint:gochecknoglobals // This pointer is the shared identity sentinel for terminal worker admission.
var closedRemoteWorkers = &remoteWorkers{}

type remoteWorkers struct {
	state atomic.Uint64 // closed bit and admitted worker count share one admission gate.
	drain chan struct{}
}

// NewHTTPInputs borrows the nonnil storage owner, Network and task service.
func NewHTTPInputs(owner *cache.Cache, network *Network, tasks *task.Service) HTTPInputs {
	client := &http.Client{Transport: network, CheckRedirect: httpconditional.Redirect}
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
				if cancel := value.(*remoteFetch).cancel.Load(); cancel != nil { //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
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
	accounting  manifest.AcquisitionWork
	acquisition string
	start       sync.Once
	cancel      atomic.Pointer[context.CancelFunc]
	done        chan struct{}
	input       fs.FS
	err         error
}

type remoteKey struct {
	url   string
	retry cache.RetryConfig
}

// Acquire returns an immutable filesystem containing content and shares
// complete acquisitions with equal policies. Successes
// and definitive client errors remain available for this owner's lifetime.
// Shared work outlives individual waiters. Only
// Close guarantees worker cleanup; an Acquire return does not join the owner.
// heldSlot is the borrowed service when the caller holds its slot, or nil.
func (h *HTTPInputs) Acquire(ctx context.Context, requestValue *HTTPRequest) (fs.FS, error) {
	request := *requestValue

	urlStr, retry, heldSlot := request.URL, request.Retry, request.HeldSlot

	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	workers := h.remoteWorkers()
	if workers == closedRemoteWorkers {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("remote fetch: %w", fs.ErrClosed), "")
	}
	key := remoteKey{url: urlStr, retry: retry}
	key.normalize()
	candidate := &remoteFetch{done: make(chan struct{})}
	loaded, _ := h.remoteFetches.LoadOrStore(key, candidate)
	rf := loaded.(*remoteFetch) //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
	winner := false
	rf.start.Do(func() {
		winner = true
		rf.accounting.Scope = request.Work
		// Shared acquisition MUST outlive any one waiter's cancellation.
		workerCtx, cancel := context.WithCancel(context.Background())
		rf.cancel.Store(&cancel)
		if !workers.admit() {
			cancel()
			rf.err = manifest.NewError(manifest.ErrFlux, fmt.Errorf("remote fetch: %w", fs.ErrClosed), "")
			h.remoteFetches.CompareAndDelete(key, rf)
			rf.cancel.Store(nil)
			close(rf.done)
			return
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
			rf.acquire(workerCtx, h, key, retry)
		})
	})
	var input fs.FS
	var err error
	served := false
	wait := func() {
		select {
		case <-rf.done:
			input, err = rf.input, rf.err
			served = true
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if heldSlot == nil {
		wait()
	} else {
		heldSlot.YieldSlot(wait)
	}
	if !winner && served && rf.acquisition != "" {
		work := request.Work.Record(manifest.WorkCache, rf.acquisition)
		work.Add(&manifest.WorkCounts{Attempts: 1, CacheService: 1})
		work.Finish(err)
	}
	return input, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
}

//nolint:nonamedreturns // Deferred body Close must join the returned error, including during panic unwinding.
func (h *HTTPInputs) httpGetURL(
	ctx context.Context,
	urlStr string,
	w cache.ProducerInput,
) (result cache.ProducerResult, resultErr error) {
	previous, err := w.Previous(ctx)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, http.NoBody)
	if err != nil {
		return cache.ProducerResult{}, fmt.Errorf("create source HTTP request: %w", err)
	}
	if httpconditional.Apply(req, previous) {
		work := w.Work().Validate()
		work.Add(&manifest.WorkCounts{Attempts: 1})
		defer func() { work.Finish(resultErr) }()
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return cache.ProducerResult{}, err //nolint:wrapcheck // The HTTP client already returns a URL error identifying method and target, including net.Error behavior.
	}
	defer func() { resultErr = errors.Join(resultErr, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusNotModified && (resp.StatusCode < 200 || resp.StatusCode > 299) {
		return cache.ProducerResult{}, &fetcherr.ResponseError{StatusCode: resp.StatusCode, Op: http.MethodGet}
	}
	result.Descriptor, result.Unchanged, err = httpconditional.Metadata(req, resp, previous)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	if result.Unchanged {
		return result, nil
	}
	w.Work().Physical().Add(&manifest.WorkCounts{RemoteTransfers: 1})
	size, err := io.Copy(w, resp.Body)
	if err != nil {
		return cache.ProducerResult{}, fmt.Errorf("copy source content: %w", err)
	}
	result.Size = size
	return result, nil
}

func (h *HTTPInputs) remoteWorkers() *remoteWorkers {
	workers := h.workers.Load()
	if workers == nil {
		workers = &remoteWorkers{drain: make(chan struct{})}
		if !h.workers.CompareAndSwap(nil, workers) {
			workers = h.workers.Load()
		}
	}
	return workers
}

func (rf *remoteFetch) acquire(ctx context.Context, h *HTTPInputs, key remoteKey, retry cache.RetryConfig) {
	if err := ctx.Err(); err != nil {
		rf.err = manifest.NewError(manifest.ErrFlux, fmt.Errorf("remote fetch: %w", err), "")
		h.remoteFetches.CompareAndDelete(key, rf)
		return
	}
	generation, _, acquisition, err := h.owner.Blob(
		ctx,
		cache.BlobKey{Content: struct{ URL string }{key.url}},
		&cache.Policy{Retry: retry, Work: &rf.accounting},
		func(ctx context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			return h.httpGetURL(ctx, key.url, w)
		},
	)
	if err == nil {
		rf.input = cache.NameBlob("content", generation)
		rf.acquisition = acquisition
	}
	rf.err = err
	if physical := rf.accounting.Physical(); physical != nil {
		rf.acquisition = physical.Key.Input
	}
	if err != nil && fetcherr.Classes(err)&fetcherr.Client == 0 {
		h.remoteFetches.CompareAndDelete(key, rf)
	}
}

func (k *remoteKey) normalize() {
	// NaN cannot be a comparable dictionary key value.
	if math.IsNaN(k.retry.Jitter) || math.IsInf(k.retry.Jitter, 0) {
		k.retry.Jitter = 0
	} else {
		k.retry.Jitter = max(0, min(1, k.retry.Jitter))
	}
}

// HTTPRequest carries one remote input and its caller's execution facts.
type HTTPRequest struct {
	URL      string
	Retry    cache.RetryConfig
	HeldSlot *task.Service
	Work     manifest.WorkScope
}

func (w *remoteWorkers) admit() bool {
	for {
		state := w.state.Load()
		if state&remoteClosed != 0 {
			return false
		}
		if w.state.CompareAndSwap(state, state+1) {
			return true
		}
	}
}
