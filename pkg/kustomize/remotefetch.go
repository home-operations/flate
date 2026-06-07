package kustomize

import (
	"context"
	"errors"
	"sync"
)

// remoteFetch carries the result of one URL fetch. The fetch runs in
// a single background goroutine (gated by start.Do) detached from any
// caller's ctx so a cancellation on the first caller doesn't poison
// the cached result for everyone else — the previous OnceValues+ctx
// capture would freeze ctx.Canceled into every subsequent FetchRemote
// call for the same URL. Callers select on their own ctx vs the
// done channel; the fetch runs to completion under the package-level
// remoteFetchTimeout.
type remoteFetch struct {
	start sync.Once
	done  chan struct{}
	body  []byte
	err   error
}

// FetchRemote returns the body of urlStr, fetched at most once per
// (url, success) cache entry. Successful bodies are cached for the
// TreeCache lifetime; transient errors (DNS, connection reset,
// timeout, 5xx) are NOT cached — the next caller retries. Only
// definitive HTTP 4xx responses are cached as negative entries
// (they won't change between retries within a run).
//
// Without the success-only cache, a single transient hiccup at
// orchestrator startup poisoned every subsequent reconcile of every
// KS referencing that URL for the rest of the run.
//
// The fetch runs in a background goroutine seeded with a detached
// context (httpGetURL applies remoteFetchTimeout internally) so a
// cancellation on the first caller doesn't propagate into the
// cached error. Each caller still honors its own ctx via the
// select below.
func (c *TreeCache) FetchRemote(ctx context.Context, urlStr string) ([]byte, error) {
	loaded, _ := c.remoteFetches.LoadOrStore(urlStr, &remoteFetch{done: make(chan struct{})})
	rf := loaded.(*remoteFetch)
	rf.start.Do(func() {
		go func() {
			rf.body, rf.err = httpGetURL(context.Background(), urlStr)
			close(rf.done)
			// On transient failure (network / 5xx / timeout — anything
			// that isn't a definitive 4xx), drop the cache entry so
			// the next caller retries instead of inheriting our
			// failure for the rest of the run. isHTTPClientError uses
			// errors.As against httpStatusError so it stays correct
			// even when the error is wrapped (e.g. "preflight: %w").
			if rf.err != nil && !isHTTPClientError(rf.err) {
				c.remoteFetches.CompareAndDelete(urlStr, rf)
			}
		}()
	})
	select {
	case <-rf.done:
		return rf.body, rf.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// isHTTPClientError reports whether err is a definitive HTTP 4xx
// response (which won't change between retries within one run).
// Anything else — transport errors, timeouts, 5xx — is treated as
// transient so the cache entry gets dropped.
//
// Uses errors.As against httpStatusError so the check stays correct
// when the error is wrapped (e.g. fmt.Errorf("preflight: %w", err)).
func isHTTPClientError(err error) bool {
	var hse *httpStatusError
	return errors.As(err, &hse) && hse.Code >= 400 && hse.Code < 500
}
