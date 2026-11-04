package source

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testAcquireExactSuccessorCase(t *testing.T, concurrency *int,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := newTestHTTPInputs(t, "")
		entered, release := make(chan struct{}), make(chan struct{})
		c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
			close(entered)
			<-release
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		})}
		key := remoteKey{url: "https://example.invalid/successor", retry: sourcecache.RetryConfig{Attempts: 1}}
		done := make(chan error, (*concurrency))
		for range *concurrency {
			go func() {
				_, err := c.Acquire(t.Context(), &HTTPRequest{URL: key.url, Retry: key.retry, HeldSlot: nil})
				done <- err
			}()
		}
		<-entered
		synctest.Wait()
		successor := &remoteFetch{done: make(chan struct{})}
		c.remoteFetches.Store(key, successor)
		close(release)
		for range *concurrency {
			{
				err := <-done
				assert.FatalIf(t, err == nil, "%s", "missing transient failure")
			}
		}
		{
			got, _ := c.remoteFetches.Load(key)
			assert.FatalIf(t, got != successor, "%s", "predecessor retired its successor")
		}
	})
}
