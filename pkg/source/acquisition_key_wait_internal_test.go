package source

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testAcquireCloseJoinsKeyWaitCase(t *testing.T, concurrency *int,
	backing *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		root := ""
		if (*backing) == "disk" {
			root = t.TempDir()
		}
		c := newTestHTTPInputs(t, root)
		url := "https://example.invalid/key"
		key := sourcecache.BlobKey{Content: struct{ URL string }{url}}
		entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			_, _, _, err := c.owner.Blob(
				t.Context(),
				key,
				&sourcecache.Policy{},
				func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					close(entered)
					<-release
					return sourcecache.ProducerResult{}, nil
				},
			)
			held <- err
		}()
		<-entered
		c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
			t.Error("key waiter reached producer after Close")
			return nil, context.Canceled
		})}
		done := make(chan error, (*concurrency))
		for range *concurrency {
			go func() {
				_, err := c.Acquire(
					t.Context(),
					&HTTPRequest{URL: url, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
				)
				done <- err
			}()
		}
		synctest.Wait()
		c.Close()
		for range *concurrency {
			{
				err := <-done
				assert.ErrorIf(t, !errors.Is(err, context.Canceled), "key waiter: %v", err)
			}
		}
		close(release)
		{
			err := <-held
			assert.NoError(t, err)
		}
	})
}
