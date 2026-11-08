package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
)

func testAcquireCancelDoesNotPoisonCacheCase(
	t *testing.T, concurrency *int,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireCancelDoesNotPoisonCacheCaseCallback(
			t,
			testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs{concurrency: &concurrency},
		)
	},
	)
}

type testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs struct {
	concurrency **int
}

func testAcquireCancelDoesNotPoisonCacheCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs,
) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	hits := 0
	c := newTestHTTPInputs(t, "")
	c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		hits++
		if hits == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("body: ok\n")),
			Header:     make(http.Header),
		}, nil
	})}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	ctx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	go func() {
		_, err := c.Acquire(ctx, &HTTPRequest{URL: "https://example.invalid/resource", Retry: retry, HeldSlot: nil})
		cancelled <- err
	}()
	<-entered
	done := make(chan error, (*(*testCaseInput.concurrency))-1)
	for range (*(*testCaseInput.concurrency)) - 1 {
		go func() {
			input, err := c.Acquire(
				t.Context(),
				&HTTPRequest{URL: "https://example.invalid/resource", Retry: retry, HeldSlot: nil},
			)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if err == nil && string(body) != "body: ok\n" {
				err = fmt.Errorf("body=%q", body)
			}
			done <- err
		}()
	}
	synctest.Wait()
	cancel()
	{
		err := <-cancelled
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancelled waiter=%v", err)
	}
	close(release)
	for range (*(*testCaseInput.concurrency)) - 1 {
		{
			err := <-done
			assert.NoError(t, err)
		}
	}
	assert.FatalIf(t, hits != 2, "requests=%d, want 2", hits)
	{
		_, err := c.Acquire(ctx, &HTTPRequest{URL: "https://example.invalid/resource", Retry: retry, HeldSlot: nil})
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "canceled warm waiter: %v", err)
	}
}
