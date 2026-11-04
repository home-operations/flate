package source

import (
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/fetcherr"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testAcquireRetryTransientCacheClientErrorCase(
	t *testing.T, tc *struct {
		status int
		client bool
		retry  bool
	},
) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(tc.status)
			return
		}
		_, _ = w.Write([]byte("retried"))
	}))
	t.Cleanup(srv.Close)
	cache := newTestHTTPInputs(t, "")
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	input, err := cache.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: retry, HeldSlot: nil})
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	if !tc.retry {
		status, ok := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || status.StatusCode != tc.status || status.Op != http.MethodGet || hits.Load() != 1 {
			t.Fatalf("initial status=%+v err=%v hits=%d", status, err, hits.Load())
		}
		input, err = cache.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: retry, HeldSlot: nil})
		if err == nil {
			body, err = fs.ReadFile(input, "content")
		}
	}
	if tc.client {
		if fetcherr.Classes(err)&fetcherr.Client == 0 || hits.Load() != 1 {
			t.Fatalf("4xx cache: err=%v, hits=%d", err, hits.Load())
		}
	} else if err != nil || string(body) != "retried" || hits.Load() != 2 {
		t.Fatalf("retry or next fetch: body=%q, err=%v, hits=%d", body, err, hits.Load())
	}
}
