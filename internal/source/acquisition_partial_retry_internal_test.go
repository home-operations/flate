package source

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
)

func testAcquirePartialBodyRetryCase(t *testing.T, concurrency *int,
) {
	t.Helper()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			testAcquirePartialBodyRetryCaseCase(
				t,
				testAcquirePartialBodyRetryCaseCaseInputs{concurrency: &concurrency, backing: &backing},
			)
		},
		)
	}
}

type testAcquirePartialBodyRetryCaseCaseInputs struct {
	concurrency **int
	backing     *string
}

func testAcquirePartialBodyRetryCaseCase(t *testing.T, testCaseInput testAcquirePartialBodyRetryCaseCaseInputs) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, callbackArg2 *http.Request) {
		testAcquirePartialBodyRetryCaseCaseCallback(
			w,
			callbackArg2,
			testAcquirePartialBodyRetryCaseCaseCallbackInputs{hits: &hits},
		)
	},
	))
	defer srv.Close()
	root := ""
	if (*testCaseInput.backing) == "disk" {
		root = t.TempDir()
	}
	c := newTestHTTPInputs(t, root)
	retry := sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	bodies := make(chan []byte, (*(*testCaseInput.concurrency)))
	for range *(*testCaseInput.concurrency) {
		go func() {
			input, err := c.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: retry, HeldSlot: nil})
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			assert.ErrorIf(t, err != nil || string(body) != "complete", "body=%q, error=%v", body, err)
			bodies <- body
		}()
	}
	var body []byte
	for range *(*testCaseInput.concurrency) {
		body = <-bodies
	}
	if hits.Load() != 2 {
		t.Fatalf("requests=%d, want 2", hits.Load())
	}
	body[0] = 'X'
	input, err := c.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil})
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	assert.FatalIf(
		t,
		err != nil || string(body) != "replacement",
		"replacement body=%q, error=%v",
		body,
		err,
	)
	input, err = c.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: retry, HeldSlot: nil})
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	if err != nil || string(body) != "complete" || hits.Load() != 3 {
		t.Fatalf("completed generation body=%q, error=%v, hits=%d", body, err, hits.Load())
	}
}

type testAcquirePartialBodyRetryCaseCaseCallbackInputs struct {
	hits *atomic.Int32
}

func testAcquirePartialBodyRetryCaseCaseCallback(
	w http.ResponseWriter,
	_ *http.Request,
	testCaseInput testAcquirePartialBodyRetryCaseCaseCallbackInputs,
) {
	switch testCaseInput.hits.Add(1) {
	case 1:
		w.Header().Set("Content-Length", "20")
		_, _ = io.WriteString(w, "partial")
	case 2:
		_, _ = io.WriteString(w, "complete")
	default:
		_, _ = io.WriteString(w, "replacement")
	}
}
