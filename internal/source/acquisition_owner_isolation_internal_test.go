package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testAcquireOwnerIsolationCase(t *testing.T, concurrency *int,
	sharedNetwork *bool,
	sameKey *bool,
) {
	t.Helper()
	entered, served := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Owner") == "A" {
			close(entered)
			<-r.Context().Done()
			close(served)
			return
		}
		_, _ = io.WriteString(w, "healthy")
	}))
	t.Cleanup(srv.Close)
	owner := cachetest.New(t, "", sourcecache.New)
	na, nb := new(Network), new(Network)
	t.Cleanup(na.Close)
	t.Cleanup(nb.Close)
	if *sharedNetwork {
		nb = na
	}
	a, b := newHTTPInputs(
		t,
		owner,
		na,
		task.NewBounded(2),
	), newHTTPInputs(
		t,
		owner,
		nb,
		task.NewBounded(2),
	)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	transport := a.httpClient.Transport
	a.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Owner", "A")
		return transport.RoundTrip(r)
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	adone, bdone := make(chan error, (*concurrency)), make(chan error, (*concurrency))
	for range *concurrency {
		go func() {
			_, err := a.Acquire(ctx, &HTTPRequest{URL: srv.URL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil})
			adone <- err
		}()
	}
	<-entered
	url := srv.URL
	if !(*sameKey) {
		url += "/other"
	}
	for range *concurrency {
		go func() {
			input, err := b.Acquire(
				t.Context(),
				&HTTPRequest{URL: url, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
			)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if err == nil && string(body) != "healthy" {
				err = fmt.Errorf("body=%q", body)
			}
			bdone <- err
		}()
	}
	cancel()
	for range *concurrency {
		if err := <-adone; !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}
	a.Close()
	<-served
	for range *concurrency {
		{
			err := <-bdone
			assert.NoError(t, err)
		}
	}
	{
		_, err := a.Acquire(t.Context(), &HTTPRequest{URL: srv.URL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil})
		assert.FatalIf(t, !errors.Is(
			err,
			fs.ErrClosed,
		), "closed owner: %v", err)
	}
	input, err := b.Acquire(
		t.Context(),
		&HTTPRequest{URL: srv.URL + "/after", Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
	)
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	assert.FatalIf(
		t,
		err != nil || string(body) != "healthy",
		"borrowed backing/Network closed: %q %v",
		body,
		err,
	)
}
