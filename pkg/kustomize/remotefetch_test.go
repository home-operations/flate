package kustomize

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestFetchRemote_CancelDoesNotPoisonCache pins the ctx-detach contract: a
// cancel on caller A must not freeze context.Canceled into the cached result
// for caller B, and the URL is fetched at most once (singleflight).
func TestFetchRemote_CancelDoesNotPoisonCache(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("body: ok\n"))
	}))
	t.Cleanup(srv.Close)

	cache := NewTreeCache()

	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() {
		_, err := cache.FetchRemote(ctxA, srv.URL+"/x.yaml")
		doneA <- err
	}()

	for hits.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancelA()
	if err := <-doneA; err == nil {
		t.Fatal("caller A should have seen ctx.Err()")
	}

	close(release)
	body, err := cache.FetchRemote(context.Background(), srv.URL+"/x.yaml")
	if err != nil {
		t.Errorf("caller B got poisoned error: %v", err)
	}
	if string(body) != "body: ok\n" {
		t.Errorf("caller B got wrong body: %q", body)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server hit %d times; want 1 (singleflight broken)", got)
	}
}

// TestIsHTTPClientError confirms only definitive 4xx responses are treated as
// permanent (cacheable) errors; everything else is transient.
func TestIsHTTPClientError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"400", &httpStatusError{Code: 400}, true},
		{"404", &httpStatusError{Code: 404}, true},
		{"418", &httpStatusError{Code: 418}, true},
		{"499", &httpStatusError{Code: 499}, true},
		{"500 transient", &httpStatusError{Code: 500}, false},
		{"200 never-client-error", &httpStatusError{Code: 200}, false},
		{"wrapped 404", fmt.Errorf("preflight: %w", &httpStatusError{Code: 404}), true},
		{"wrapped 500", fmt.Errorf("preflight: %w", &httpStatusError{Code: 500}), false},
		{"transport error", fmt.Errorf("connection refused"), false},
		{"deadline", fmt.Errorf("context deadline exceeded"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isHTTPClientError(tc.err); got != tc.want {
			t.Errorf("isHTTPClientError(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
