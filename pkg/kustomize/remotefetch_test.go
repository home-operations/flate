package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetchRemote_CancelDoesNotPoisonCache(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				hits := 0
				saved := remoteFetchClient
				remoteFetchClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					hits++
					if hits == 1 {
						close(entered)
						select {
						case <-release:
						case <-r.Context().Done():
							return nil, r.Context().Err()
						}
						return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("body: ok\n")), Header: make(http.Header)}, nil
				})}
				defer func() { remoteFetchClient = saved }()
				c := NewTreeCache(newTestCache(t, ""))
				retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
				ctx, cancel := context.WithCancel(t.Context())
				cancelled := make(chan error, 1)
				go func() { _, err := c.FetchRemote(ctx, "https://example.invalid/resource", retry); cancelled <- err }()
				<-entered
				done := make(chan error, concurrency-1)
				for range concurrency - 1 {
					go func() {
						body, err := c.FetchRemote(t.Context(), "https://example.invalid/resource", retry)
						if err == nil && string(body) != "body: ok\n" {
							err = fmt.Errorf("body=%q", body)
						}
						done <- err
					}()
				}
				synctest.Wait()
				cancel()
				if err := <-cancelled; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled waiter=%v", err)
				}
				close(release)
				for range concurrency - 1 {
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				if hits != 2 {
					t.Fatalf("requests=%d, want 2", hits)
				}
			})
		})
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
		{"429 transient", &httpStatusError{Code: 429}, false},
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

func TestFetchRemote_RetryTransientCacheClientError(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if hits.Add(1) == 1 {
					w.WriteHeader(status)
					return
				}
				_, _ = w.Write([]byte("retried"))
			}))
			t.Cleanup(srv.Close)
			cache := NewTreeCache(newTestCache(t, ""))
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
			body, err := cache.FetchRemote(t.Context(), srv.URL, retry)
			if status == http.StatusNotFound {
				_, err = cache.FetchRemote(t.Context(), srv.URL, retry)
			}
			if status == http.StatusNotFound {
				if !isHTTPClientError(err) || hits.Load() != 1 {
					t.Fatalf("4xx cache: err=%v, hits=%d", err, hits.Load())
				}
			} else if err != nil || string(body) != "retried" || hits.Load() != 2 {
				t.Fatalf("transient retry: body=%q, err=%v, hits=%d", body, err, hits.Load())
			}
		})
	}
}

type remoteRoundTrip func(*http.Request) (*http.Response, error)

func (f remoteRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestFetchRemote_PolicyIsolation(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			for _, equal := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/reverse-%t/equal-%t", concurrency, reverse, equal), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						entered, release := make(chan struct{}), make(chan struct{})
						hits := 0
						saved := remoteFetchClient
						remoteFetchClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
							hits++
							if hits == 1 {
								close(entered)
								select {
								case <-release:
								case <-r.Context().Done():
									return nil, r.Context().Err()
								}
							}
							return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
						})}
						defer func() { remoteFetchClient = saved }()
						c := NewTreeCache(newTestCache(t, ""))
						done := make(chan error, concurrency)
						launch := func(i int) {
							go func() {
								retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
								if !equal && i == 0 {
									retry.Attempts = 1
								}
								if equal {
									switch i % 3 {
									case 0:
										retry.Jitter = math.NaN()
									case 1:
										retry.Jitter = math.Inf(1)
									}
								}
								_, err := c.FetchRemote(t.Context(), "https://example.invalid/resource", retry)
								done <- err
							}()
						}
						first := 0
						if reverse {
							first = concurrency - 1
						}
						launch(first)
						<-entered
						for i := range concurrency {
							if i != first {
								launch(i)
							}
						}
						synctest.Wait()
						close(release)
						for range concurrency {
							err := <-done
							terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
							if !ok || !terminal.Attempted {
								t.Fatalf("terminal=%+v, error=%v", terminal, err)
							}
						}
						want := 4
						if equal {
							want = 3
						}
						if hits != want {
							t.Fatalf("requests=%d, want %d", hits, want)
						}
					})
				})
			}
		}
	}
}

func TestFetchRemote_PartialBodyRetry(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				switch hits.Add(1) {
				case 1:
					w.Header().Set("Content-Length", "20")
					_, _ = io.WriteString(w, "partial")
				case 2:
					_, _ = io.WriteString(w, "complete")
				default:
					_, _ = io.WriteString(w, "replacement")
				}
			}))
			defer srv.Close()
			root := ""
			if backing == "disk" {
				root = t.TempDir()
			}
			c := NewTreeCache(newTestCache(t, root))
			retry := sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
			body, err := c.FetchRemote(t.Context(), srv.URL, retry)
			if err != nil || string(body) != "complete" || hits.Load() != 2 {
				t.Fatalf("body=%q, error=%v, hits=%d", body, err, hits.Load())
			}
			body[0] = 'X'
			if body, err := c.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}); err != nil || string(body) != "replacement" {
				t.Fatalf("replacement body=%q, error=%v", body, err)
			}
			body, err = c.FetchRemote(t.Context(), srv.URL, retry)
			if err != nil || string(body) != "complete" || hits.Load() != 3 {
				t.Fatalf("completed generation body=%q, error=%v, hits=%d", body, err, hits.Load())
			}
		})
	}
}
