package kustomize

import (
	"context"
	"errors"
	"fmt"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"oras.land/oras-go/v2/errdef"
)

func TestFetchRemote_CancelDoesNotPoisonCache(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				hits := 0
				c := newTestTreeCache(t, "")
				c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
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
				if _, err := c.FetchRemote(ctx, "https://example.invalid/resource", retry); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled warm waiter: %v", err)
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
			cache := newTestTreeCache(t, "")
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
						c := newTestTreeCache(t, "")
						c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
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
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
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
					c := newTestTreeCache(t, root)
					retry := sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
					bodies := make(chan []byte, concurrency)
					for range concurrency {
						go func() {
							body, err := c.FetchRemote(t.Context(), srv.URL, retry)
							if err != nil || string(body) != "complete" {
								t.Errorf("body=%q, error=%v", body, err)
							}
							bodies <- body
						}()
					}
					var body []byte
					for range concurrency {
						body = <-bodies
					}
					if hits.Load() != 2 {
						t.Fatalf("requests=%d, want 2", hits.Load())
					}
					body[0] = 'X'
					if body, err := c.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}); err != nil || string(body) != "replacement" {
						t.Fatalf("replacement body=%q, error=%v", body, err)
					}
					body, err := c.FetchRemote(t.Context(), srv.URL, retry)
					if err != nil || string(body) != "complete" || hits.Load() != 3 {
						t.Fatalf("completed generation body=%q, error=%v, hits=%d", body, err, hits.Load())
					}
				})
			}
		})
	}
}

func TestFetchRemote_NetworkSuppression(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var network source.Network
			c := NewTreeCache(newTestCache(t, ""), &network)
			t.Cleanup(c.Close)
			tr, _ := source.NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			var calls atomic.Int32
			cause := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
			tr.DialContext = func(context.Context, string, string) (net.Conn, error) { calls.Add(1); return nil, cause }
			c.transport = tr
			c.httpClient = &http.Client{Transport: network.HTTPTransport(tr)}
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
			_, err := c.FetchRemote(t.Context(), "http://network.test/first", retry)
			first, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !first.Attempted || !errors.Is(err, cause) {
				t.Fatalf("first attempt: %v", err)
			}
			done := make(chan error, concurrency)
			for i := range concurrency {
				go func() {
					_, err := c.FetchRemote(t.Context(), fmt.Sprintf("http://network.test/%d", i), retry)
					done <- err
				}()
			}
			for range concurrency {
				err := <-done
				terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
				if !ok || terminal.Attempted || !errors.Is(err, cause) {
					t.Fatalf("suppression: %v", err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("physical dials=%d want=1", calls.Load())
			}
			entries := 0
			c.remoteFetches.Range(func(_, _ any) bool { entries++; return true })
			if entries != 0 {
				t.Fatalf("retained terminal failures=%d", entries)
			}
			fresh := NewTreeCache(c.owner, new(source.Network))
			t.Cleanup(fresh.Close)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new run") }))
			t.Cleanup(srv.Close)
			tr, _ = source.NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
			}
			fresh.transport = tr
			fresh.httpClient = &http.Client{Transport: fresh.network.HTTPTransport(tr)}
			body, err := fresh.FetchRemote(t.Context(), "http://network.test/first", retry)
			if err != nil || string(body) != "new run" {
				t.Fatalf("cache shared negative state: %q %v", body, err)
			}
		})
	}
}

type remoteRead func([]byte) (int, error)

func (f remoteRead) Read(p []byte) (int, error) { return f(p) }

type remoteBody struct {
	io.Reader
	close func() error
}

func (b remoteBody) Close() error { return b.close() }

func TestFetchRemote_CloseJoinsAcquisition(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			for _, stage := range []string{"headers", "read", "cleanup", "publication", "backoff"} {
				t.Run(fmt.Sprintf("%d/%s/%s", concurrency, backing, stage), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						root := ""
						if backing == "disk" {
							root = t.TempDir()
						}
						c := newTestTreeCache(t, root)
						entered, canceled, release, completed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
						hits := 0
						c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
							hits++
							if stage == "headers" {
								close(entered)
								select {
								case <-r.Context().Done():
									close(canceled)
								case <-release:
								}
								<-release
								close(completed)
								return nil, context.Canceled
							}
							status := http.StatusOK
							var reader io.Reader = strings.NewReader("complete")
							if stage == "read" {
								reader = remoteRead(func([]byte) (int, error) {
									close(entered)
									select {
									case <-r.Context().Done():
										close(canceled)
									case <-release:
									}
									<-release
									return 0, context.Canceled
								})
							}
							if stage == "backoff" {
								status = http.StatusServiceUnavailable
							}
							body := remoteBody{Reader: reader, close: func() error {
								defer close(completed)
								if stage == "cleanup" || stage == "publication" {
									close(entered)
									select {
									case <-r.Context().Done():
										close(canceled)
									case <-release:
									}
									<-release
									if stage == "cleanup" {
										return context.Canceled
									}
								}
								if stage == "backoff" {
									close(entered)
								}
								return nil
							}}
							return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
						})}
						retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour}
						url := "https://example.invalid/resource"
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						callers := make(chan error, concurrency)
						for range concurrency {
							go func() { _, err := c.FetchRemote(ctx, url, retry); callers <- err }()
						}
						<-entered
						synctest.Wait()
						cancel()
						for range concurrency {
							if err := <-callers; !errors.Is(err, context.Canceled) {
								t.Fatalf("waiter cancellation: %v", err)
							}
						}
						workers := c.workers.Load()
						loaded, _ := c.remoteFetches.Load(remoteKey{url: url, retry: retry})
						rf := loaded.(*remoteFetch)
						closed := make(chan struct{}, concurrency)
						go func() { c.Close(); closed <- struct{}{} }()
						synctest.Wait()
						for range concurrency - 1 {
							go func() { c.Close(); closed <- struct{}{} }()
						}
						if stage != "backoff" {
							select {
							case <-canceled:
							default:
								t.Error("orphaned acquisition did not receive owner cancellation")
							}
							select {
							case <-closed:
								t.Error("Close returned before synchronous cleanup completed")
								closed <- struct{}{}
							default:
							}
						}
						close(release)
						for range concurrency {
							<-closed
						}
						<-completed
						<-rf.done
						if workers.state.Load() != remoteClosed || rf.cancel.Load() != nil || hits != 1 {
							t.Fatalf("incomplete teardown: state=%d cancel=%v hits=%d", workers.state.Load(), rf.cancel.Load(), hits)
						}
						if stage != "publication" && (!errors.Is(rf.err, context.Canceled) || !errors.Is(rf.err, manifest.ErrFlux)) {
							t.Fatalf("owner cancellation identity: %v", rf.err)
						}
						check := func(owner *sourcecache.Cache) {
							t.Helper()
							reader, _, _, err := owner.Blob(t.Context(), sourcecache.BlobKey{Content: struct{ URL string }{url}}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
							if err != nil {
								if !errors.Is(err, errdef.ErrNotFound) {
									t.Fatal(err)
								}
								return
							}
							body, readErr := io.ReadAll(reader)
							closeErr := reader.Close()
							if stage != "publication" || string(body) != "complete" || errors.Join(readErr, closeErr) != nil {
								t.Fatalf("interrupted publication: %q %v %v", body, readErr, closeErr)
							}
						}
						check(c.owner)
						if err := c.owner.Close(); err != nil {
							t.Fatal(err)
						}
						if root != "" {
							check(newTestCache(t, root))
						}
					})
				})
			}
		}
	}
}

func TestFetchRemote_CloseJoinsKeyWait(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					root := ""
					if backing == "disk" {
						root = t.TempDir()
					}
					c := newTestTreeCache(t, root)
					url := "https://example.invalid/key"
					key := sourcecache.BlobKey{Content: struct{ URL string }{url}}
					entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
					go func() {
						r, _, _, err := c.owner.Blob(t.Context(), key, sourcecache.Policy{}, func(context.Context, io.Writer) (ocispec.Descriptor, error) {
							close(entered)
							<-release
							return ocispec.Descriptor{}, nil
						})
						if err == nil {
							err = r.Close()
						}
						held <- err
					}()
					<-entered
					c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
						t.Error("key waiter reached producer after Close")
						return nil, context.Canceled
					})}
					done := make(chan error, concurrency)
					for range concurrency {
						go func() { _, err := c.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}); done <- err }()
					}
					synctest.Wait()
					c.Close()
					for range concurrency {
						if err := <-done; !errors.Is(err, context.Canceled) {
							t.Errorf("key waiter: %v", err)
						}
					}
					close(release)
					if err := <-held; err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	}
}

func TestFetchRemote_ExactSuccessor(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newTestTreeCache(t, "")
				entered, release := make(chan struct{}), make(chan struct{})
				c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
					close(entered)
					<-release
					return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
				})}
				key := remoteKey{url: "https://example.invalid/successor", retry: sourcecache.RetryConfig{Attempts: 1}}
				done := make(chan error, concurrency)
				for range concurrency {
					go func() { _, err := c.FetchRemote(t.Context(), key.url, key.retry); done <- err }()
				}
				<-entered
				synctest.Wait()
				successor := &remoteFetch{done: make(chan struct{})}
				c.remoteFetches.Store(key, successor)
				close(release)
				for range concurrency {
					if err := <-done; err == nil {
						t.Fatal("missing transient failure")
					}
				}
				if got, _ := c.remoteFetches.Load(key); got != successor {
					t.Fatal("predecessor retired its successor")
				}
			})
		})
	}
}

func TestFetchRemote_OwnerIsolation(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, sharedNetwork := range []bool{false, true} {
			for _, sameKey := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/network-%t/key-%t", concurrency, sharedNetwork, sameKey), func(t *testing.T) {
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
					owner := newTestCache(t, "")
					na, nb := new(source.Network), new(source.Network)
					if sharedNetwork {
						nb = na
					}
					a, b := NewTreeCache(owner, na), NewTreeCache(owner, nb)
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
					adone, bdone := make(chan error, concurrency), make(chan error, concurrency)
					for range concurrency {
						go func() { _, err := a.FetchRemote(ctx, srv.URL, sourcecache.RetryConfig{}); adone <- err }()
					}
					<-entered
					url := srv.URL
					if !sameKey {
						url += "/other"
					}
					for range concurrency {
						go func() {
							body, err := b.FetchRemote(t.Context(), url, sourcecache.RetryConfig{})
							if err == nil && string(body) != "healthy" {
								err = fmt.Errorf("body=%q", body)
							}
							bdone <- err
						}()
					}
					cancel()
					for range concurrency {
						if err := <-adone; !errors.Is(err, context.Canceled) {
							t.Error(err)
						}
					}
					a.Close()
					<-served
					for range concurrency {
						if err := <-bdone; err != nil {
							t.Fatal(err)
						}
					}
					if _, err := a.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}); !errors.Is(err, fs.ErrClosed) {
						t.Fatalf("closed owner: %v", err)
					}
					if body, err := b.FetchRemote(t.Context(), srv.URL+"/after", sourcecache.RetryConfig{}); err != nil || string(body) != "healthy" {
						t.Fatalf("borrowed backing/Network closed: %q %v", body, err)
					}
				})
			}
		}
	}
}

func TestFetchRemote_NetworkSuppressionOwnerCancellation(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var network source.Network
			owner := newTestCache(t, "")
			c, other := NewTreeCache(owner, &network), NewTreeCache(owner, &network)
			t.Cleanup(c.Close)
			t.Cleanup(other.Close)
			entered, dialDone := make(chan struct{}, concurrency), make(chan struct{}, concurrency)
			timeout, release := make(chan struct{}), make(chan struct{})
			cause := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
			var calls atomic.Int32
			c.transport.Proxy = nil
			c.transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
				defer func() { dialDone <- struct{}{} }()
				i := calls.Add(1)
				entered <- struct{}{}
				if i == 1 {
					<-timeout
					return nil, cause
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return nil, cause
				}
			}
			// HTTPTransport MUST wrap after the private dialer is configured.
			c.httpClient.Transport = network.HTTPTransport(c.transport)
			done := make(chan error, concurrency)
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
			for i := range concurrency {
				go func() {
					_, err := c.FetchRemote(t.Context(), fmt.Sprintf("http://wave.test/%d", i), retry)
					done <- err
				}()
			}
			for range concurrency {
				<-entered
			}
			close(timeout)
			first := <-done
			terminal, ok := errors.AsType[*sourcecache.TerminalError](first)
			if !ok || !terminal.Attempted || !errors.Is(first, cause) {
				t.Fatalf("live admitted timeout: %v", first)
			}
			c.Close()
			close(release)
			for range concurrency - 1 {
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("canceled admitted request: %v", err)
				}
			}
			for range concurrency {
				<-dialDone
			}
			_, err := other.FetchRemote(t.Context(), "http://wave.test/later", retry)
			terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
			if !ok || terminal.Attempted || !errors.Is(err, cause) || calls.Load() != int32(concurrency) {
				t.Fatalf("suppressed request: %v calls=%d", err, calls.Load())
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "healthy") }))
			t.Cleanup(srv.Close)
			if body, err := other.FetchRemote(t.Context(), srv.URL, retry); err != nil || string(body) != "healthy" {
				t.Fatalf("healthy endpoint: %q %v", body, err)
			}
		})
	}
}
