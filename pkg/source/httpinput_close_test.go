package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func newHTTPInputs(tb testing.TB, owner *cache.Cache, network *Network, tasks *task.Service) *HTTPInputs {
	tb.Helper()
	if network == nil {
		network = new(Network)
		tb.Cleanup(network.Close)
	}
	inputs := NewHTTPInputs(owner, network, tasks)
	tb.Cleanup(inputs.Close)
	return &inputs
}

func newTestHTTPInputs(tb testing.TB, root string) *HTTPInputs {
	tb.Helper()
	return newHTTPInputs(tb, cachetest.New(tb, root, cache.New), nil, task.NewBounded(4))
}

func closeTestHTTPInputs(t *testing.T, h *HTTPInputs) {
	t.Helper()
	h.Close()
	if err := h.owner.Close(); err != nil {
		t.Fatal(err)
	}
}

//nolint:tparallel // The lazy-selection subtest mutates the process proxy environment.
func TestHTTPInputs_Close(t *testing.T) {
	t.Run("lazy-selection", func(t *testing.T) {
		var hits atomic.Int32
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = io.WriteString(w, "proxied")
		}))
		defer proxy.Close()
		t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
		t.Setenv("http_proxy", "")
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")
		var network Network
		defer network.Close()
		c := newHTTPInputs(t, cachetest.New(t, "", cache.New), &network, task.NewBounded(2))
		defer c.Close()
		t.Setenv("HTTP_PROXY", proxy.URL)
		if c.httpClient.Timeout != 0 {
			t.Fatalf("resource client has a second timer: %v", c.httpClient.Timeout)
		}
		var body strings.Builder
		_, err := c.httpGetURL(t.Context(), "http://lazy.test/resource", cache.ProducerInput{Writer: &body})
		if err != nil || body.String() != "proxied" || hits.Load() != 1 {
			t.Fatalf("lazy Network policy: %q hits=%d err=%v", body.String(), hits.Load(), err)
		}
	})
	t.Run("borrowed-network", func(t *testing.T) {
		t.Parallel()
		var requests atomic.Int32
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, "%d", requests.Load())
		}))
		srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				requests.Add(1)
			}
		}
		srv.Start()
		defer srv.Close()
		network := new(Network)
		c := newHTTPInputs(t, cachetest.New(t, "", cache.New), network, task.NewBounded(2))
		defer network.Close()
		defer c.Close()
		client := NewHTTPClient(network, HTTPStream)
		fetch := func() string {
			t.Helper()
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			if err := errors.Join(err, resp.Body.Close()); err != nil {
				t.Fatal(err)
			}
			return string(body)
		}
		first := fetch()
		workers := &remoteWorkers{drain: make(chan struct{})}
		workers.state.Store(1)
		c.workers.Store(workers)
		canceled := make(chan struct{})
		cancel := context.CancelFunc(func() { close(canceled) })
		rf := &remoteFetch{}
		rf.cancel.Store(&cancel)
		c.remoteFetches.Store(remoteKey{url: srv.URL}, rf)
		closed := make(chan struct{})
		go func() { c.Close(); close(closed) }()
		<-canceled
		// Borrowed idle connections MUST survive owner teardown.
		during := fetch()
		workers.state.Add(^uint64(0))
		close(workers.drain)
		<-closed
		after := fetch()
		if during != first || after != first {
			t.Fatalf("Network cleanup order: first=%s during=%s after=%s", first, during, after)
		}
	})
	for _, concurrency := range []int{2, 4} {
		for _, stage := range []string{"unused", "idle", "published", "admitted-before-launch", "retired"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, stage), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					c := newTestHTTPInputs(t, "")
					c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
						t.Error("frozen worker launched")
						return nil, context.Canceled
					})
					var workers *remoteWorkers
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					rf := &remoteFetch{done: make(chan struct{})}
					rf.cancel.Store(&cancel)
					key := remoteKey{url: "https://example.invalid/resource"}
					if stage != "unused" {
						workers = &remoteWorkers{drain: make(chan struct{})}
						c.workers.Store(workers)
					}
					if stage == "published" {
						// A stale live pointer MUST still observe the freeze bit.
						workers.state.Store(remoteClosed)
						close(workers.drain)
						c.remoteFetches.Store(key, rf)
						_, err := c.Acquire(t.Context(), key.url, key.retry, nil)
						if !errors.Is(err, fs.ErrClosed) || !errors.Is(err, manifest.ErrFlux) || rf.cancel.Load() != nil {
							t.Fatalf("rejected admission: %v, cancel=%v", err, rf.cancel.Load())
						}
						if _, ok := c.remoteFetches.Load(key); ok {
							t.Fatal("rejected entry retained")
						}
					}
					counted := stage == "admitted-before-launch" || stage == "retired"
					if counted {
						workers.state.Store(1)
						c.remoteFetches.Store(key, rf)
						if stage == "retired" {
							cancel()
							rf.cancel.Store(nil)
							c.remoteFetches.CompareAndDelete(key, rf)
							close(rf.done)
						}
					}
					memos := []struct {
						name string
						memo *sync.Map
					}{
						{"remote", &c.remoteFetches},
					}
					if stage == "idle" {
						completed := &remoteFetch{done: make(chan struct{})}
						close(completed.done)
						c.remoteFetches.Store(key, completed)
						for _, memo := range memos {
							populated := false
							memo.memo.Range(func(_, _ any) bool { populated = true; return false })
							if !populated {
								t.Fatalf("%s memo empty before Close", memo.name)
							}
						}
					}
					done := make(chan struct{}, concurrency)
					go func() { c.Close(); done <- struct{}{} }()
					synctest.Wait()
					for range concurrency - 1 {
						go func() { c.Close(); done <- struct{}{} }()
					}
					if counted {
						if ctx.Err() == nil {
							t.Error("admitted worker did not receive owner cancellation")
						}
						select {
						case <-done:
							t.Error("Close returned before final accounting release")
							done <- struct{}{}
						default:
						}
						if stage != "retired" {
							c.remoteFetches.CompareAndDelete(key, rf)
							rf.cancel.Store(nil)
							close(rf.done)
						}
						if workers.state.Add(^uint64(0)) == remoteClosed {
							close(workers.drain)
						}
					}
					for range concurrency {
						<-done
					}
					for _, memo := range memos {
						memo.memo.Range(func(_, _ any) bool {
							t.Errorf("%s memo retained after Close", memo.name)
							return false
						})
					}
					if c.workers.Load() != closedRemoteWorkers {
						t.Fatal("Close lost terminal sentinel")
					}
					candidate := &remoteWorkers{drain: make(chan struct{})}
					if c.workers.CompareAndSwap(nil, candidate) {
						t.Fatal("paused initializer replaced closed sentinel")
					}
					if _, err := c.Acquire(t.Context(), key.url, key.retry, nil); !errors.Is(err, fs.ErrClosed) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("closed fetch: %v", err)
					}
					cancel()
					if _, err := c.Acquire(ctx, key.url, key.retry, nil); !errors.Is(err, context.Canceled) {
						t.Fatalf("already canceled waiter: %v", err)
					}
					c.Close()
				})
			})
		}
	}
}

func TestHTTPInputs_CloseDoesNotLeak(t *testing.T) {
	stacks := func() string {
		var buf bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 2); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	owned := func(s string) int {
		n := 0
		for stack := range strings.SplitSeq(s, "\n\n") {
			if strings.Contains(stack, "source.(*HTTPInputs).Acquire.func") ||
				strings.Contains(stack, "source/cache.(*Cache).acquire") ||
				strings.Contains(stack, "net/http.(*persistConn).") {
				n++
			}
		}
		return n
	}
	baseline := stacks()
	check := func(t *testing.T) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			current := stacks()
			if !strings.Contains(current, "source.(*HTTPInputs).Acquire.func") &&
				!strings.Contains(current, "source/cache.(*Cache).acquire") && owned(current) <= owned(baseline) {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("owned stacks remain after explicit joins\nbaseline:\n%s\nafter:\n%s", baseline, current)
				return
			}
			runtime.Gosched()
		}
	}
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			for range 3 {
				entered, served := make(chan struct{}, concurrency), make(chan struct{}, concurrency)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/warm" {
						_, _ = io.WriteString(w, "complete")
						return
					}
					defer func() { served <- struct{}{} }()
					_, _ = io.WriteString(w, "partial")
					w.(http.Flusher).Flush()
					entered <- struct{}{}
					<-r.Context().Done()
				}))
				var network Network
				c := newHTTPInputs(t, cachetest.New(t, "", cache.New), &network, task.NewBounded(concurrency+1))
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, concurrency)
				for i := range concurrency {
					go func() {
						_, err := c.Acquire(ctx, fmt.Sprintf("%s/%d", srv.URL, i), cache.RetryConfig{}, nil)
						done <- err
					}()
				}
				for range concurrency {
					<-entered
				}
				cancel()
				for range concurrency {
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				}
				if _, err := c.Acquire(t.Context(), srv.URL+"/warm", cache.RetryConfig{}, nil); err != nil {
					t.Error(err)
				}
				c.Close()
				network.Close()
				for range concurrency {
					<-served
				}
				check(t)
				srv.Close()
			}
		})
	}
	check(t)
}

func TestHTTPInputs_CloseAdmissionRace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tasks := task.NewBounded(2)
		c := newHTTPInputs(t, cachetest.New(t, "", cache.New), nil, tasks)
		held, release := make(chan struct{}, 2), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer func() { unblock(); c.Close(); tasks.BlockTillDone() }()
		for range 2 {
			tasks.Go(t.Context(), "hold-slot", func(context.Context) { held <- struct{}{}; <-release })
		}
		<-held
		<-held
		c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
			t.Error("canceled queued acquisition contacted its endpoint")
			return nil, context.Canceled
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := c.Acquire(ctx, "https://example.invalid/queued", cache.RetryConfig{}, nil)
			done <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter cancellation: %v", err)
		}
		workers := c.workers.Load()
		if workers == nil || workers.state.Load() != 1 {
			t.Fatal("queued owner was not counted")
		}
		closed := make(chan struct{}, 2)
		go func() { c.Close(); closed <- struct{}{} }()
		synctest.Wait()
		select {
		case <-closed:
			t.Error("Close returned before queued owner cleanup")
		default:
		}
		go func() { c.Close(); closed <- struct{}{} }()
		unblock()
		<-closed
		<-closed
		if workers.state.Load() != remoteClosed {
			t.Error("queued owner count leaked")
		}
		tasks.BlockTillDone()
		if c.workers.Load() != closedRemoteWorkers || tasks.Failures() != 0 {
			t.Fatal("terminal admission or task accounting corrupted")
		}
	})
}
