package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (pkg/kustomize cannot import pkg/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	c := newTestTreeCache(t, "")
	called := false
	c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, cache.RetryConfig) (*store.SourceArtifact, error) {
		called = true
		return nil, nil
	})
	if c.gitBase == nil {
		t.Fatal("git base fetcher not set")
	}
	_, _ = c.gitBase(t.Context(), GitBaseRequest{URL: "https://example.com/x", Ref: "main"}, cache.RetryConfig{})
	if !called {
		t.Fatal("wired fetcher was not invoked")
	}
}

func newTestCache(t testing.TB, root string) *cache.Cache {
	t.Helper()
	owner, err := cache.New(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return owner
}

func newTestTreeCache(t testing.TB, root string) *TreeCache {
	t.Helper()
	c := NewTreeCache(newTestCache(t, root), nil)
	t.Cleanup(c.Close)
	return c
}

func TestTreeCache_Close(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, stage := range []string{"unused", "idle", "published", "admitted-before-launch", "retired"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, stage), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := newTestTreeCache(t, "")
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
						_, err := c.FetchRemote(t.Context(), key.url, key.retry)
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
						{"source-root", &c.sourceRoots},
					}
					if stage == "idle" {
						completed := &remoteFetch{done: make(chan struct{})}
						close(completed.done)
						c.remoteFetches.Store(key, completed)
						c.sourceRoots.Store("source-root", func() (*sourceRoot, error) { return &sourceRoot{}, nil })
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
					if _, err := c.FetchRemote(t.Context(), key.url, key.retry); !errors.Is(err, fs.ErrClosed) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("closed fetch: %v", err)
					}
					cancel()
					if _, err := c.FetchRemote(ctx, key.url, key.retry); !errors.Is(err, context.Canceled) {
						t.Fatalf("already canceled waiter: %v", err)
					}
					c.Close()
				})
			})
		}
	}
}

func TestTreeCache_CloseDoesNotLeak(t *testing.T) {
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
			if strings.Contains(stack, "kustomize.(*TreeCache).FetchRemote.func") ||
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
			if !strings.Contains(current, "kustomize.(*TreeCache).FetchRemote.func") &&
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
				c := newTestTreeCache(t, "")
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, concurrency)
				for i := range concurrency {
					go func() {
						_, err := c.FetchRemote(ctx, fmt.Sprintf("%s/%d", srv.URL, i), cache.RetryConfig{})
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
				if _, err := c.FetchRemote(t.Context(), srv.URL+"/warm", cache.RetryConfig{}); err != nil {
					t.Error(err)
				}
				c.Close()
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

func TestTreeCache_CloseAdmissionRace(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			owner := newTestCache(t, "")
			for range 40 {
				c := NewTreeCache(owner, nil)
				c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				})
				start, done, closed := make(chan struct{}), make(chan error, concurrency), make(chan struct{}, concurrency)
				for range concurrency {
					go func() {
						<-start
						_, err := c.FetchRemote(t.Context(), "https://example.invalid/race", cache.RetryConfig{})
						done <- err
					}()
					go func() { <-start; c.Close(); closed <- struct{}{} }()
				}
				close(start)
				for range concurrency {
					<-closed
					if err := <-done; !errors.Is(err, fs.ErrClosed) && !errors.Is(err, context.Canceled) {
						t.Errorf("admission race: %v", err)
					}
				}
				if c.workers.Load() != closedRemoteWorkers {
					t.Fatal("initializer overwrote terminal sentinel")
				}
			}
		})
	}
}
