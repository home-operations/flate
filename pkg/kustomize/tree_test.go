package kustomize

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (pkg/kustomize cannot import pkg/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	t.Parallel()
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

func newTestTreeCache(tb testing.TB, root string) *TreeCache {
	tb.Helper()
	c := newTestTreeCacheWithTasks(tb, cachetest.New(tb, root, cache.New), task.NewBounded(4))
	tb.Cleanup(c.Close)
	return c
}

func newTestTreeCacheWithTasks(tb testing.TB, owner *cache.Cache, tasks *task.Service) *TreeCache {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	tb.Cleanup(inputs.Close)
	return NewTreeCache(owner, &inputs, tasks)
}

func stubHTTPTransport(tb testing.TB, transport http.RoundTripper) {
	tb.Helper()
	original := http.DefaultTransport
	tr := original.(*http.Transport).Clone()
	tr.DialTLSContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			req, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			req.URL.Scheme, req.URL.Host = "https", req.Host
			resp, err := transport.RoundTrip(req.WithContext(ctx))
			if err != nil {
				return
			}
			defer resp.Body.Close()
			resp.Body = io.NopCloser(resp.Body)
			_ = resp.Write(server)
		}()
		return client, nil
	}
	http.DefaultTransport = tr
	tb.Cleanup(func() { http.DefaultTransport = original; tr.CloseIdleConnections() })
}

type remoteRoundTrip func(*http.Request) (*http.Response, error)

func (f remoteRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTreeCache_CloseAdmissionRace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tasks := task.NewBounded(2)
		owner := cachetest.New(t, "", cache.New)
		c := newTestTreeCacheWithTasks(t, owner, tasks)
		held, release := make(chan struct{}, 2), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer func() { unblock(); c.Close(); c.remoteInputs.Close(); tasks.BlockTillDone() }()
		for range 2 {
			tasks.Go(t.Context(), "hold-slot", func(context.Context) { held <- struct{}{}; <-release })
		}
		<-held
		<-held

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			snap := readSetSnapshot{Remotes: []remoteEntry{{Input: remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/queued"}}}}
			_, err := snap.stillValid(ctx, c, &treeFS{}, true)
			done <- err
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case <-done:
			t.Error("validation returned before its queued completion settled")
		default:
		}
		unblock()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("validation cancellation: %v", err)
		}
		c.Close()
		c.remoteInputs.Close()
		tasks.BlockTillDone()
		if tasks.Failures() != 0 {
			t.Fatal("task accounting corrupted")
		}
	})
}

func TestTreeCache_Close(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	c.sourceRoots.Store("source-root", func() (*sourceRoot, error) { return &sourceRoot{}, nil })
	c.Close()
	c.Close()
	c.sourceRoots.Range(func(_, _ any) bool { t.Error("source-root memo retained after Close"); return false })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "borrowed") }))
	defer server.Close()
	input, err := c.remoteInputs.Acquire(t.Context(), server.URL, cache.RetryConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := fs.ReadFile(input, "content")
	if err != nil || string(body) != "borrowed" {
		t.Fatalf("borrowed input after render Close: %q %v", body, err)
	}
}
