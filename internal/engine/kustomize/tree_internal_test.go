package kustomize

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (internal/engine/kustomize cannot import internal/engine/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	called := false
	c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, cache.RetryConfig) (*store.SourceArtifact, error) {
		called = true
		//nolint:nilnil // The wiring witness invokes the fetcher without requiring an artifact.
		return nil, nil
	})

	assert.FatalIf(t, c.gitBase == nil, "%s", "git base fetcher not set")

	_, _ = c.gitBase(t.Context(), GitBaseRequest{URL: "https://example.com/x", Ref: "main"}, cache.RetryConfig{})

	assert.FatalIf(t, !called, "%s", "wired fetcher was not invoked")
}

func newTestTreeCache(tb testing.TB, root string) *TreeCache {
	tb.Helper()
	return newTestTreeCacheWithTasks(tb, cachetest.New(tb, root, cache.New), task.NewBounded(4))
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
	tb.Cleanup(func() {
		http.DefaultTransport = original
		tr.CloseIdleConnections()
	})
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
		defer func() { unblock(); c.remoteInputs.Close(); tasks.BlockTillDone() }()
		for range 2 {
			tasks.Go(t.Context(), "hold-slot", func(context.Context) { held <- struct{}{}; <-release })
		}
		<-held
		<-held

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			snap := readSetSnapshot{
				Remotes: []remoteEntry{
					{Input: remoteInput{Operation: remoteHTTP, URL: "https://example.invalid/queued"}},
				},
			}
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
		{
			err := <-done
			assert.FatalIf(t, !errors.Is(err, context.Canceled), "validation cancellation: %v", err)
		}
		c.remoteInputs.Close()
		tasks.BlockTillDone()

		assert.FatalIf(t, tasks.Failures() != 0, "%s", "task accounting corrupted")
	})
}
