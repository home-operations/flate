package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func testAcquireNetworkSuppressionCase(t *testing.T, concurrency *int,
) {
	t.Helper()
	var network Network
	c := newHTTPInputs(t, cachetest.New(t, "", sourcecache.New), &network, task.NewBounded(2))
	t.Cleanup(c.Close)
	var calls atomic.Int32
	var failing atomic.Bool
	failing.Store(true)
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			calls.Add(1)
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new run") }),
	)
	t.Cleanup(srv.Close)
	target := strings.Replace(srv.URL, "127.0.0.1", "network.test", 1)
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
	_, err := c.Acquire(t.Context(), target+"/first", retry, nil)
	first, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !first.Attempted, "first attempt: %v", err)
	cause, before := first.Err, calls.Load()
	assert.FatalIf(t, before == 0, "%s", "seed performed no DNS lookup")

	done := make(chan error, (*concurrency))
	for i := range *concurrency {
		go func() {
			_, acquireErr := c.Acquire(t.Context(), fmt.Sprintf("%s/%d", target, i), retry, nil)
			done <- acquireErr
		}()
	}
	for range *concurrency {
		innerErr := <-done
		terminal, ok := errors.AsType[*sourcecache.TerminalError](innerErr)
		assert.FatalIf(t, !ok || terminal.Attempted || !errors.Is(innerErr, cause), "suppression: %v", innerErr)
	}
	if calls.Load() != before {
		t.Fatalf("physical dials=%d want=1", calls.Load())
	}
	entries := 0
	c.remoteFetches.Range(func(_, _ any) bool { entries++; return true })
	assert.FatalIf(t, entries != 0, "retained terminal failures=%d", entries)
	fresh := newHTTPInputs(t, c.owner, nil, task.NewBounded(2))
	t.Cleanup(fresh.Close)
	failing.Store(false)
	input, err := fresh.Acquire(t.Context(), target+"/first", retry, nil)
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	assert.FatalIf(t, err != nil || string(body) != "new run", "cache shared negative state: %q %v", body, err)
}

func testAcquireNetworkSuppressionOwnerCancellationCase(
	t *testing.T, concurrency *int,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testAcquireNetworkSuppressionOwnerCancellationCaseCallback(
			t,
			testAcquireNetworkSuppressionOwnerCancellationCaseCallbackInputs{concurrency: &concurrency},
		)
	},
	)
}

type testAcquireNetworkSuppressionOwnerCancellationCaseCallbackInputs struct {
	concurrency **int
}

func testAcquireNetworkSuppressionOwnerCancellationCaseCallback(
	t *testing.T,
	testCaseInput testAcquireNetworkSuppressionOwnerCancellationCaseCallbackInputs,
) {
	t.Helper()
	var network Network
	owner := cachetest.New(t, "", sourcecache.New)
	c, other := newHTTPInputs(
		t,
		owner,
		&network,
		task.NewBounded((*(*testCaseInput.concurrency))),
	), newHTTPInputs(
		t,
		owner,
		&network,
		task.NewBounded((*(*testCaseInput.concurrency))),
	)
	testutil.StubDNS(t, func(context.Context, string) error { return os.ErrDeadlineExceeded })
	srv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "healthy") },
		),
	)
	entered := make(chan struct{}, (*(*testCaseInput.concurrency)))
	release := make(chan struct{})
	var senders sync.WaitGroup
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		c.Close()
		other.Close()
		senders.Wait()
		network.Close()
		srv.Close()
	})
	transport := c.httpClient.Transport
	c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		entered <- struct{}{}
		if r.URL.Path == "/0" {
			<-release
			return transport.RoundTrip(r)
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	done := make(chan error, (*(*testCaseInput.concurrency)))
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
	for i := range *(*testCaseInput.concurrency) {
		senders.Go(func() {
			_, err := c.Acquire(t.Context(), fmt.Sprintf("http://wave.test/%d", i), retry, nil)
			done <- err
		})
	}
	for range *(*testCaseInput.concurrency) {
		<-entered
	}
	close(release)
	first := <-done
	terminal, ok := errors.AsType[*sourcecache.TerminalError](first)
	assert.FatalIf(
		t,
		!ok || !terminal.Attempted || !fetcherr.DialTimeout(terminal.Err),
		"live admitted timeout: %v",
		first,
	)
	cause := terminal.Err
	c.Close()
	for range (*(*testCaseInput.concurrency)) - 1 {
		{
			err := <-done
			assert.ErrorIf(t, !errors.Is(err, context.Canceled), "canceled admitted request: %v", err)
		}
	}
	senders.Wait()
	_, err := other.Acquire(t.Context(), "http://wave.test/later", retry, nil)
	terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || terminal.Attempted || !errors.Is(err, cause), "suppressed request: %v", err)
	input, err := other.Acquire(t.Context(), srv.URL, retry, nil)
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	assert.FatalIf(t, err != nil || string(body) != "healthy", "healthy endpoint: %q %v", body, err)
}
