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

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func testAcquireCloseJoinsAcquisitionCase(t *testing.T, concurrency *int,
	storage *string,
	stage *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireCloseJoinsAcquisitionCaseCallback(
			t,
			testAcquireCloseJoinsAcquisitionCaseCallbackInputs{
				concurrency: &concurrency,
				storage:     &storage,
				stage:       &stage,
			},
		)
	},
	)
}

func testAcquireCloseJoinsKeyWaitCase(t *testing.T, concurrency *int,
	backing *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		root := ""
		if (*backing) == "disk" {
			root = t.TempDir()
		}
		c := newTestHTTPInputs(t, root)
		url := "https://example.invalid/key"
		key := sourcecache.BlobKey{Content: struct{ URL string }{url}}
		entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			_, _, _, err := c.owner.Blob(
				t.Context(),
				key,
				sourcecache.Policy{},
				func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					close(entered)
					<-release
					return sourcecache.ProducerResult{}, nil
				},
			)
			held <- err
		}()
		<-entered
		c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
			t.Error("key waiter reached producer after Close")
			return nil, context.Canceled
		})}
		done := make(chan error, (*concurrency))
		for range *concurrency {
			go func() { _, err := c.Acquire(t.Context(), url, sourcecache.RetryConfig{}, nil); done <- err }()
		}
		synctest.Wait()
		c.Close()
		for range *concurrency {
			{
				err := <-done
				assert.ErrorIf(t, !errors.Is(err, context.Canceled), "key waiter: %v", err)
			}
		}
		close(release)
		{
			err := <-held
			assert.NoError(t, err)
		}
	})
}

func testAcquireExactSuccessorCase(t *testing.T, concurrency *int,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := newTestHTTPInputs(t, "")
		entered, release := make(chan struct{}), make(chan struct{})
		c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
			close(entered)
			<-release
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		})}
		key := remoteKey{url: "https://example.invalid/successor", retry: sourcecache.RetryConfig{Attempts: 1}}
		done := make(chan error, (*concurrency))
		for range *concurrency {
			go func() { _, err := c.Acquire(t.Context(), key.url, key.retry, nil); done <- err }()
		}
		<-entered
		synctest.Wait()
		successor := &remoteFetch{done: make(chan struct{})}
		c.remoteFetches.Store(key, successor)
		close(release)
		for range *concurrency {
			{
				err := <-done
				assert.FatalIf(t, err == nil, "%s", "missing transient failure")
			}
		}
		{
			got, _ := c.remoteFetches.Load(key)
			assert.FatalIf(t, got != successor, "%s", "predecessor retired its successor")
		}
	})
}

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
		go func() { _, err := a.Acquire(ctx, srv.URL, sourcecache.RetryConfig{}, nil); adone <- err }()
	}
	<-entered
	url := srv.URL
	if !(*sameKey) {
		url += "/other"
	}
	for range *concurrency {
		go func() {
			input, err := b.Acquire(t.Context(), url, sourcecache.RetryConfig{}, nil)
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
		_, err := a.Acquire(
			t.Context(),
			srv.URL,
			sourcecache.RetryConfig{},
			nil,
		)
		assert.FatalIf(t, !errors.Is(
			err,
			fs.ErrClosed,
		), "closed owner: %v", err)
	}
	input, err := b.Acquire(t.Context(), srv.URL+"/after", sourcecache.RetryConfig{}, nil)
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
			input, err := c.Acquire(t.Context(), srv.URL, retry, nil)
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
	input, err := c.Acquire(t.Context(), srv.URL, sourcecache.RetryConfig{}, nil)
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
	input, err = c.Acquire(t.Context(), srv.URL, retry, nil)
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	if err != nil || string(body) != "complete" || hits.Load() != 3 {
		t.Fatalf("completed generation body=%q, error=%v, hits=%d", body, err, hits.Load())
	}
}

type testAcquireCloseJoinsAcquisitionCaseCallbackInputs struct {
	concurrency **int
	storage     **string
	stage       **string
}

func testAcquireCloseJoinsAcquisitionCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCloseJoinsAcquisitionCaseCallbackInputs,
) {
	t.Helper()
	root := ""
	if (*(*testCaseInput.storage)) == "disk" {
		root = t.TempDir()
	}
	c := newHTTPInputs(
		t,
		cachetest.New(t, root, sourcecache.New),
		nil,
		task.NewBounded((*(*testCaseInput.concurrency))),
	)
	t.Cleanup(c.Close)
	entered, canceled, release, completed := make(
		chan struct{},
	), make(
		chan struct{},
	), make(
		chan struct{},
	), make(
		chan struct{},
	)
	hits := 0
	c.httpClient = &http.Client{
		Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			hits++
			if (*(*testCaseInput.stage)) == "headers" {
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
			if (*(*testCaseInput.stage)) == "read" {
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
			if (*(*testCaseInput.stage)) == "backoff" {
				status = http.StatusServiceUnavailable
			}
			body := remoteBody{Reader: reader, close: func() error {
				defer close(completed)
				if (*(*testCaseInput.stage)) == "cleanup" || (*(*testCaseInput.stage)) == "publication" {
					close(entered)
					select {
					case <-r.Context().Done():
						close(canceled)
					case <-release:
					}
					<-release
					if (*(*testCaseInput.stage)) == "cleanup" {
						return context.Canceled
					}
				}
				if (*(*testCaseInput.stage)) == "backoff" {
					close(entered)
				}
				return nil
			}}
			return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
		}),
	}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour}
	url := "https://example.invalid/resource"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	callers := make(chan error, (*(*testCaseInput.concurrency)))
	for range *(*testCaseInput.concurrency) {
		go func() { _, err := c.Acquire(ctx, url, retry, nil); callers <- err }()
	}
	<-entered
	synctest.Wait()
	cancel()
	for range *(*testCaseInput.concurrency) {
		{
			err := <-callers
			assert.FatalIf(t, !errors.Is(err, context.Canceled), "waiter cancellation: %v", err)
		}
	}
	testAcquireCloseJoinsAcquisitionCaseCallbackCompletion(
		t,
		&testAcquireCloseJoinsAcquisitionCaseCallbackCompletionInputs{
			c:             &c,
			url:           &url,
			retry:         &retry,
			testCaseInput: &testCaseInput,
			canceled:      &canceled,
			release:       &release,
			completed:     &completed,
			hits:          &hits,
			root:          &root,
		},
	)
}

func testAcquireCloseJoinsAcquisitionCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquireCloseJoinsAcquisitionCaseCallbackCompletionInputs,
) {
	t.Helper()
	workers := (*captures.c).workers.Load()
	loaded, _ := (*captures.c).remoteFetches.Load(remoteKey{url: (*captures.url), retry: (*captures.retry)})
	rf := loaded.(*remoteFetch)
	closed := make(chan struct{}, (*(*captures.testCaseInput.concurrency)))
	go func() {
		(*captures.c).Close()
		closed <- struct{}{}
	}()
	synctest.Wait()
	for range (*(*captures.testCaseInput.concurrency)) - 1 {
		go func() {
			(*captures.c).Close()
			closed <- struct{}{}
		}()
	}
	if (*(*captures.testCaseInput.stage)) != "backoff" {
		select {
		case <-(*captures.canceled):
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
	close((*captures.release))
	for range *(*captures.testCaseInput.concurrency) {
		<-closed
	}
	<-(*captures.completed)
	<-rf.done
	if workers.state.Load() != remoteClosed || rf.cancel.Load() != nil || (*captures.hits) != 1 {
		t.Fatalf(
			"incomplete teardown: state=%d cancel=%v hits=%d",
			workers.state.Load(),
			rf.cancel.Load(), (*captures.hits),
		)
	}
	if (*(*captures.testCaseInput.stage)) != "publication" &&
		(!errors.Is(rf.err, context.Canceled) || !errors.Is(rf.err, manifest.ErrFlux)) {
		t.Fatalf("owner cancellation identity: %v", rf.err)
	}
	check := func(owner *sourcecache.Cache) {
		t.Helper()
		reader, _, _, err := owner.Blob(
			t.Context(),
			sourcecache.BlobKey{Content: struct{ URL string }{(*captures.url)}},
			sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		if err != nil {
			if !errors.Is(err, errdef.ErrNotFound) {
				t.Fatal(err)
			}
			return
		}
		body, readErr := fs.ReadFile(reader, "content")
		assert.FatalIf(
			t, (*(*captures.testCaseInput.stage)) !=
				"publication" || string(body) != "complete" || readErr != nil, "interrupted publication: %q %v",
			body,
			readErr,
		)
	}
	check((*captures.c).owner)
	{
		err := (*captures.c).owner.Close()
		assert.NoError(t, err)
	}

	if (*captures.root) != "" {
		check(cachetest.New(t, (*captures.root), sourcecache.New))
	}
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

type testAcquireCloseJoinsAcquisitionCaseCallbackCompletionInputs struct {
	c             **HTTPInputs
	url           *string
	retry         *sourcecache.RetryConfig
	testCaseInput *testAcquireCloseJoinsAcquisitionCaseCallbackInputs
	canceled      *chan struct{}
	release       *chan struct{}
	completed     *chan struct{}
	hits          *int
	root          *string
}
