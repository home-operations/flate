package source

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
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
				return testAcquireCloseResponse(r, &acquisitionCloseResponse{
					testCaseInput: &testCaseInput,
					entered:       &entered,
					canceled:      &canceled,
					release:       &release,
					completed:     &completed,
				})
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
		go func() { _, err := c.Acquire(ctx, &HTTPRequest{URL: url, Retry: retry, HeldSlot: nil}); callers <- err }()
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
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
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

type acquisitionCloseResponse struct {
	testCaseInput                         *testAcquireCloseJoinsAcquisitionCaseCallbackInputs
	entered, canceled, release, completed *chan struct{}
}

func testAcquireCloseResponse(r *http.Request, captures *acquisitionCloseResponse) error {
	defer close(*captures.completed)
	if (*(*captures.testCaseInput.stage)) == "cleanup" || (*(*captures.testCaseInput.stage)) == "publication" {
		close(*captures.entered)
		select {
		case <-r.Context().Done():
			close(*captures.canceled)
		case <-*captures.release:
		}
		<-*captures.release
		if (*(*captures.testCaseInput.stage)) == "cleanup" {
			return context.Canceled
		}
	}
	if (*(*captures.testCaseInput.stage)) == "backoff" {
		close(*captures.entered)
	}
	return nil
}
