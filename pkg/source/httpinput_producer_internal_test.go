package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestHttpGetURL_AcceptsLargeBody(t *testing.T) {
	t.Parallel()
	const size = (64 << 20) + 1
	c := newTestHTTPInputs(t, "")
	closes := 0
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		reader := io.LimitReader(remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), size)
		body := remoteBody{Reader: reader, close: func() error { closes++; return nil }}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})
	desc, err := c.httpGetURL(
		t.Context(),
		"https://example.invalid/big.yaml",
		sourcecache.ProducerInput{Writer: io.Discard},
	)
	if err != nil || desc.Size != size || closes != 1 {
		t.Fatalf("large body: size=%d closes=%d error=%v, want %d bytes and one close", desc.Size, closes, err, size)
	}
}

func TestHttpGetURL_BodyCloseErrors(t *testing.T) {
	t.Parallel()
	readErr, writeErr, closeErr := errors.New("body read"), errors.New("body write"), errors.New("body close")
	for _, outcome := range []string{"success", "created", "last-success", "status", "read", "large-read", "write"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			testHTTPGetURLBodyCloseErrorsCase(
				t,
				testHTTPGetURLBodyCloseErrorsCaseInputs{
					readErr:  &readErr,
					writeErr: &writeErr,
					closeErr: &closeErr,
					outcome:  &outcome,
				},
			)
		},
		)
	}
}

func TestHttpGetURL_RequestLifetime(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"headers", "body", "caller-deadline", "previous", "close"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testHTTPGetURLRequestLifetimeCase(t, &stage)
		},
		)
	}
	t.Run(
		"network-body-cancellation",
		func(t *testing.T) { t.Parallel(); testHTTPGetURLRequestLifetimeNetworkBodyCancellation(t) },
	)
}

type testHTTPGetURLBodyCloseErrorsCaseInputs struct {
	readErr  *error
	writeErr *error
	closeErr *error
	outcome  *string
}

func testHTTPGetURLBodyCloseErrorsCase(t *testing.T, testCaseInput testHTTPGetURLBodyCloseErrorsCaseInputs) {
	t.Helper()

	c := newTestHTTPInputs(t, "")
	closes := 0
	c.httpClient.Transport = remoteRoundTrip(func(callbackArg1 *http.Request) (*http.Response, error) {
		return testHTTPGetURLBodyCloseErrorsCaseCallback(
			callbackArg1,
			testHTTPGetURLBodyCloseErrorsCaseCallbackInputs{testCaseInput: &testCaseInput, closes: &closes},
		)
	},
	)
	writer := io.Discard
	if (*testCaseInput.outcome) == "write" {
		reader, pipe := io.Pipe()
		{
			err := reader.CloseWithError((*testCaseInput.writeErr))
			assert.NoError(t, err)
		}

		defer pipe.Close()
		writer = pipe
	}
	_, err := c.httpGetURL(
		t.Context(),
		"https://example.invalid/body",
		sourcecache.ProducerInput{Writer: writer},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, (*testCaseInput.closeErr)) || closes != 1 ||
			strings.Count(err.Error(), (*testCaseInput.closeErr).Error()) != 1,
		"body cleanup: %v closes=%d",
		err,
		closes,
	)
	assert.FatalIf(
		t,
		((*testCaseInput.outcome) == "read" || (*testCaseInput.outcome) == "large-read") && !errors.Is(err, (*testCaseInput.readErr)) || (*testCaseInput.outcome) ==
			"status" && fetcherr.Classes(err)&fetcherr.Client == 0 ||
			(*testCaseInput.outcome) ==
				"write" && !errors.Is(err, (*testCaseInput.writeErr)),
		"operation identity: %v",
		err,
	)
	if (*testCaseInput.outcome) == "status" {
		status, ok := errors.AsType[*fetcherr.ResponseError](err)
		assert.FatalIf(
			t,
			!ok || status.StatusCode != http.StatusNotFound || status.Op != http.MethodGet,
			"response facts=%+v error=%v",
			status,
			err,
		)
	}
	assert.FatalIf(
		t,
		((*testCaseInput.outcome) == "status" || (*testCaseInput.outcome) == "read" || (*testCaseInput.outcome) == "large-read" || (*testCaseInput.outcome) == "write") &&
			!strings.HasSuffix(err.Error(), "\n"+(*testCaseInput.closeErr).Error()),
		"operation must precede cleanup: %v",
		err,
	)
}

func testHTTPGetURLRequestLifetimeCase(t *testing.T, stage *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testHTTPGetURLRequestLifetimeCaseCallback(t, testHTTPGetURLRequestLifetimeCaseCallbackInputs{stage: &stage})
	},

	//nolint:fatcontext // The callback records the context for later lifetime assertions.

	)
}

func testHTTPGetURLRequestLifetimeNetworkBodyCancellation(t *testing.T) {
	t.Helper()

	entered, canceled := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthy" {
			_, _ = io.WriteString(w, "healthy")
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(canceled)
	}))
	defer srv.Close()
	var network Network
	defer network.Close()
	c := newTestHTTPInputs(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		resp, err := network.RoundTrip(r)
		if err != nil || r.URL.Path == "/healthy" {
			return resp, err
		}
		body := resp.Body
		enteredBody := sync.OnceFunc(func() { close(entered) })
		resp.Body = remoteBody{Reader: remoteRead(func(p []byte) (int, error) {
			enteredBody()
			return body.Read(p)
		}), close: body.Close}
		return resp, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.httpGetURL(ctx, srv.URL+"/stalled", sourcecache.ProducerInput{Writer: io.Discard})
		done <- err
	}()
	<-entered
	cancel()
	{
		err := <-done
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "wrapped Network body cancellation: %v", err)
	}
	<-canceled
	var body strings.Builder
	if _, err := c.httpGetURL(
		t.Context(),
		srv.URL+"/healthy",
		sourcecache.ProducerInput{Writer: &body},
	); err != nil ||
		body.String() != "healthy" {
		t.Fatalf("Network after caller cancellation: %q %v", body.String(), err)
	}
}

type testHTTPGetURLBodyCloseErrorsCaseCallbackInputs struct {
	testCaseInput *testHTTPGetURLBodyCloseErrorsCaseInputs
	closes        *int
}

func testHTTPGetURLBodyCloseErrorsCaseCallback(
	callbackArg1 *http.Request,
	testCaseInput testHTTPGetURLBodyCloseErrorsCaseCallbackInputs,
) (*http.Response, error) {
	var reader io.Reader = strings.NewReader("complete")
	status := http.StatusOK
	switch *testCaseInput.testCaseInput.outcome {
	case "created":
		status = http.StatusCreated
	case "last-success":
		status = 299
	case "status":
		status = http.StatusNotFound
	case "read":
		reader = remoteRead(func([]byte) (int, error) { return 0, (*testCaseInput.testCaseInput.readErr) })
	case "large-read":
		reader = io.MultiReader(
			io.LimitReader(
				remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }),
				(64<<20)+1,
			),
			iotest.ErrReader((*testCaseInput.testCaseInput.readErr)),
		)
	}
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body: remoteBody{
			Reader: reader,
			close:  func() error { (*testCaseInput.closes)++; return (*testCaseInput.testCaseInput.closeErr) },
		},
	}, nil
}

type testHTTPGetURLRequestLifetimeCaseCallbackInputs struct {
	stage **string
}

func testHTTPGetURLRequestLifetimeCaseCallback(
	t *testing.T,
	testCaseInput testHTTPGetURLRequestLifetimeCaseCallbackInputs,
) {
	t.Helper()
	c := newTestHTTPInputs(t, "")
	started := time.Now()
	ctx := t.Context()
	bound := 6 * time.Second
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if (*(*testCaseInput.stage)) == "caller-deadline" {
		var innerCancel context.CancelFunc
		bound = time.Second
		ctx, innerCancel = context.WithTimeout(ctx, bound)
		defer innerCancel()
	}
	if (*(*testCaseInput.stage)) == "previous" {
		var innerCancel2 context.CancelFunc
		ctx, innerCancel2 = context.WithCancel(ctx)
		innerCancel2()
	}
	var requestCtx context.Context
	closes := 0
	c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		if (*(*testCaseInput.stage)) == "previous" {
			t.Error("request issued after Previous rejected cancellation")
		}
		requestCtx = r.Context() //nolint:fatcontext // The callback records the request context for lifetime assertions.
		deadline, ok := requestCtx.Deadline()
		if (*(*testCaseInput.stage)) == "caller-deadline" && (!ok || deadline.Sub(started) != bound) ||
			(*(*testCaseInput.stage)) !=
				"caller-deadline" && ok {
			t.Fatalf("request deadline = %v, present = %t, want %v", deadline.Sub(started), ok, bound)
		}
		if (*(*testCaseInput.stage)) == "headers" || (*(*testCaseInput.stage)) == "body" {
			go func() { time.Sleep(bound); cancel() }()
		}
		if (*(*testCaseInput.stage)) == "headers" {
			<-requestCtx.Done()
			return nil, requestCtx.Err()
		}
		var reader io.Reader = remoteRead(func([]byte) (int, error) {
			<-requestCtx.Done()
			return 0, requestCtx.Err()
		})
		if (*(*testCaseInput.stage)) == "close" {
			reader = strings.NewReader("complete")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: remoteBody{Reader: reader, close: func() error {
				closes++
				if (*(*testCaseInput.stage)) == "close" && requestCtx.Err() != nil {
					t.Errorf("request context canceled before body close: %v", requestCtx.Err())
				}
				return nil
			}},
		}, nil
	})
	testHTTPGetURLRequestLifetimeCaseCallbackCompletion(
		t,
		&testHTTPGetURLRequestLifetimeCaseCallbackCompletionInputs{
			ctx:           &ctx,
			c:             &c,
			testCaseInput: &testCaseInput,
			closes:        &closes,
			requestCtx:    &requestCtx,
			started:       &started,
			bound:         &bound,
		},
	)
}

func testHTTPGetURLRequestLifetimeCaseCallbackCompletion(
	t *testing.T,
	captures *testHTTPGetURLRequestLifetimeCaseCallbackCompletionInputs,
) {
	t.Helper()
	_, err := (*captures.c).httpGetURL((*captures.ctx), "https://example.invalid/resource",
		sourcecache.ProducerInput{Writer: io.Discard},
	)
	switch *(*captures.testCaseInput.stage) {
	case "close":
		if err != nil || (*captures.closes) != 1 || (*captures.requestCtx).Err() != nil {
			t.Fatalf(
				"completed lifetime: err=%v closes=%d context=%v",
				err,
				(*captures.closes),
				(*captures.requestCtx).Err(),
			)
		}
	case "previous":
		assert.FatalIf(
			t,
			!errors.Is(err, context.Canceled) || (*captures.requestCtx) != nil,
			"Previous cancellation: err=%v request=%v",
			err, (*captures.requestCtx),
		)
	default:
		cause := context.Canceled
		if (*(*captures.testCaseInput.stage)) == "caller-deadline" {
			cause = context.DeadlineExceeded
		}
		if !errors.Is(err, cause) || time.Since((*captures.started)) != (*captures.bound) {
			t.Fatalf(
				"request lifetime: err=%v elapsed=%v want=%v",
				err,
				time.Since((*captures.started)),
				(*captures.bound),
			)
		}
		assert.FatalIf(
			t,
			(*(*captures.testCaseInput.stage)) != "headers" && (*captures.closes) != 1,
			"expired body closes=%d",
			(*captures.closes),
		)
	}
}

type testHTTPGetURLRequestLifetimeCaseCallbackCompletionInputs struct {
	ctx           *context.Context
	c             **HTTPInputs
	testCaseInput *testHTTPGetURLRequestLifetimeCaseCallbackInputs
	closes        *int
	requestCtx    *context.Context
	started       *time.Time
	bound         *time.Duration
}
