package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func testAcquireConditionalValidationOwnerUserinfo(t *testing.T) {
	t.Helper()

	const password = "owner-cache-test-password"
	const token = "owner-cache-test-query-token"
	const content = "stable: true\n"
	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return content })
	endpoint, err := url.Parse(srv.URL + "/input?token=" + token)
	assert.NoError(t, err)

	endpoint.User = url.UserPassword("reader", password)
	root := t.TempDir()
	disk := os.DirFS(root)
	var firstIdentity string
	for run := range 2 {
		c := newTestHTTPInputs(t, root)
		input, err := c.Acquire(t.Context(), endpoint.String(), sourcecache.RetryConfig{}, nil)
		assert.NoError(t, err)

		body, err := fs.ReadFile(input, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(body), content)
		identity := sourcecache.AcquisitionIdentity(input)
		if identity == "" || run == 1 && identity != firstIdentity {
			t.Error("unchanged authenticated input did not reuse acquisition identity")
		}
		firstIdentity = identity
		closeTestHTTPInputs(t, c)
		assert.NoError(t, fs.WalkDir(disk, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(disk, name)
			if err != nil {
				return err
			}
			assert.ErrorIf(
				t,
				strings.Contains(string(data), password) || strings.Contains(string(data), token),
				"persistent cache file %s contains URL credentials",
				name,
			)
			return nil
		}))
	}
	assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{
		{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len(content)},
		{
			Method:   http.MethodGet,
			Path:     "/input",
			ETag:     `"` + manifest.Fingerprint(content) + `"`,
			Modified: "Mon, 01 Jun 2026 12:00:00 GMT",
			Status:   http.StatusNotModified,
		},
	})
}

func testAcquireConditionalValidationOwnerMissing(t *testing.T) {
	t.Helper()

	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return "changed" })
	url := srv.URL + "/input?version=2"
	root := t.TempDir()
	c := newTestHTTPInputs(t, root)
	reader, _, _, err := c.owner.Blob(
		t.Context(),
		sourcecache.BlobKey{Content: struct{ URL string }{url}},
		sourcecache.Policy{},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			annotations := map[string]string{
				httpETagAnnotation:         `"` + manifest.Fingerprint("changed") + `"`,
				httpLastModifiedAnnotation: "Mon, 01 Jun 2026 12:00:00 GMT",
			}
			size, err := io.WriteString(w, "original")
			return sourcecache.ProducerResult{Size: int64(size), Annotations: annotations}, err
		},
	)
	assert.NoError(t, err)

	firstIdentity := sourcecache.AcquisitionIdentity(sourcecache.NameBlob("content", reader))

	closeTestHTTPInputs(t, c)
	c = newTestHTTPInputs(t, root)
	input, err := c.Acquire(t.Context(), url, sourcecache.RetryConfig{}, nil)
	assert.NoError(t, err)

	body, err := fs.ReadFile(input, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "changed")
	if sourcecache.AcquisitionIdentity(input) == firstIdentity {
		t.Error("different owner retained previous acquisition identity")
	}
	assert.Diff(
		t,
		trace.snapshot(),
		[]remoteHTTPRequest{{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len("changed")}},
	)
	closeTestHTTPInputs(t, c)
}

func testAcquireConditionalValidationCase3(t *testing.T, scenario *string,
) {
	t.Helper()

	root := t.TempDir()
	const url = "https://reader:owner-cache-test-password@example.invalid/input"
	const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
	etag := `W/"opaque-tag"`
	closeErr := errors.New("304 close failed")
	var firstIdentity string
	for phase := range 3 {
		c := newTestHTTPInputs(t, root)
		requests := 0
		c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			return testAcquireConditionalValidationCase3Callback(
				r,
				testAcquireConditionalValidationCase3CallbackInputs{
					t:        &t,
					scenario: &scenario,
					etag:     &etag,
					closeErr: &closeErr,
					phase:    &phase,
					requests: &requests,
				},
			)
		},
		)
		input, err := c.Acquire(t.Context(), url, sourcecache.RetryConfig{}, nil)
		assert.FatalIf(t, requests != 1, "phase=%d requests=%d, want 1", phase, requests)
		if (*scenario) == "unsolicited-304" ||
			phase == 1 &&
				((*scenario) == "unauthorized" || (*scenario) == "close-304" || (*scenario) == "unvalidated-304") {
			assert.FatalIf(t, err == nil || input != nil, "invalid validation succeeded: %v", err)
			assert.FatalIf(
				t, (*scenario) ==
					"close-304" && !errors.Is(err, closeErr), "close identity lost: %v",
				err,
			)
			closeTestHTTPInputs(t, c)
			break
		}
		assert.NoError(t, err)

		identity := sourcecache.AcquisitionIdentity(input)
		if phase == 0 {
			firstIdentity = identity
		} else if (*scenario) == "clear" {
			assert.FatalIf(t, identity == firstIdentity, "%s", "changed bytes retained identity")
		} else {
			assert.FatalIf(t, identity != firstIdentity, "%s", "unchanged bytes changed identity")
		}

		body, err := fs.ReadFile(input, "content")
		wantBody := "original"
		if phase != 0 && (*scenario) == "clear" {
			wantBody = "changed"
		}
		assert.FatalIf(t, err != nil || string(body) != wantBody, "body=%q, error=%v", body, err)
		if phase == 1 && (*scenario) == "update-304" {
			etag = `"rotated"`
		}
		closeTestHTTPInputs(t, c)
		if phase == 0 && (*scenario) == "missing" {
			data, err := os.ReadFile(filepath.Join(root, "oci/index.json"))
			assert.NoError(t, err)

			var index ocispec.Index
			{
				err := json.Unmarshal(data, &index)
				assert.NoError(t, err)
			}

			for _, desc := range index.Manifests {
				if desc.MediaType == "application/octet-stream" {
					path := filepath.Join(
						root,
						"oci/blobs",
						desc.Digest.Algorithm().String(),
						desc.Digest.Encoded(),
					)
					if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func testAcquireCancelDoesNotPoisonCacheCase(
	t *testing.T, concurrency *int,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireCancelDoesNotPoisonCacheCaseCallback(
			t,
			testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs{concurrency: &concurrency},
		)
	},
	)
}

func testAcquireRetryTransientCacheClientErrorCase(
	t *testing.T, tc *struct {
		status int
		client bool
		retry  bool
	},
) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(tc.status)
			return
		}
		_, _ = w.Write([]byte("retried"))
	}))
	t.Cleanup(srv.Close)
	cache := newTestHTTPInputs(t, "")
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	input, err := cache.Acquire(t.Context(), srv.URL, retry, nil)
	var body []byte
	if err == nil {
		body, err = fs.ReadFile(input, "content")
	}
	if !tc.retry {
		status, ok := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || status.StatusCode != tc.status || status.Op != http.MethodGet || hits.Load() != 1 {
			t.Fatalf("initial status=%+v err=%v hits=%d", status, err, hits.Load())
		}
		input, err = cache.Acquire(t.Context(), srv.URL, retry, nil)
		if err == nil {
			body, err = fs.ReadFile(input, "content")
		}
	}
	if tc.client {
		if fetcherr.Classes(err)&fetcherr.Client == 0 || hits.Load() != 1 {
			t.Fatalf("4xx cache: err=%v, hits=%d", err, hits.Load())
		}
	} else if err != nil || string(body) != "retried" || hits.Load() != 2 {
		t.Fatalf("retry or next fetch: body=%q, err=%v, hits=%d", body, err, hits.Load())
	}
}

func testAcquirePolicyIsolationCase(t *testing.T, concurrency *int,
	reverse *bool,
	equal *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquirePolicyIsolationCaseCallback(
			t,
			testAcquirePolicyIsolationCaseCallbackInputs{concurrency: &concurrency, reverse: &reverse, equal: &equal},
		)
	},
	)
}

func testAcquirePartialBodyRetryCase(t *testing.T, concurrency *int,
) {
	t.Helper()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			testAcquirePartialBodyRetryCaseCase(
				t,
				testAcquirePartialBodyRetryCaseCaseInputs{concurrency: &concurrency, backing: &backing},
			)
		},
		)
	}
}

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

type testAcquireConditionalValidationCase3CallbackInputs struct {
	t        **testing.T
	scenario **string
	etag     *string
	closeErr *error
	phase    *int
	requests *int
}

func testAcquireConditionalValidationCase3Callback(
	r *http.Request,
	testCaseInput testAcquireConditionalValidationCase3CallbackInputs,
) (*http.Response, error) {
	(*testCaseInput.requests)++
	const modified = "Mon, 01 Jun 2026 12:00:00 GMT"

	wantETag, wantModified := "", ""
	if (*testCaseInput.phase) != 0 && ((*testCaseInput.phase) != 1 || (*(*testCaseInput.scenario)) != "missing") {
		if (*(*testCaseInput.scenario)) != "last-modified" && (*(*testCaseInput.scenario)) != "none" &&
			(*(*testCaseInput.scenario)) != "unvalidated-304" {
			wantETag = (*testCaseInput.etag)
		}
		if (*(*testCaseInput.scenario)) != "etag" && (*(*testCaseInput.scenario)) != "none" &&
			(*(*testCaseInput.scenario)) != "unvalidated-304" {
			wantModified = modified
		}
		if (*testCaseInput.phase) == 2 && (*(*testCaseInput.scenario)) == "clear" {
			wantETag, wantModified = "", ""
		}
	}
	{
		got := r.Header.Get("If-None-Match")
		assert.ErrorIf(
			(*testCaseInput.t),
			got != wantETag,
			"phase=%d: ETag=%q, want %q",
			(*testCaseInput.phase),
			got,
			wantETag,
		)
	}
	{
		got := r.Header.Get("If-Modified-Since")
		assert.ErrorIf((*testCaseInput.t), got != wantModified,
			"phase=%d: modified=%q, want %q", (*testCaseInput.phase), got,
			wantModified,
		)
	}
	status := http.StatusOK
	body := "original"
	headers := make(http.Header)
	if (*(*testCaseInput.scenario)) != "last-modified" && (*(*testCaseInput.scenario)) != "none" &&
		(*(*testCaseInput.scenario)) != "unvalidated-304" {
		headers.Set("ETag", (*testCaseInput.etag))
	}
	if (*(*testCaseInput.scenario)) != "etag" && (*(*testCaseInput.scenario)) != "none" &&
		(*(*testCaseInput.scenario)) != "unvalidated-304" {
		headers.Set("Last-Modified", modified)
	}
	return testAcquireConditionalValidationCase3CallbackCompletion(
		&testAcquireConditionalValidationCase3CallbackCompletionInputs{
			testCaseInput: &testCaseInput,
			wantETag:      &wantETag,
			wantModified:  &wantModified,
			status:        &status,
			body:          &body,
			headers:       &headers,
			r:             &r,
		},
	)
}

func testAcquireConditionalValidationCase3CallbackCompletion(
	captures *testAcquireConditionalValidationCase3CallbackCompletionInputs,
) (*http.Response, error) {
	if (*captures.testCaseInput.phase) != 0 && ((*captures.wantETag) != "" || (*captures.wantModified) != "") {
		(*captures.status), (*captures.body) = http.StatusNotModified, ""
	}
	if (*captures.testCaseInput.phase) != 0 && (*(*(*captures.testCaseInput).scenario)) == "clear" {
		(*captures.status), (*captures.body), (*captures.headers) = http.StatusOK, "changed", make(http.Header)
	}
	if (*captures.testCaseInput.phase) == 1 && (*(*(*captures.testCaseInput).scenario)) == "update-304" {
		(*captures.headers) = http.Header{"Etag": {`"rotated"`}}
	}
	if (*(*captures.testCaseInput.scenario)) == "unsolicited-304" || (*(*captures.testCaseInput).phase) !=
		0 && (*(*captures.testCaseInput.scenario)) == "unvalidated-304" {
		(*captures.status), (*captures.body) = http.StatusNotModified, ""
	}
	if (*captures.testCaseInput.phase) == 1 && (*(*(*captures.testCaseInput).scenario)) == "unauthorized" {
		(*captures.status), (*captures.body) = http.StatusUnauthorized, ""
	}
	var closer error
	if (*captures.testCaseInput.phase) == 1 && (*(*(*captures.testCaseInput).scenario)) == "close-304" {
		closer = (*captures.testCaseInput.closeErr)
	}
	return &http.Response{
		Request:    (*captures.r).Clone((*captures.r).Context()),
		StatusCode: (*captures.status),
		Header:     (*captures.headers),
		Body:       remoteBody{Reader: strings.NewReader((*captures.body)), close: func() error { return closer }},
	}, nil
}

type testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs struct {
	concurrency **int
}

func testAcquireCancelDoesNotPoisonCacheCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCancelDoesNotPoisonCacheCaseCallbackInputs,
) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	hits := 0
	c := newTestHTTPInputs(t, "")
	c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
		hits++
		if hits == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("body: ok\n")),
			Header:     make(http.Header),
		}, nil
	})}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	ctx, cancel := context.WithCancel(t.Context())
	cancelled := make(chan error, 1)
	go func() { _, err := c.Acquire(ctx, "https://example.invalid/resource", retry, nil); cancelled <- err }()
	<-entered
	done := make(chan error, (*(*testCaseInput.concurrency))-1)
	for range (*(*testCaseInput.concurrency)) - 1 {
		go func() {
			input, err := c.Acquire(t.Context(), "https://example.invalid/resource", retry, nil)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if err == nil && string(body) != "body: ok\n" {
				err = fmt.Errorf("body=%q", body)
			}
			done <- err
		}()
	}
	synctest.Wait()
	cancel()
	{
		err := <-cancelled
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "cancelled waiter=%v", err)
	}
	close(release)
	for range (*(*testCaseInput.concurrency)) - 1 {
		{
			err := <-done
			assert.NoError(t, err)
		}
	}
	assert.FatalIf(t, hits != 2, "requests=%d, want 2", hits)
	{
		_, err := c.Acquire(
			ctx,
			"https://example.invalid/resource",
			retry,
			nil,
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "canceled warm waiter: %v", err)
	}
}

type testAcquirePolicyIsolationCaseCallbackInputs struct {
	concurrency **int
	reverse     **bool
	equal       **bool
}

func testAcquirePolicyIsolationCaseCallback(t *testing.T, testCaseInput testAcquirePolicyIsolationCaseCallbackInputs) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	hits := 0
	c := newTestHTTPInputs(t, "")
	c.httpClient = &http.Client{
		Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			hits++
			if hits == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
			}, nil
		}),
	}
	done := make(chan error, (*(*testCaseInput.concurrency)))
	launch := func(i int) {
		go func() {
			retry := sourcecache.RetryConfig{
				Attempts: 3,
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			}
			if !(*(*testCaseInput.equal)) && i == 0 {
				retry.Attempts = 1
			}
			if *(*testCaseInput.equal) {
				switch i % 3 {
				case 0:
					retry.Jitter = math.NaN()
				case 1:
					retry.Jitter = math.Inf(1)
				}
			}
			_, err := c.Acquire(t.Context(), "https://example.invalid/resource", retry, nil)
			done <- err
		}()
	}
	testAcquirePolicyIsolationCaseCallbackCompletion(
		t,
		&testAcquirePolicyIsolationCaseCallbackCompletionInputs{
			testCaseInput: &testCaseInput,
			launch:        &launch,
			entered:       &entered,
			release:       &release,
			done:          &done,
			hits:          &hits,
		},
	)
}

func testAcquirePolicyIsolationCaseCallbackCompletion(
	t *testing.T,
	captures *testAcquirePolicyIsolationCaseCallbackCompletionInputs,
) {
	t.Helper()
	first := 0
	if *(*captures.testCaseInput.reverse) {
		first = (*(*captures.testCaseInput.concurrency)) - 1
	}
	(*captures.launch)(first)
	<-(*captures.entered)
	for i := range *(*captures.testCaseInput.concurrency) {
		if i != first {
			(*captures.launch)(i)
		}
	}
	synctest.Wait()
	close((*captures.release))
	for range *(*captures.testCaseInput.concurrency) {
		err := <-(*captures.done)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		assert.FatalIf(t, !ok || !terminal.Attempted, "terminal=%+v, error=%v", terminal, err)
	}
	want := 4
	if *(*captures.testCaseInput.equal) {
		want = 3
	}
	assert.FatalIf(t, (*captures.hits) != want, "requests=%d, want %d", (*captures.hits), want)
}

type testAcquireConditionalValidationCase3CallbackCompletionInputs struct {
	testCaseInput *testAcquireConditionalValidationCase3CallbackInputs
	wantETag      *string
	wantModified  *string
	status        *int
	body          *string
	headers       *http.Header
	r             **http.Request
}
type testAcquirePolicyIsolationCaseCallbackCompletionInputs struct {
	testCaseInput *testAcquirePolicyIsolationCaseCallbackInputs
	launch        *func(i int)
	entered       *chan struct{}
	release       *chan struct{}
	done          *chan error
	hits          *int
}
