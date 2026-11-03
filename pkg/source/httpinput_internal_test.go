package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type remoteHTTPRequest struct {
	Method, Path, ETag, Modified string
	Status, Bytes                int
}

type remoteHTTPTrace struct {
	mu          sync.Mutex
	requests    []remoteHTTPRequest
	connections atomic.Int32
}

func (tr *remoteHTTPTrace) serve(t *testing.T, body func() string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content := body()
		etag := `"` + manifest.Fingerprint(content) + `"`
		const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", modified)
		record := remoteHTTPRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			ETag:     r.Header.Get("If-None-Match"),
			Modified: r.Header.Get("If-Modified-Since"),
			Status:   http.StatusOK,
		}
		if record.ETag == etag {
			record.Status = http.StatusNotModified
			w.WriteHeader(record.Status)
		} else {
			record.Bytes, _ = io.WriteString(w, content)
		}
		tr.mu.Lock()
		tr.requests = append(tr.requests, record)
		tr.mu.Unlock()
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			tr.connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func (tr *remoteHTTPTrace) snapshot() []remoteHTTPRequest {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]remoteHTTPRequest(nil), tr.requests...)
}

func TestAcquire_ConditionalValidation(t *testing.T) {
	t.Parallel()
	for _, validator := range []string{"etag", "last-modified"} {
		t.Run("alias-304/"+validator, func(t *testing.T) {
			t.Parallel()
			testAcquireConditionalValidationCase(t, &validator)
		},
		)
	}
	for _, scenario := range []string{"changed-redirect", "redirected-304", "direct-to-redirect", "direct-to-redirected-304", "redirect-limit"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testAcquireConditionalValidationCase2(t, &scenario)
		},
		)
	}
	testAcquireConditionalValidationCompletion(t)
}

func testAcquireConditionalValidationCompletion(t *testing.T) {
	t.Helper()
	t.Run("owner/userinfo", func(t *testing.T) {
		t.Parallel()
		testAcquireConditionalValidationOwnerUserinfo(

			t)
	})
	t.Run("owner/missing", func(t *testing.T) {
		t.Parallel()
		testAcquireConditionalValidationOwnerMissing(

			t)
	})
	var version atomic.Int32
	version.Store(1)
	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return fmt.Sprintf("version: v%d\n", version.Load()) })
	root := t.TempDir()
	for _, run := range []struct {
		name              string
		version           int32
		status, bodyBytes int
	}{
		{"initial", 1, http.StatusOK, len("version: v1\n")},
		{"unchanged", 1, http.StatusNotModified, 0},
		{"changed", 2, http.StatusOK, len("version: v2\n")},
	} {
		version.Store(run.version)
		c := newTestHTTPInputs(t, root)
		before := len(trace.snapshot())
		beforeConnections := trace.connections.Load()
		for _, path := range []string{"/resource", "/other", "/resource", "/other"} {
			input, err := c.Acquire(t.Context(), srv.URL+path, sourcecache.RetryConfig{}, nil)
			assert.NoError(t, err)

			body, err := fs.ReadFile(input, "content")
			if err != nil || string(body) != fmt.Sprintf("version: v%d\n", run.version) {
				t.Fatalf("%s: body=%q, error=%v", run.name, body, err)
			}
		}
		got := trace.snapshot()[before:]
		t.Logf("%s: requests=%+v accepted_connections=%d", run.name, got, trace.connections.Load())
		if len(got) != 2 {
			t.Errorf("%s: requests=%d, want 1 per URL/policy/run", run.name, len(got))
		}
		for _, request := range got {
			if request.Method != http.MethodGet || request.Status != run.status || request.Bytes != run.bodyBytes {
				t.Errorf("%s: response=%+v, want GET status=%d bytes=%d", run.name, request, run.status, run.bodyBytes)
			}
			if run.name != "initial" && (request.ETag == "" || request.Modified == "") {
				t.Errorf("%s: missing persisted conditional headers: %+v", run.name, request)
			}
		}
		if got := trace.connections.Load() - beforeConnections; got != 1 {
			t.Errorf("%s: compatible URLs accepted_connections=%d, want 1", run.name, got)
		}
		closeTestHTTPInputs(t, c)
	}
	for _, scenario := range []string{"etag", "last-modified", "both", "none", "clear", "update-304", "missing", "unsolicited-304", "unvalidated-304", "unauthorized", "close-304"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testAcquireConditionalValidationCase3(t, &scenario)
		},
		)
	}
}

func TestAcquire_CancelDoesNotPoisonCache(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			t.Parallel()
			testAcquireCancelDoesNotPoisonCacheCase(
				t,
				&concurrency,
			)
		},
		)
	}
}

func TestIsHTTPClientError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"400", &fetcherr.ResponseError{StatusCode: 400}, true},
		{"401", &fetcherr.ResponseError{StatusCode: 401}, true},
		{"403", &fetcherr.ResponseError{StatusCode: 403}, true},
		{"404", &fetcherr.ResponseError{StatusCode: 404}, true},
		{"418", &fetcherr.ResponseError{StatusCode: 418}, true},
		{"499", &fetcherr.ResponseError{StatusCode: 499}, true},
		{"429 transient", &fetcherr.ResponseError{StatusCode: 429}, false},
		{"500 transient", &fetcherr.ResponseError{StatusCode: 500}, false},
		{"599 transient", &fetcherr.ResponseError{StatusCode: 599}, false},
		{"600 terminal", &fetcherr.ResponseError{StatusCode: 600}, false},
		{
			"client after transient",
			errors.Join(
				&fetcherr.ResponseError{StatusCode: 503},
				fmt.Errorf("edge: %w", &fetcherr.ResponseError{StatusCode: 404}),
			),
			true,
		},
		{
			"client before transient",
			errors.Join(&fetcherr.ResponseError{StatusCode: 404}, &fetcherr.ResponseError{StatusCode: 503}),
			true,
		},
		{"200 never-client-error", &fetcherr.ResponseError{StatusCode: 200}, false},
		{"wrapped 404", fmt.Errorf("preflight: %w", &fetcherr.ResponseError{StatusCode: 404}), true},
		{"wrapped 500", fmt.Errorf("preflight: %w", &fetcherr.ResponseError{StatusCode: 500}), false},
		{"transport error", fmt.Errorf("connection refused"), false},
		{"deadline", fmt.Errorf("context deadline exceeded"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := fetcherr.Classes(tc.err)&fetcherr.Client != 0; got != tc.want {
			t.Errorf("isHTTPClientError(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAcquire_RetryTransientCacheClientError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status int
		client bool
		retry  bool
	}{
		{400, true, false},
		{401, true, false},
		{403, true, false},
		{404, true, false},
		{429, false, true},
		{499, true, false},
		{500, false, true},
		{599, false, true},
		{600, false, false},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			t.Parallel()

			testAcquireRetryTransientCacheClientErrorCase(
				t,
				&tc,
			)
		},
		)
	}
}

type remoteRoundTrip func(*http.Request) (*http.Response, error)

func (f remoteRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := f(r)
	if resp != nil && resp.Request == nil {
		resp.Request = r
	}
	return resp, err
}

func TestAcquire_PolicyIsolation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			for _, equal := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/reverse-%t/equal-%t", concurrency, reverse, equal), func(t *testing.T) {
					t.Parallel()
					testAcquirePolicyIsolationCase(
						t,

						&concurrency,
						&reverse,
						&equal,
					)
				},
				)
			}
		}
	}
}

func TestAcquire_PartialBodyRetry(t *testing.T) {
	t.Parallel()

	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			t.Parallel()

			testAcquirePartialBodyRetryCase(t, &concurrency)
		},
		)
	}
}

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestAcquire_NetworkSuppression(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			testAcquireNetworkSuppressionCase(t, &concurrency)
		},
		)
	}
}

type remoteRead func([]byte) (int, error)

func (f remoteRead) Read(p []byte) (int, error) { return f(p) }

type remoteBody struct {
	io.Reader

	close func() error
}

func (b remoteBody) Close() error { return b.close() }

func TestAcquire_CloseJoinsAcquisition(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, storage := range []string{"memory", "disk"} {
			for _, stage := range []string{"headers", "read", "cleanup", "publication", "backoff"} {
				t.Run(fmt.Sprintf("%d/%s/%s", concurrency, storage, stage), func(t *testing.T) {
					t.Parallel()
					testAcquireCloseJoinsAcquisitionCase(
						t,

						&concurrency,
						&storage,
						&stage,
					)
				},
				)
			}
		}
	}
}

func TestAcquire_CloseJoinsKeyWait(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				testAcquireCloseJoinsKeyWaitCase(
					t,
					&concurrency, &backing,
				)
			},
			)
		}
	}
}

func TestAcquire_ExactSuccessor(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			t.Parallel()
			testAcquireExactSuccessorCase(t, &concurrency)
		},
		)
	}
}

func TestAcquire_OwnerIsolation(t *testing.T) {
	t.Parallel()

	for _, concurrency := range []int{2, 4} {
		for _, sharedNetwork := range []bool{false, true} {
			for _, sameKey := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/network-%t/key-%t", concurrency, sharedNetwork, sameKey), func(t *testing.T) {
					t.Parallel()

					testAcquireOwnerIsolationCase(
						t,

						&concurrency,
						&sharedNetwork,
						&sameKey,
					)
				},
				)
			}
		}
	}
}

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestAcquire_NetworkSuppressionOwnerCancellation(t *testing.T) {
	// net's process-global resolver semaphore MUST be initialized outside the bubbles.
	_, _ = (&net.Resolver{PreferGo: true}).LookupHost(t.Context(), "invalid hostname")
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			testAcquireNetworkSuppressionOwnerCancellationCase(
				t,
				&concurrency,
			)
		},
		)
	}
}

func TestAcquire_NoWorkerDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newTestHTTPInputs(t, "")
		start := time.Now()
		hits := 0
		entered := make(chan struct{})
		c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			hits++
			{
				deadline, ok := r.Context().Deadline()
				assert.ErrorIf(t, ok, "worker deadline=%v present=%t", deadline, ok)
			}
			body := remoteBody{Reader: io.MultiReader(remoteRead(func([]byte) (int, error) {
				close(entered)
				select {
				case <-time.After(6 * time.Second):
					return 0, io.EOF
				case <-r.Context().Done():
					return 0, r.Context().Err()
				}
			}), strings.NewReader("complete")), close: func() error { return nil }}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body, Request: r}, nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		first, second := make(chan error, 1), make(chan error, 1)
		go func() {
			_, err := c.Acquire(ctx, "https://example.invalid/resource", sourcecache.RetryConfig{}, nil)
			first <- err
		}()
		<-entered
		go func() {
			input, err := c.Acquire(t.Context(), "https://example.invalid/resource", sourcecache.RetryConfig{}, nil)
			if err == nil {
				data, readErr := fs.ReadFile(input, "content")
				assert.ErrorIf(
					t,
					readErr != nil || string(data) != "complete",
					"shared content=%q error=%v",
					data,
					readErr,
				)
			}
			second <- err
		}()
		if err := <-first; !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Fatalf("first waiter: elapsed=%v error=%v", time.Since(start), err)
		}
		if err := <-second; err != nil || time.Since(start) != 6*time.Second || hits != 1 {
			t.Fatalf("shared body: elapsed=%v requests=%d error=%v", time.Since(start), hits, err)
		}
	})
}

func testAcquireConditionalValidationCase(t *testing.T, validator *string,
) {
	t.Helper()

	older := time.Date(2020, time.June, 1, 12, 0, 0, 0, time.UTC)
	var phase atomic.Int32
	var trace remoteHTTPTrace
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testAcquireConditionalValidationCaseCallback(
			w,
			r,
			testAcquireConditionalValidationCaseCallbackInputs{
				validator: &validator,
				older:     &older,
				phase:     &phase,
				trace:     &trace,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	root := t.TempDir()
	var firstIdentity string
	for run := range 2 {
		phase.Store(int32(run))
		c := newTestHTTPInputs(t, root)
		input, err := c.Acquire(t.Context(), srv.URL+"/latest.yaml", sourcecache.RetryConfig{}, nil)
		assert.NoError(t, err)

		body, err := fs.ReadFile(input, "content")
		identity := sourcecache.AcquisitionIdentity(input)
		t.Logf("run=%d body=%q identity=%s requests=%+v", run, body, identity, trace.snapshot())
		want := "version: v2\n"
		if run == 1 {
			want = "version: v1\n"
			if identity == firstIdentity {
				t.Error("changed alias target retained acquisition identity")
			}
		}
		assert.ErrorIf(
			t,
			err != nil || string(body) != want,
			"run=%d body=%q, want %q, error=%v",
			run,
			body,
			want,
			err,
		)
		firstIdentity = identity
		closeTestHTTPInputs(t, c)
	}
	assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{
		{Method: http.MethodGet, Path: "/latest.yaml", Status: http.StatusFound},
		{Method: http.MethodGet, Path: "/v2.yaml", Status: http.StatusOK, Bytes: len("version: v2\n")},
		{Method: http.MethodGet, Path: "/latest.yaml", Status: http.StatusFound},
		{Method: http.MethodGet, Path: "/v1.yaml", Status: http.StatusOK, Bytes: len("version: v1\n")},
	})
}

func testAcquireConditionalValidationCase2(t *testing.T, scenario *string,
) {
	t.Helper()

	var phase, requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testAcquireConditionalValidationCase2Callback(
			w,
			r,
			testAcquireConditionalValidationCase2CallbackInputs{
				t:        &t,
				scenario: &scenario,
				phase:    &phase,
				requests: &requests,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	root := t.TempDir()
	var firstIdentity string
	for run := range 2 {
		phase.Store(int32(run))
		c := newTestHTTPInputs(t, root)
		input, err := c.Acquire(t.Context(), srv.URL+"/input", sourcecache.RetryConfig{}, nil)
		if (*scenario) == "redirect-limit" ||
			run == 1 &&
				((*scenario) == "redirected-304" || (*scenario) == "direct-to-redirected-304") {
			assert.FatalIf(t, err == nil || input != nil, "invalid redirect succeeded: %v", err)
			if (*scenario) == "redirect-limit" && requests.Load() != 10 {
				t.Fatalf("redirect requests=%d, want 10", requests.Load())
			}
			closeTestHTTPInputs(t, c)
			break
		}
		assert.NoError(t, err)

		body, err := fs.ReadFile(input, "content")
		assert.FatalIf(
			t,
			err != nil || string(body) != fmt.Sprintf("object: %d\n", run),
			"run=%d body=%q, error=%v",
			run,
			body,
			err,
		)
		identity := sourcecache.AcquisitionIdentity(input)
		assert.FatalIf(
			t,
			run == 1 && identity == firstIdentity,
			"%s",
			"changed redirect target retained acquisition identity",
		)

		firstIdentity = identity
		closeTestHTTPInputs(t, c)
	}
}

type testAcquireConditionalValidationCaseCallbackInputs struct {
	validator **string
	older     *time.Time
	phase     *atomic.Int32
	trace     *remoteHTTPTrace
}

func testAcquireConditionalValidationCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testAcquireConditionalValidationCaseCallbackInputs,
) {
	record := remoteHTTPRequest{
		Method:   r.Method,
		Path:     r.URL.Path,
		ETag:     r.Header.Get("If-None-Match"),
		Modified: r.Header.Get("If-Modified-Since"),
		Status:   http.StatusOK,
	}
	const etag = "\"colliding-target-tag\""
	const modified = "Mon, 01 Jun 2026 12:00:00 GMT"

	defer func() {
		testCaseInput.trace.
			mu.Lock()
		testCaseInput.trace.
			requests = append(testCaseInput.trace.requests, record)
		testCaseInput.trace.
			mu.Unlock()
	}()
	if r.URL.Path == "/latest.yaml" {
		since, err := http.ParseTime(record.Modified)
		if testCaseInput.phase.Load() != 0 &&
			(record.ETag == etag || err == nil && !testCaseInput.older.After(since)) {
			record.Status = http.StatusNotModified
			w.WriteHeader(record.Status)
			return
		}
		target := "/v2.yaml"
		if testCaseInput.phase.Load() != 0 {
			target = "/v1.yaml"
		}
		record.Status = http.StatusFound
		http.Redirect(w, r, target, record.Status)
		return
	}
	switch {
	case (*(*testCaseInput.validator)) == "etag":
		w.Header().Set("ETag", etag)
	case r.URL.Path == "/v2.yaml":
		w.Header().Set("Last-Modified", modified)
	default:

		w.Header().Set("Last-Modified", testCaseInput.older.Format(http.TimeFormat))
	}

	record.Bytes, _ = fmt.Fprintf(
		w,
		"version: %s\n",
		strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), ".yaml"),
	)
}

type testAcquireConditionalValidationCase2CallbackInputs struct {
	t        **testing.T
	scenario **string
	phase    *atomic.Int32
	requests *atomic.Int32
}

func testAcquireConditionalValidationCase2Callback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testAcquireConditionalValidationCase2CallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	const etag = "\"target-local-tag\""
	const modified = "Mon, 01 Jun 2026 12:00:00 GMT"

	if (*(*testCaseInput.scenario)) == "redirect-limit" {
		http.Redirect(w, r, "/input", http.StatusFound)
		return
	}
	if r.URL.Path == "/input" {
		wantETag, wantModified := "", ""
		if testCaseInput.phase.Load() != 0 && strings.HasPrefix((*(*testCaseInput.scenario)), "direct-to-") {
			wantETag, wantModified = etag, modified
		}
		if r.Header.Get("If-None-Match") != wantETag || r.Header.Get("If-Modified-Since") != wantModified {
			(*testCaseInput.t).
				Errorf("origin validators=%v, want %q and %q", r.Header, wantETag, wantModified)
		}
		if testCaseInput.phase.Load() == 0 && strings.HasPrefix((*(*testCaseInput.scenario)), "direct-to-") {
			w.Header().Set("ETag", etag)
			w.Header().Set("Last-Modified", modified)
			_, _ = io.WriteString(w, "object: 0\n")
			return
		}
		http.Redirect(w, r, "/hop", http.StatusFound)
		return
	}
	if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
		(*testCaseInput.t).
			Errorf("redirect received conditional headers: %v", r.Header)
	}
	if r.URL.Path == "/hop" {
		http.Redirect(w, r, fmt.Sprintf("/object-%d", testCaseInput.phase.Load()), http.StatusFound)
		return
	}
	lastModified := modified
	if testCaseInput.phase.Load() != 0 {
		lastModified = "Mon, 01 Jun 2020 12:00:00 GMT"
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", lastModified)
	if testCaseInput.phase.Load() != 0 &&
		((*(*testCaseInput.scenario)) == "redirected-304" || (*(*testCaseInput.scenario)) == "direct-to-redirected-304" || r.Header.Get("If-None-Match") == etag || r.Header.Get("If-Modified-Since") == modified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = fmt.Fprintf(w, "object: %d\n", testCaseInput.phase.Load())
}
