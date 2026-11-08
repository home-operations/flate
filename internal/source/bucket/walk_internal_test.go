package bucket

import (
	"archive/tar"
	"bufio"
	"bytes"
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

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"

	"github.com/minio/minio-go/v7"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func delayedBucket(tb testing.TB) (*minio.Client, *atomic.Int32) {
	tb.Helper()
	var active, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
			for i := 15; i >= 0; i-- {
				_, _ = fmt.Fprintf(w, "<Contents><Key>file%02d</Key><Size>1</Size><ETag>\"x\"</ETag></Contents>", i)
			}
			_, _ = fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", "\"x\"")
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "x")
	}))
	tb.Cleanup(srv.Close)
	client, err := minio.New(
		strings.TrimPrefix(srv.URL, "http://"),
		&minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath},
	)
	assert.NoError(tb, err)

	return client, &peak
}

func TestWalkBucket_BoundedParallelOrder(t *testing.T) {
	t.Parallel()
	client, peak := delayedBucket(t)
	var data bytes.Buffer
	{
		_, err := walkBucket(t.Context(), client, &manifest.Bucket{BucketName: "data"}, bucketOutput{Writer: &data})
		assert.NoError(t, err)
	}

	{
		got := peak.Load()
		assert.FatalIf(t, got < 2 || got > 8, "parallel downloads=%d, want 2..8", got)
	}
	var repeated bytes.Buffer
	{
		_, err := walkBucket(t.Context(), client, &manifest.Bucket{BucketName: "data"}, bucketOutput{Writer: &repeated})
		assert.NoError(t, err)
	}
	assert.FatalIf(t, !bytes.Equal(data.Bytes(), repeated.Bytes()), "%s", "bucket archive differs between two runs")

	tr := tar.NewReader(&data)
	for i := range 16 {
		h, err := tr.Next()
		assert.NoError(t, err)

		if want := fmt.Sprintf("file%02d", i); h.Name != want {
			t.Fatalf("entry=%s, want %s", h.Name, want)
		}
	}
	{
		_, err := tr.Next()
		assert.FatalIf(t, err != io.EOF, "archive end: %v", err)
	}
}

type blockedArchiveWriter struct{ release <-chan struct{} }

func (w blockedArchiveWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestWalkBucket_ChunkBound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		requested := make(chan struct{}, 16)
		completed := make(chan struct{}, 16)
		client, err := minio.New("bucket.invalid", &minio.Options{
			Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
			Transport: bucketRoundTrip(func(r *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				testWalkBucketChunkBoundCallback(
					recorder,
					r,
					testWalkBucketChunkBoundCallbackInputs{requested: &requested, completed: &completed},
				)
				response := recorder.Result()
				response.Request = r
				return response, nil
			}),
		})
		assert.NoError(t, err)

		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		var workers sync.WaitGroup
		t.Cleanup(func() {
			unblock()
			cancel()
			workers.Wait()
		})
		workers.Go(func() {
			_, err := walkBucket(
				ctx,
				client,
				&manifest.Bucket{BucketName: "data"},
				bucketOutput{Writer: blockedArchiveWriter{release}},
			)
			done <- err
		})
		synctest.Wait()
		for range 8 {
			select {
			case <-completed:
			default:
				t.Fatal("downloads stalled before filling window")
			}
		}
		for range 8 {
			select {
			case <-requested:
			default:
				t.Fatal("download request missing before filling window")
			}
		}
		select {
		case <-requested:
			t.Fatal("download exceeded chunk window")
		default:
		}
		select {
		case <-done:
			t.Fatal("walk completed before writer release")
		default:
		}
		unblock()
		assert.NoError(t, <-done)
	})
}

func TestWalkBucket_Faults(t *testing.T) {
	t.Parallel()

	for _, route := range []string{"list", "object"} {
		for _, tc := range []struct {
			name   string
			status int
		}{
			{"missing", http.StatusNotFound},
			{"server", http.StatusInternalServerError},
			{"forbidden", http.StatusForbidden},
			{"bad_request", http.StatusBadRequest},
			{"unauthorized", http.StatusUnauthorized},
			{"limited", http.StatusTooManyRequests},
			{"client_boundary", 499},
			{"server_boundary", 599},
			{"terminal", 600},
			{"hostile", 0},
			{"short", 0},
			{"long", 0},
			{"hang", 0},
		} {
			if route == "list" && tc.status == 0 {
				continue
			}
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				testWalkBucketFaultsCase(t, &route, &tc)
			},
			)
		}
	}
}

func TestWalkBucket_ArchiveNames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, prefix, key, want string
		invalid                 bool
	}{
		{name: "delimiter", prefix: "a", key: "a/x", want: "x"},
		{name: "slash prefix", prefix: "a/", key: "a/x", want: "x"},
		{name: "literal prefix", prefix: "a", key: "abc", want: "bc"},
		{name: "exact key fallback", prefix: "a/x", key: "a/x", want: "x"},
		{name: "raw traversal laundered by prefix", prefix: "a/..", key: "a/../x", invalid: true},
		{name: "file root", key: ".", invalid: true},
		{name: "unmatched prefix", prefix: "a", key: "b/x", invalid: true},
		{name: "invalid derived name", prefix: "ab", key: "ab../x", invalid: true},
		{name: "directory marker", key: "dir/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testWalkBucketArchiveNamesCase(t, &tc)
		},
		)
	}
}

func TestWalkBucket_RejectsNegativeListedSize(t *testing.T) {
	t.Parallel()
	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "" {
			gets.Add(1)
			return
		}
		_, _ = io.WriteString(
			w,
			"<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>big</Key><Size>-1</Size><ETag>\"x\"</ETag></Contents></ListBucketResult>",
		)
	}))
	t.Cleanup(srv.Close)
	client, err := minio.New(
		strings.TrimPrefix(srv.URL, "http://"),
		&minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath},
	)
	assert.NoError(t, err)

	if _, err := walkBucket(
		t.Context(),
		client,
		&manifest.Bucket{BucketName: "data"},
		bucketOutput{Writer: io.Discard},
	); err == nil ||
		err.Error() != `bucket object "big" has negative size` ||
		gets.Load() != 0 {
		t.Fatalf("err=%v gets=%d, want a listing-time error and no download", err, gets.Load())
	}
}

type bucketRoundTrip func(*http.Request) (*http.Response, error)

func (tr bucketRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	return tr(req)
}

type bucketReadBody struct {
	io.ReadCloser

	notify func()
}

func (b bucketReadBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.notify()
	}
	return n, err
}

func TestWalkBucket_LargeListedSize(t *testing.T) {
	t.Parallel()

	for _, ranges := range []bool{false, true} {
		t.Run(fmt.Sprintf("ranges=%t", ranges), func(t *testing.T) {
			t.Parallel()
			testWalkBucketLargeListedSizeCase(t, &ranges)
		},
		)
	}
}

// Children MUST use AfterFunc so the gate holds cancellation propagation
// until the writer's body error reaches errgroup.
type gatedCancelContext struct {
	context.Context //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

	gate <-chan struct{}
}

// Err MUST preserve the cancellation cause when AfterFunc cancels a child.
func (c gatedCancelContext) Err() error { return context.Cause(c.Context) }

func (c gatedCancelContext) Value(any) any { return nil }

func (c gatedCancelContext) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.Context, func() {
		<-c.gate
		f()
	})
}

type cancelArchiveWriter struct {
	cancel context.CancelFunc
	header bool
}

func (w *cancelArchiveWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.header {
		w.cancel()
	}
	w.header = true
	return len(p), nil
}

func TestWalkBucket_Ranges(t *testing.T) {
	t.Parallel()

	for _, behavior := range []string{"ordered", "ignore", "changed", "wrong_range", "short", "long", "cancel", "ignore_short", "ignore_long", "ignore_cancel", "ignore_changed", "ignore_missing", "ignore_server", "ignore_big", "ignore_cancel_ordered", "probe_forbidden", "probe_limited", "probe_server"} {
		t.Run(behavior, func(t *testing.T) {
			t.Parallel()
			testWalkBucketRangesCase(t, &behavior)
		},
		)
	}
}

func timeoutBucketFetcher(t *testing.T, respond func(*http.Request) (*http.Response, error)) *Fetcher {
	t.Helper()
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	var workers sync.WaitGroup
	transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		workers.Go(func() {
			defer server.Close()
			req, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			workers.Go(func() {
				_, _ = io.Copy(io.Discard, server)
				cancel()
			})
			response, err := respond(req.WithContext(ctx))
			if err == nil {
				defer response.Body.Close()
				_ = response.Write(server)
			}
		})
		return client, nil
	}
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			http.DefaultTransport = previous //nolint:reassign // The test must restore the process HTTP transport after offline acquisition.
		},
	)
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	t.Cleanup(func() { f.Network.Close(); workers.Wait() })
	return f
}

//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
func TestWalkBucket_SourceTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout *metav1.Duration
		caller  time.Duration
		want    time.Duration
	}{
		{"default", nil, 0, time.Minute},
		{"shorter", &metav1.Duration{Duration: 10 * time.Second}, 0, 10 * time.Second},
		{"longer", &metav1.Duration{Duration: 180 * time.Second}, 0, 180 * time.Second},
		{"caller", &metav1.Duration{Duration: 180 * time.Second}, 5 * time.Second, 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				f := timeoutBucketFetcher(t, func(req *http.Request) (*http.Response, error) {
					calls++
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       bucketTimeoutBody{req.Context()},
						Request:    req,
					}, nil
				})
				b := &manifest.Bucket{
					BucketName: "data",
					Endpoint:   "https://bucket.invalid",
					Region:     "us-east-1",
					Timeout:    tc.timeout,
				}
				ctx := t.Context()
				if tc.caller > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.caller)
					defer cancel()
				}
				start := time.Now()
				artifact, err := fetchtest.Fetch(ctx, f.Fetch, b, (testInputs{}).readSet(b), &cache.Policy{}, nil)
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) || artifact != nil ||
					calls != 1 ||
					time.Since(start) != tc.want {
					t.Fatalf(
						"source deadline: artifact=%v calls=%d elapsed=%v error=%v",
						artifact,
						calls,
						time.Since(start),
						err,
					)
				}
			})
		})
	}
}

type bucketTimeoutBody struct{ ctx context.Context } //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

func (b bucketTimeoutBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (bucketTimeoutBody) Close() error { return nil }

//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
//nolint:paralleltest // The test replaces the process HTTP transport and restores it afterward.
func TestWalkBucket_PhaseTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects int
	}{
		{"aggregate phases", 2},
		{"listing body", 2},
		{"download body", 2},
		{"complete download batch", 9},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { testWalkBucketPhaseTimeoutsCase(t, &tc) },
		)
	}
}

func testWalkBucketFaultsCase(t *testing.T, route *string,
	tc *struct {
		name   string
		status int
	},
) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testWalkBucketFaultsCaseCallback(w, r, testWalkBucketFaultsCaseCallbackInputs{route: &route, tc: &tc})
	},
	))
	defer srv.Close()
	var transfers atomic.Uint64
	transport := bucketRoundTrip(func(req *http.Request) (*http.Response, error) {
		response, err := srv.Client().Transport.RoundTrip(req)
		if err == nil && req.URL.Query().Get("list-type") == "" && response.StatusCode == http.StatusOK {
			transfers.Add(1)
		}
		return response, err
	})
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{
		Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1, Transport: transport,
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	b := &manifest.Bucket{Name: "source", Namespace: "apps", BucketName: "data"}
	key := manifest.WorkKey{
		Tree:  manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead},
		Owner: b.Named(), Source: b.Named(), Phase: manifest.WorkAcquisition, Input: "archive",
	}
	work := manifest.WorkRecorder{Key: key}
	physical := work.Scope().Record(manifest.WorkAcquisition, key.Input)
	_, err = walkBucket(ctx, client, b, bucketOutput{Writer: io.Discard, work: physical})
	wantWork := manifest.WorkSummary{}
	if count := transfers.Load(); count != 0 {
		wantWork = append(wantWork, manifest.WorkRecord{Key: key, Counts: manifest.WorkCounts{RemoteTransfers: count}})
	}
	if diff := cmp.Diff(wantWork, work.Scope().Snapshot(0).Normalize(), cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("successful object transfers (-want +got):\n%s", diff)
	}

	assert.FatalIf(t, err == nil, "%s", "fault accepted")

	assert.FatalIf(
		t, tc.
			name != "hang" && errors.Is(err, context.DeadlineExceeded), "fault timed out: %v",
		err,
	)
	if tc.status != 0 {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || response.StatusCode != tc.status || response.Op != http.MethodGet ||
			err.Error() != "bucket status failure" {
			t.Fatalf("HTTP fault: %v, response=%+v, want GET status %d", err, response, tc.status)
		}
		if tc.name == "forbidden" || tc.name == "missing" {
			cause, ok := errors.AsType[minio.ErrorResponse](err)
			assert.FatalIf(
				t,
				!ok || cause.StatusCode != tc.status || cause.Message != "bucket status failure" ||
					!errors.Is(response.Err, cause) ||
					err.Error() != cause.Error(),
				"terminal MinIO cause: %v, response=%+v, cause=%+v",
				err,
				response,
				cause,
			)
		}
		return
	}
	switch tc.name {
	case "hostile":
		assert.FatalIf(t, !errors.Is(err, fs.ErrInvalid), "path fault: %v", err)
	case "short":
		assert.FatalIf(t, !errors.Is(err, io.ErrUnexpectedEOF), "short body: %v", err)
	case "long":
		assert.FatalIf(t, err.Error() != `bucket object "file05" exceeds chunk size`, "long body: %v", err)
	case "hang":
		assert.FatalIf(t, !errors.Is(err, context.DeadlineExceeded), "deadline identity: %v", err)
	}
}

type testWalkBucketChunkBoundCallbackInputs struct {
	requested *chan struct{}
	completed *chan struct{}
}

func testWalkBucketChunkBoundCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testWalkBucketChunkBoundCallbackInputs,
) {
	const size = 8388608
	if r.URL.Query().Get("list-type") != "" {
		_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for i := range 16 {
			_, _ = fmt.Fprintf(
				w,
				"<Contents><Key>file%02d</Key><Size>%d</Size><ETag>\"x\"</ETag></Contents>",
				i,
				size,
			)
		}
		_, _ = fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	(*testCaseInput.requested) <- struct{}{}
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", "\"x\"")
	w.Header().Set("Content-Length", strconv.Itoa(size))
	buf := []byte(strings.Repeat("x", min(size, 64<<10)))
	for sent := 0; sent < size; sent += len(buf) {
		if _, err := w.Write(buf[:min(len(buf), size-sent)]); err != nil {
			return
		}
	}
	w.(http.Flusher).Flush()
	(*testCaseInput.completed) <- struct{}{}
}

type testWalkBucketFaultsCaseCallbackInputs struct {
	route **string
	tc    **struct {
		name   string
		status int
	}
}

func testWalkBucketFaultsCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testWalkBucketFaultsCaseCallbackInputs,
) {
	listing := r.URL.Query().Get("list-type") != ""
	if (*testCaseInput.tc).status != 0 &&
		(listing && (*(*testCaseInput.route)) == "list" || !listing && strings.HasSuffix(r.URL.Path, "file05")) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader((*testCaseInput.tc).status)
		_, _ = io.WriteString(
			w,
			`<Error><Code>Fault</Code><Message>bucket status failure</Message></Error>`,
		)
		return
	}
	if listing {
		_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for i := range 16 {
			key := fmt.Sprintf("file%02d", i)
			if (*testCaseInput.tc).name == "hostile" && i == 3 {
				key = "../escape"
			}
			_, _ = fmt.Fprintf(
				w,
				"<Contents><Key>%s</Key><Size>4</Size><ETag>\"x\"</ETag></Contents>",
				key,
			)
		}
		_, _ = fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", "\"x\"")
	if strings.HasSuffix(r.URL.Path, "file05") {
		switch (*testCaseInput.tc).name {
		case "long":
			w.Header().Set("Content-Length", "5")
			_, _ = io.WriteString(w, "abcde")
			return
		case "hang":
			<-r.Context().Done()
			return
		case "short":
			w.Header().Set("Content-Length", "4")
			_, _ = io.WriteString(w, "ab")
			return
		}
	}
	w.Header().Set("Content-Length", "4")
	_, _ = io.WriteString(w, "abcd")
}
