package bucket

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
)

func testWalkBucketArchiveNamesCase(t *testing.T, tc *struct {
	name    string
	prefix  string
	key     string
	want    string
	invalid bool
},
) {
	t.Helper()

	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			if got := r.URL.Query().Get("prefix"); got != tc.prefix {
				t.Errorf("listing prefix=%q, want %q", got, tc.prefix)
			}
			_, _ = fmt.Fprintf(
				w,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><EncodingType>url</EncodingType><Contents><Key>%s</Key><Size>1</Size><ETag>"x"</ETag></Contents></ListBucketResult>`,
				url.QueryEscape(tc.key),
			)
			return
		}
		gets.Add(1)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", `"x"`)
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "x")
	}))
	defer srv.Close()
	client, err := minio.New(
		strings.TrimPrefix(srv.URL, "http://"),
		&minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath},
	)
	assert.NoError(t, err)

	var data bytes.Buffer
	_, err = walkBucket(
		t.Context(),
		client,
		&manifest.Bucket{BucketName: "data", Prefix: tc.prefix},
		&data,
	)
	if tc.invalid {
		if !errors.Is(err, fs.ErrInvalid) || gets.Load() != 0 || data.Len() != 0 {
			t.Fatalf(
				"err=%v gets=%d bytes=%d, want invalid name before reads or headers",
				err,
				gets.Load(),
				data.Len(),
			)
		}
		return
	}
	assert.NoError(t, err)

	tr := tar.NewReader(&data)
	if tc.want != "" {
		h, err := tr.Next()
		if err != nil || h.Name != tc.want {
			t.Fatalf("header=%v err=%v, want %q", h, err, tc.want)
		}
		body, err := io.ReadAll(tr)
		if err != nil || string(body) != "x" || gets.Load() != 1 {
			t.Fatalf("body=%q err=%v gets=%d", body, err, gets.Load())
		}
	} else if gets.Load() != 0 {
		t.Fatalf("directory marker triggered %d reads", gets.Load())
	}
	{
		_, err := tr.Next()
		assert.FatalIf(t, !errors.Is(err, io.EOF), "archive end: %v", err)
	}
}

func testWalkBucketLargeListedSizeCase(t *testing.T, ranges *bool,
) {
	t.Helper()
	const size int64 = (1 << 30) + 1
	const timeout = 10 * time.Second
	cause := errors.New("large bucket body")
	arrived := make(chan struct{}, downloads)

	gate := make(chan struct{})
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate()
	var active, peak atomic.Int32
	var gets [2]atomic.Int32
	var listDeadline, downloadDeadline time.Time
	client, err := minio.New("bucket.invalid", &minio.Options{
		Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1,
		Transport: bucketRoundTrip(func(req *http.Request) (*http.Response, error) {
			return testWalkBucketLargeListedSizeCaseCallback(
				req, &testWalkBucketLargeListedSizeCaseCallbackInputs{
					t:                &t,
					ranges:           &ranges,
					cause:            &cause,
					arrived:          &arrived,
					gate:             &gate,
					active:           &active,
					peak:             &peak,
					gets:             &gets,
					listDeadline:     &listDeadline,
					downloadDeadline: &downloadDeadline,
				},
			)
		},
		),
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := walkBucket(ctx, client, &manifest.Bucket{BucketName: "data"}, io.Discard)
		done <- err
	}()
	for range 2 {
		select {
		case <-arrived:
		case err := <-done:
			t.Fatalf("large acquisition ended before concurrent body reads: %v", err)
		}
	}
	openGate()
	{
		err := <-done
		assert.FatalIf(t, !errors.Is(err, cause), "large object did not reach original reader fault: %v", err)
	}
	if peak.Load() < 2 || peak.Load() > downloads {
		t.Fatalf("large object parallel work=%d, want 2..%d", peak.Load(), downloads)
	}
	if !(*ranges) && (gets[0].Load() != 1 || gets[1].Load() != 1) {
		t.Fatalf("ignored ranges split whole objects: gets=%d/%d", gets[0].Load(), gets[1].Load())
	}
}

func testWalkBucketRangesCase(t *testing.T, behavior *string,
) {
	t.Helper()
	const part = 8 << 20
	copyCtx, cancelCopy := context.WithCancel(t.Context())

	defer cancelCopy()
	ctx := t.Context()
	var cancel context.CancelCauseFunc
	wantRange := ""
	wantStatus := http.StatusOK
	if (*behavior) == "cancel" || (*behavior) == "ignore_cancel" {
		if (*behavior) == "cancel" {
			wantRange = fmt.Sprintf("bytes=%d-%d", part, 2*part-1)
			wantStatus = http.StatusPartialContent
		}
		ctx, cancel = context.WithCancelCause(ctx)
		defer cancel(nil)
		gate := make(chan struct{})
		close(gate)
		ctx = gatedCancelContext{Context: ctx, gate: gate}
	}
	var gets atomic.Int32
	later := make(chan struct{})
	streamed := make(chan struct{})
	clientRead := make(chan struct{})
	notifyRead := sync.OnceFunc(func() { close(clientRead) })
	completeLater := sync.OnceFunc(func() { close(later) })
	sizes := []int64{0, 2*part + 17, 2*part + 17}
	fault := 0
	probeStatus := map[string]int{
		"probe_forbidden": http.StatusForbidden,
		"probe_limited":   http.StatusTooManyRequests,
		"probe_server":    http.StatusServiceUnavailable,
	}[(*behavior)]
	switch *behavior {
	case "ignore_changed":
		fault = http.StatusPreconditionFailed
	case "ignore_missing":
		fault = http.StatusNotFound
	case "ignore_server":
		fault = http.StatusInternalServerError
	case "ignore_cancel_ordered":
		sizes = []int64{9 << 20}
	case "ignore_big":
		sizes = []int64{downloads*part + 1, 0, 0}
	}
	if fault != 0 {
		sizes = []int64{9 << 20, 1}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testWalkBucketRangesCaseCallback(
			w,
			r, &testWalkBucketRangesCaseCallbackInputs{
				t:             &t,
				behavior:      &behavior,
				copyCtx:       &copyCtx,
				cancel:        &cancel,
				wantRange:     &wantRange,
				gets:          &gets,
				later:         &later,
				streamed:      &streamed,
				clientRead:    &clientRead,
				completeLater: &completeLater,
				sizes:         &sizes,
				fault:         &fault,
				probeStatus:   &probeStatus,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	opts := &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1}
	if cancel != nil {
		transport := srv.Client().Transport
		opts.Transport = bucketRoundTrip(func(req *http.Request) (*http.Response, error) {
			resp, err := transport.RoundTrip(req)
			if err == nil && req.Method == http.MethodGet && req.URL.Path == "/data/prefix/file01" &&
				req.Header.Get("Range") == wantRange &&
				resp.StatusCode == wantStatus {
				resp.Body = bucketReadBody{ReadCloser: resp.Body, notify: notifyRead}
			}
			return resp, err
		})
	}
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), opts)
	assert.NoError(t, err)

	var data bytes.Buffer
	var archive io.Writer = &data
	if (*behavior) == "ignore_cancel_ordered" {
		gate := make(chan struct{})
		defer close(gate)
		ctx = gatedCancelContext{Context: copyCtx, gate: gate}
		archive = &cancelArchiveWriter{cancel: cancelCopy}
	}
	desc, err := walkBucket(ctx, client, &manifest.Bucket{BucketName: "data", Prefix: "prefix/"}, archive)
	if probeStatus != 0 {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		if !ok || response.StatusCode != probeStatus || response.Op != http.MethodGet ||
			err.Error() != "range probe failure" ||
			gets.Load() != 1 {
			t.Fatalf("probe failure: %v, response=%+v, requests=%d", err, response, gets.Load())
		}
		if (*behavior) == "probe_forbidden" {
			cause, ok := errors.AsType[minio.ErrorResponse](err)
			assert.FatalIf(t, !ok || cause.StatusCode != probeStatus || !errors.Is(response.Err, cause) ||
				err.Error() != cause.Error(), "probe MinIO cause: %v, response=%+v, cause=%+v", err, response, cause)
		}
		return
	}
	shouldReturn := testWalkBucketRangesCaseCompletion(
		t,
		&testWalkBucketRangesCaseCompletionInputs{
			fault:      &fault,
			err:        &err,
			behavior:   &behavior,
			streamed:   &streamed,
			clientRead: &clientRead,
			gets:       &gets,
			sizes:      &sizes,
			data:       &data,
			desc:       &desc,
		},
	)
	if shouldReturn {
		return
	}
}

func testWalkBucketRangesCaseCompletion(t *testing.T, captures *testWalkBucketRangesCaseCompletionInputs) bool {
	t.Helper()
	if (*captures.fault) != 0 {
		{
			response, ok := errors.AsType[*fetcherr.ResponseError]((*captures.err))
			assert.FatalIf(t, !ok || response.StatusCode != (*captures.fault) ||
				response.Op != http.MethodGet, "streamed sibling failure: %v, want status %d", (*captures.err), (*captures.fault))
		}
		if (*(*captures.behavior)) != "ignore_server" {
			{
				response, ok := errors.AsType[minio.ErrorResponse]((*captures.err))
				assert.FatalIf(
					t,
					!ok || response.StatusCode != (*captures.fault),
					"streamed sibling MinIO cause: %v, want status %d", (*captures.err), (*captures.fault),
				)
			}
		}
		return true
	}
	if (*(*captures.behavior)) != "ordered" && (*(*captures.behavior)) != "ignore" &&
		(*(*captures.behavior)) != "ignore_big" {
		assert.FatalIf(t, (*captures.err) == nil, "%s", "invalid range accepted")

		assert.FatalIf(
			t,
			!strings.HasSuffix((*(*captures.behavior)), "cancel") &&
				errors.Is((*captures.err), context.DeadlineExceeded),
			"range fault timed out: %v",
			(*captures.err),
		)
		switch *(*captures.behavior) {
		case "ignore_cancel_ordered":
			assert.FatalIf(t, !errors.Is((*captures.err), context.Canceled), "context identity: %v", (*captures.err))
		case "changed":
			{
				response, ok := errors.AsType[*fetcherr.ResponseError]((*captures.err))
				assert.FatalIf(t, !ok || response.StatusCode != http.StatusPreconditionFailed ||
					response.Op != http.MethodGet, "ETag mismatch status: %v", (*captures.err))
			}
			{
				response, ok := errors.AsType[minio.ErrorResponse]((*captures.err))
				assert.FatalIf(t, !ok ||
					response.StatusCode != http.StatusPreconditionFailed, "ETag mismatch: %v", (*captures.err))
			}
		case "wrong_range":
			assert.FatalIf(
				t,
				!strings.Contains((*captures.err).Error(), "Content-Range"),
				"wrong range: %v",
				(*captures.err),
			)
		case "short":
			assert.FatalIf(t, !errors.Is((*captures.err), io.ErrUnexpectedEOF), "short range: %v", (*captures.err))
		case "long":
			assert.FatalIf(
				t, (*captures.err).
					Error() != `bucket object "prefix/file01" exceeds chunk size`, "long range: %v", (*captures.err),
			)
		case "ignore_short":
			assert.FatalIf(
				t,
				!strings.Contains((*captures.err).Error(), "missed writing 1 bytes"),
				"short stream: %v",
				(*captures.err),
			)
		case "ignore_long":
			assert.FatalIf(t, !errors.Is((*captures.err), tar.ErrWriteTooLong), "long stream: %v", (*captures.err))
		case "cancel", "ignore_cancel":
			assert.FatalIf(
				t,
				!errors.Is((*captures.err), context.DeadlineExceeded),
				"context identity: %v",
				(*captures.err),
			)
			select {
			case <-(*captures.streamed):
			default:
				t.Fatal("canceled before partial stream")
			}
			select {
			case <-(*captures.clientRead):
			default:
				t.Fatal("canceled before client read")
			}
		}
		return true
	}
	return testWalkBucketRangesCaseCompletionTail(t, captures)
}

type testWalkBucketLargeListedSizeCaseCallbackInputs struct {
	t                **testing.T
	ranges           **bool
	cause            *error
	arrived          *chan struct{}
	gate             *chan struct{}
	active           *atomic.Int32
	peak             *atomic.Int32
	gets             *[2]atomic.Int32
	listDeadline     *time.Time
	downloadDeadline *time.Time
}

func testWalkBucketLargeListedSizeCaseCallback(
	req *http.Request, testCaseInputValue *testWalkBucketLargeListedSizeCaseCallbackInputs,
) (*http.Response, error) {
	testCaseInput := *testCaseInputValue
	deadline, ok := req.Context().Deadline()
	const size int64 = 1073741825
	const timeout time.Duration = 10000000000

	remaining := time.Until(deadline)
	assert.ErrorIf((*testCaseInput.t), !ok || remaining <= timeout-time.Second || remaining > timeout,
		"large object deadline: present=%t remaining=%v, want %v",
		ok,
		remaining,
		timeout,
	)
	if req.URL.Query().Get("list-type") != "" {
		(*testCaseInput.listDeadline) = deadline
		var listing strings.Builder
		listing.WriteString("<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for i := range 2 {
			fmt.Fprintf(
				&listing,
				"<Contents><Key>big%d</Key><Size>%d</Size><ETag>\"v1\"</ETag></Contents>",
				i,
				size,
			)
		}
		listing.WriteString("</ListBucketResult>")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(listing.String())),
			Request:    req,
		}, nil
	}
	if req.Header.Get("If-Match") != `"v1"` {
		(*testCaseInput.t).
			Errorf("large object ETag: %q", req.Header.Get("If-Match"))
	}
	response := &http.Response{
		StatusCode:    http.StatusOK,
		Header:        make(http.Header),
		Request:       req,
		ContentLength: size,
	}
	response.Header.Set("ETag", `"v1"`)
	response.Header.Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	probe := req.Header.Get("Range") == "bytes=0-0"
	if *(*testCaseInput.ranges) {
		var start, end int64
		if n, err := fmt.Sscanf(
			req.Header.Get("Range"),
			"bytes=%d-%d",
			&start,
			&end,
		); n != 2 || err != nil ||
			end-start+1 > chunkBytes {
			(*testCaseInput.t).
				Errorf("large object range: %q", req.Header.Get("Range"))
		}
		response.StatusCode = http.StatusPartialContent
		response.ContentLength = end - start + 1
		response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	} else if !probe && req.Header.Get("Range") != "" {
		(*testCaseInput.t).
			Errorf("ignored-range object split: %q", req.Header.Get("Range"))
	}
	response.Header.Set("Content-Length", strconv.FormatInt(response.ContentLength, 10))
	if probe {
		(*testCaseInput.downloadDeadline) = deadline
		assert.ErrorIf(
			(*testCaseInput.t),
			!testCaseInput.downloadDeadline.Equal((*testCaseInput.listDeadline)),
			"probe deadline=%v, want listing deadline=%v",
			(*testCaseInput.downloadDeadline),
			(*testCaseInput.listDeadline),
		)
		response.Body = io.NopCloser(strings.NewReader("x"))
		return response, nil
	}
	assert.ErrorIf((*testCaseInput.t), !deadline.Equal((*testCaseInput.downloadDeadline)),
		"download deadline=%v, want probe deadline=%v",
		deadline, (*testCaseInput.downloadDeadline),
	)
	i, err := strconv.Atoi(strings.TrimPrefix(req.URL.Path, "/data/big"))
	if err != nil || i < 0 || i >= len((*testCaseInput.gets)) {
		return nil, fmt.Errorf("unexpected large bucket path: %s", req.URL.Path)
	}
	(*testCaseInput.gets)[i].Add(1)
	n := testCaseInput.active.Add(1)
	defer testCaseInput.active.Add(-1)
	for old := testCaseInput.peak.Load(); old < n; old = testCaseInput.peak.Load() {
		if testCaseInput.peak.CompareAndSwap(old, n) {
			break
		}
	}
	select {
	case (*testCaseInput.arrived) <- struct{}{}:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	select {
	case <-(*testCaseInput.gate):
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	response.Body = io.NopCloser(iotest.ErrReader((*testCaseInput.cause)))
	return response, nil
}

type testWalkBucketRangesCaseCallbackInputs struct {
	t             **testing.T
	behavior      **string
	copyCtx       *context.Context
	cancel        *context.CancelCauseFunc
	wantRange     *string
	gets          *atomic.Int32
	later         *chan struct{}
	streamed      *chan struct{}
	clientRead    *chan struct{}
	completeLater *func()
	sizes         *[]int64
	fault         *int
	probeStatus   *int
}

func testWalkBucketRangesCaseCallback(
	w http.ResponseWriter,
	r *http.Request, testCaseInputValue *testWalkBucketRangesCaseCallbackInputs,
) {
	testCaseInput := *testCaseInputValue
	if r.URL.Query().Get("list-type") != "" {
		_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for i, size := range slices.Backward((*testCaseInput.sizes)) {
			_, _ = fmt.Fprintf(
				w,
				`<Contents><Key>prefix/file%02d</Key><Size>%d</Size><ETag>"v1"</ETag></Contents>`,
				i,
				size,
			)
		}
		_, _ = fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	const part = 8388608
	testCaseInput.gets.
		Add(1)
	{
		got := r.Header.Get("If-Match")
		assert.ErrorIf((*testCaseInput.t), got != `"v1"`, "If-Match=%q, want listed ETag", got)
	}
	if (*testCaseInput.probeStatus) != 0 {
		{
			got := r.Header.Get("Range")
			assert.ErrorIf((*testCaseInput.t), got != "bytes=0-0", "probe Range=%q, want bytes=0-0", got)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader((*testCaseInput.probeStatus))
		_, _ = io.WriteString(w, `<Error><Code>Fault</Code><Message>range probe failure</Message></Error>`)
		return
	}
	i, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/data/prefix/file"))
	if (*testCaseInput.fault) != 0 && i == 1 {
		select {
		case <-(*testCaseInput.streamed):
		case <-r.Context().Done():
			return
		}
		http.Error(w, "sibling failed", (*testCaseInput.fault))
		return
	}
	start, end := int64(0), (*testCaseInput.sizes)[i]-1
	status := http.StatusOK
	if (*testCaseInput.sizes)[i] > 0 && !strings.HasPrefix((*(*testCaseInput.behavior)), "ignore") {
		if n, err := fmt.Sscanf(
			r.Header.Get("Range"),
			"bytes=%d-%d",
			&start,
			&end,
		); n != 2 || err != nil ||
			end-start+1 > part {
			(*testCaseInput.t).
				Errorf("invalid chunk range: %q", r.Header.Get("Range"))
			http.Error(w, "bad range", http.StatusBadRequest)
			return
		}
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, (*testCaseInput.sizes)[i]))
	}
	testWalkBucketRangesCaseCallbackTail(
		&testWalkBucketRangesCaseCallbackTailState{
			w:             &w,
			r:             r,
			testCaseInput: &testCaseInput,
			i:             &i,
			start:         &start,
			end:           &end,
			status:        &status,
		},
	)
}

func testWalkBucketRangesCaseCallbackCompletion(captures *testWalkBucketRangesCaseCallbackCompletionInputs) bool {
	(*captures.w).
		Header().Set("Content-Length", strconv.FormatInt((*captures.length), 10))
	(*captures.w).
		Header().Set("ETag", `"v1"`)
	(*captures.w).
		Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	(*captures.w).
		WriteHeader((*captures.status))
	(*captures.w).(http.Flusher).Flush()
	if (*(*captures.testCaseInput.behavior)) == "ignore_cancel_ordered" {
		_, _ = (*captures.w).Write(make([]byte, 64<<10))
		(*captures.w).(http.Flusher).Flush()
		if (*captures.r).Header.Get("Range") != "" {
			<-(*captures.r).Context().Done()
			return true
		}
		<-(*captures.testCaseInput.copyCtx).Done()
		return true
	}
	if (*captures.i) == 1 &&
		((*(*captures.testCaseInput.behavior)) == "ignore_cancel" && (*captures.r).Header.Get("Range") == "" || (*(*(*captures.testCaseInput).behavior)) == "cancel" && (*captures.start) == captures.part) {
		{
			got := (*captures.r).Header.Get("Range")
			assert.ErrorIf(
				(*captures.testCaseInput.t),
				got != (*captures.testCaseInput.wantRange),
				"cancel Range=%q, want %q",
				got,
				(*captures.testCaseInput.wantRange),
			)
		}
		if _, err := (*captures.w).Write(make([]byte, 64<<10)); err != nil {
			(*captures.testCaseInput.t).
				Errorf("partial stream: %v", err)
			return true
		}
		(*captures.w).(http.Flusher).Flush()
		close((*captures.testCaseInput.streamed))
		select {
		case <-(*captures.testCaseInput.clientRead):
			(*captures.testCaseInput.cancel)(context.DeadlineExceeded)
		case <-(*captures.r).Context().Done():
			return true
		}
		<-(*captures.r).Context().Done()
		return true
	}
	return testWalkBucketRangesCaseCallbackCompletionTail(captures)
}

type testWalkBucketRangesCaseCompletionInputs struct {
	fault      *int
	err        *error
	behavior   **string
	streamed   *chan struct{}
	clientRead *chan struct{}
	gets       *atomic.Int32
	sizes      *[]int64
	data       *bytes.Buffer
	desc       *ocispec.Descriptor
}
type testWalkBucketRangesCaseCallbackCompletionInputs struct {
	w             *http.ResponseWriter
	length        *int64
	status        *int
	testCaseInput *testWalkBucketRangesCaseCallbackInputs
	r             **http.Request
	i             *int
	start         *int64
	part          int64
}

func testWalkBucketRangesCaseCallbackCompletionTail(captures *testWalkBucketRangesCaseCallbackCompletionInputs) bool {
	if (*captures.testCaseInput.fault) != 0 && (*captures.i) == 0 && (*captures.r).Header.Get("Range") == "" {
		_, _ = (*captures.w).Write(make([]byte, 64<<10))
		(*captures.w).(http.Flusher).Flush()
		close((*captures.testCaseInput.streamed))
		<-(*captures.r).Context().Done()
		return true
	}
	if (*(*captures.testCaseInput.behavior)) == "ordered" && (*captures.i) == 1 && (*captures.start) == 0 {
		select {
		case <-(*captures.testCaseInput.later):
		case <-(*captures.r).Context().Done():
			return true
		}
	}
	for sent := int64(0); sent < (*captures.length); {
		n := min(int64(64<<10), (*captures.length)-sent)
		buf := bytes.Repeat([]byte{byte(((*captures.start)+sent)/(1<<20) + int64((*captures.i)))}, int(n))
		if _, err := (*captures.w).Write(buf); err != nil {
			return true
		}
		sent += n
	}
	if (*captures.i) == 1 && (*captures.start) == captures.part {
		(*captures.testCaseInput.completeLater)()
	}
	return false
}

type testWalkBucketRangesCaseCallbackTailState struct {
	w             *http.ResponseWriter
	r             *http.Request
	testCaseInput *testWalkBucketRangesCaseCallbackInputs
	i             *int
	start         *int64
	end           *int64
	status        *int
}

func testWalkBucketRangesCaseCallbackTail(scope *testWalkBucketRangesCaseCallbackTailState) {
	const part = 8388608
	if (*(*(*scope.testCaseInput).behavior)) == "wrong_range" && (*scope.start) == part {
		(*scope.w).Header().
			Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", (*scope.start)+1, (*scope.end)+1, (*(*scope.testCaseInput).sizes)[(*scope.i)]))
	}
	if (*(*(*scope.testCaseInput).behavior)) == "changed" && (*scope.start) >= part {
		http.Error((*scope.w), "changed object", http.StatusPreconditionFailed)
		return
	}
	length := (*scope.end) - (*scope.start) + 1
	if (*scope.i) == 1 &&
		((*scope.start) == part || strings.HasPrefix((*(*(*scope.testCaseInput).behavior)), "ignore_")) {
		switch strings.TrimPrefix((*(*(*scope.testCaseInput).behavior)), "ignore_") {
		case "short":
			length--
		case "long":
			length++
		}
	}
	shouldReturn := testWalkBucketRangesCaseCallbackCompletion(
		&testWalkBucketRangesCaseCallbackCompletionInputs{
			w:             &(*scope.w),
			length:        &length,
			status:        &(*scope.status),
			testCaseInput: &(*scope.testCaseInput),
			r:             &scope.r,
			i:             &(*scope.i),
			start:         &(*scope.start),
			part:          int64(part),
		},
	)
	if shouldReturn {
		return
	}
}

func testWalkBucketRangesCaseCompletionTail(t *testing.T, captures *testWalkBucketRangesCaseCompletionInputs) bool {
	t.Helper()
	assert.NoError(t, (*captures.err))

	wantGets := int32(8)
	if (*(*captures.behavior)) == "ignore" || (*(*captures.behavior)) == "ignore_big" {
		wantGets = 4
	}
	if captures.gets.Load() != wantGets {
		t.Fatalf("GETs=%d, want %d", captures.gets.Load(), wantGets)
	}
	h := sha256.New()
	tw := tar.NewWriter(h)
	for i, size := range *captures.sizes {
		{
			err := tw.WriteHeader(
				&tar.Header{Name: fmt.Sprintf("file%02d", i), Mode: 0o600, Size: size},
			)
			assert.NoError(t, err)
		}

		for offset := int64(0); offset < size; offset += 1 << 20 {
			_, _ = tw.Write(bytes.Repeat([]byte{byte(offset/(1<<20) + int64(i))}, int(min(1<<20, size-offset))))
		}
	}
	{
		err := tw.Close()
		assert.NoError(t, err)
	}

	{
		got, want := fmt.Sprintf(
			"%x",
			sha256.Sum256(captures.data.Bytes()),
		), hex.EncodeToString(
			h.Sum(nil),
		)
		assert.FatalIf(t, got != want, "tar digest=%s, want %s", got, want)
	}
	revision := fmt.Sprintf(
		"sha256:%x",
		sha256.Sum256([]byte("prefix/file00 v1\nprefix/file01 v1\nprefix/file02 v1\n")),
	)
	if captures.desc.Annotations["flate.revision"] != revision ||
		(*captures.desc).Annotations["flate.object-count"] != "3" {
		t.Fatalf("metadata=%v, want revision=%s count=3", captures.desc.Annotations, revision)
	}
	return false
}
