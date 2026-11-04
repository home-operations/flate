package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testWalkBucketLargeListedSizeCase(t *testing.T, ranges *bool,
) {
	t.Helper()
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
	testCaseInput.gets[i].Add(1)
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
