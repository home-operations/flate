package helmchart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetchHTTPChartNestedIndexExhaustionDueWaveCase(
	t *testing.T, chartBytes *[]byte,
	callers *int,
	result *string,
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallback(
			t,
			testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackInputs{
				chartBytes: &chartBytes,
				callers:    &callers,
				result:     &result,
			},
		)
	},
	)
}

func testFetchHTTPChartRequestTimeoutsCase(t *testing.T, tc *struct {
	stage  string
	reopen bool
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testFetchHTTPChartRequestTimeoutsCaseCallback(t, testFetchHTTPChartRequestTimeoutsCaseCallbackInputs{tc: &tc})
	},
	)
}

type testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackInputs struct {
	chartBytes **[]byte
	callers    **int
	result     **string
}

func testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallback(
	t *testing.T,
	testCaseInput testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackInputs,
) {
	t.Helper()
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	defer func() { _ = c.Close() }()
	repo := httpRepo("https://charts.invalid")
	repo.Interval.Duration = 0
	fInputs := testInputs{}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	prepared := preparedHTTP(t, f, fInputs, repo)
	outer := struct{ Repository, Chart, Transport any }{
		repo.Content(),
		helmChart("repo", "app-template", "1.0.0").Content(),
		prepared.material,
	}
	inner := struct{ Repository, Transport any }{repo.Content(), prepared.material}
	cause := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	seed := func(content any) {
		_, _, _, blobErr := c.Blob(
			t.Context(),
			sourcecache.BlobKey{Content: content},
			sourcecache.Policy{},
			func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				return sourcecache.ProducerResult{}, cause
			},
		)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](blobErr)
		assert.FatalIf(t, !ok || !terminal.Attempted, "seed: %v", blobErr)
	}
	seed(outer)
	if (*(*testCaseInput.result)) == "cached child" {
		time.Sleep(100 * time.Millisecond)
	}
	if (*(*testCaseInput.result)) != "cold child" {
		seed(inner)
	}
	before, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
	assert.NoError(t, err)

	delay := 750 * time.Millisecond
	if (*(*testCaseInput.result)) == "cached child" {
		delay -= 100 * time.Millisecond
	}
	time.Sleep(delay)
	gate := make(chan struct{})
	var indexCalls, chartCalls atomic.Int32
	prepared.client.Transport = chartTransport(func(req *http.Request) (*http.Response, error) {
		body := string((*(*testCaseInput.chartBytes)))
		if strings.HasSuffix(req.URL.Path, "index.yaml") {
			indexCalls.Add(1)
			<-gate
			if (*(*testCaseInput.result)) == "failure" || (*(*testCaseInput.result)) == "cold child" {
				return nil, cause
			}
			body = helmRepoIndex(chartDigest((*(*testCaseInput.chartBytes))))
		} else {
			chartCalls.Add(1)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})
	var wg sync.WaitGroup
	results := make(chan error, (*(*testCaseInput.callers)))
	for range *(*testCaseInput.callers) {
		wg.Go(func() {
			_, err := f.fetchHTTPChart(
				t.Context(),
				prepared,
				helmChart("repo", "app-template", "1.0.0"),
				sourcecache.Policy{
					Retry: sourcecache.RetryConfig{
						Attempts: 5,
						MinWait:  time.Nanosecond,
						MaxWait:  time.Nanosecond,
					},
				},
			)
			results <- err
		})
	}
	testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackCompletion(
		t,
		&testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackCompletionInputs{
			gate:          &gate,
			wg:            &wg,
			results:       &results,
			testCaseInput: &testCaseInput,
			indexCalls:    &indexCalls,
			chartCalls:    &chartCalls,
			root:          &root,
			before:        &before,
		},
	)
}

func testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackCompletion(
	t *testing.T,
	captures *testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackCompletionInputs,
) {
	t.Helper()
	synctest.Wait()
	close((*captures.gate))
	captures.wg.
		Wait()
	close((*captures.results))
	attempted := 0
	for err := range *captures.results {
		if (*(*captures.testCaseInput.result)) == "success" {
			assert.NoError(t, err)

			continue
		}
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		assert.FatalIf(t, !ok || !errors.Is(err, syscall.ECONNRESET), "nested cause: %v", err)
		if terminal.Attempted {
			attempted++
		}
	}
	wantIndex, wantChart, wantAttempted := int32(1), int32(0), 1
	if (*(*captures.testCaseInput.result)) == "success" {
		wantChart, wantAttempted = 1, 0
	}
	if (*(*captures.testCaseInput.result)) == "cached child" {
		wantIndex, wantAttempted = 0, 0
	}
	if captures.indexCalls.Load() != wantIndex || captures.chartCalls.Load() != wantChart ||
		attempted != wantAttempted {
		t.Fatalf("index=%d chart=%d attempted=%d", captures.indexCalls.Load(), captures.chartCalls.Load(), attempted)
	}
	if (*(*captures.testCaseInput.result)) == "cached child" {
		after, err := os.ReadFile(filepath.Join((*captures.root), "oci", "index.json"))
		assert.FatalIf(
			t,
			err != nil || !bytes.Equal((*captures.before), after),
			"%s",
			"cached child advanced outer sequence",
		)
	}
}

type testFetchHTTPChartRequestTimeoutsCaseCallbackInputs struct {
	tc **struct {
		stage  string
		reopen bool
	}
}

func testFetchHTTPChartRequestTimeoutsCaseCallback(
	t *testing.T,
	testCaseInput testFetchHTTPChartRequestTimeoutsCaseCallbackInputs,
) {
	t.Helper()
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	root := t.TempDir()
	network := newTestNetwork(t)
	f := New(nil, Config{Network: network, Cache: cachetest.New(t, root, sourcecache.New)})
	r := httpRepo("https://charts.invalid")
	r.Timeout = &metav1.Duration{Duration: 5 * time.Second}
	blocked := true
	calls := 0
	deadline := time.Now().Add(5 * time.Second)
	tr := chartTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		{
			got, ok := req.Context().Deadline()
			assert.ErrorIf(
				t,
				!ok || !got.Equal(deadline),
				"operation deadline=%v present=%t, want %v",
				got,
				ok,
				deadline,
			)
		}
		index := strings.HasSuffix(req.URL.Path, "/index.yaml")
		data := chart
		if index {
			data = []byte(helmRepoIndex(chartDigest(chart)))
		}
		select {
		case <-time.After(3 * time.Second):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		body := io.NopCloser(bytes.NewReader(data))
		if blocked &&
			(index && (*testCaseInput.tc).stage == "index" || !index && (*testCaseInput.tc).stage == "chart") {
			body = timeoutChartBody{req.Context()}
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	})
	prepared := preparedHTTP(t, f, testInputs{}, r)
	prepared.client.Transport = tr
	hc := helmChart(r.Name, "app-template", "1.0.0")
	start := time.Now()
	_, err := f.fetchHTTPChart(t.Context(), prepared, hc, sourcecache.Policy{MaxAge: time.Hour})
	want := 5 * time.Second
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != want {
		t.Fatalf("request timeout: elapsed=%v error=%v", time.Since(start), err)
	}
	assert.FatalIf(t, len(f.indexDecodes) != 0, "%s", "timed out operation retained an index permit")

	if (*testCaseInput.tc).reopen {
		{
			closeErr3 := f.cache.Close()
			assert.NoError(t, closeErr3)
		}

		f = New(nil, Config{Network: network, Cache: cachetest.New(t, root, sourcecache.New)})
	}
	blocked = false
	longer := *r
	longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
	prepared = preparedHTTP(t, f, testInputs{}, &longer)
	prepared.client.Transport = tr
	start = time.Now()
	deadline = start.Add(10 * time.Second)
	acquired, err := f.fetchHTTPChart(t.Context(), prepared, hc, sourcecache.Policy{MaxAge: time.Hour})
	assert.FatalIf(t, err != nil, "longer policy replayed timeout: %v", err)
	want = 6 * time.Second
	if (*testCaseInput.tc).stage == "chart" {
		want = 3 * time.Second
	}
	if time.Since(start) != want {
		t.Fatalf("recovered operation: elapsed=%v want=%v", time.Since(start), want)
	}
	{
		body, readFileErr := fs.ReadFile(
			acquired.FS,
			source.HelmChartArchiveFilename,
		)
		assert.FatalIf(t, readFileErr != nil ||
			!bytes.Equal(body, chart), "chart bytes differ: %v", readFileErr)
	}
	{
		closeErr4 := f.cache.Close()
		assert.NoError(t, closeErr4)
	}

	f = New(nil, Config{Network: network, Cache: cachetest.New(t, root, sourcecache.New)})
	shorter := longer
	shorter.Timeout = &metav1.Duration{Duration: 0}
	prepared = preparedHTTP(t, f, testInputs{}, &shorter)
	prepared.client.Transport = tr
	before := calls
	warm, err := f.fetchHTTPChart(t.Context(), prepared, hc, sourcecache.Policy{MaxAge: time.Hour})
	assert.FatalIf(
		t,
		err != nil || calls != before || warm.Key != acquired.Key,
		"timeout changed persistent content identity: calls=%d->%d error=%v",
		before,
		calls,
		err,
	)
}

type testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackCompletionInputs struct {
	gate          *chan struct{}
	wg            *sync.WaitGroup
	results       *chan error
	testCaseInput *testFetchHTTPChartNestedIndexExhaustionDueWaveCaseCallbackInputs
	indexCalls    *atomic.Int32
	chartCalls    *atomic.Int32
	root          *string
	before        *[]byte
}
