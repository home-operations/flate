package helmchart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetchHTTPChart_NestedIndexExhaustion_DueWave(t *testing.T) {
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	for _, callers := range []int{2, 4} {
		for _, result := range []string{"success", "failure", "cold child", "cached child"} {
			t.Run(fmt.Sprintf("callers-%d/%s", callers, result), func(t *testing.T) {
				testFetchHTTPChartNestedIndexExhaustionDueWaveCase(
					t,

					&chartBytes,
					&callers,
					&result,
				)
			},
			)
		}
	}
}

func TestFetchHTTPChart_NestedRetryDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := httpRepo("https://charts.invalid")
		r.Timeout = &metav1.Duration{Duration: 5 * time.Second}
		f, inputs := newHTTPFetcher(t, r)
		prepared := preparedHTTP(t, f, inputs, r)
		start := time.Now()
		calls := 0
		prepared.client.Transport = chartTransport(func(req *http.Request) (*http.Response, error) {
			calls++
			{
				deadline, ok := req.Context().Deadline()
				assert.ErrorIf(
					t,
					!ok || !deadline.Equal(start.Add(5*time.Second)),
					"nested retry deadline=%v present=%t",
					deadline,
					ok,
				)
			}
			select {
			case <-time.After(2 * time.Second):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    req,
			}, nil
		})
		_, err := f.fetchHTTPChart(
			t.Context(),
			prepared,
			helmChart(r.Name, "app-template", "1.0.0"),
			&sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Second}},
		)
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) || calls != 2 ||
			time.Since(start) != 5*time.Second {
			t.Fatalf("nested retries: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
		}
	})
}

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
			&sourcecache.Policy{},
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
				&sourcecache.Policy{
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
