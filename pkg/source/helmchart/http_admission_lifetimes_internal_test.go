package helmchart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetchHTTPChart_AdmissionTimeoutIsolation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) { testFetchHTTPChartAdmissionTimeoutIsolationCallback(t) })
}

func TestFetchHTTPChart_AdmissionCancellation(t *testing.T) {
	t.Parallel()
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				r := httpRepo("https://queued.invalid")
				r.Timeout = &metav1.Duration{Duration: 5 * time.Second}
				f, inputs := newHTTPFetcher(t, r)
				prepared := preparedHTTP(t, f, inputs, r)
				calls := 0
				prepared.client.Transport = chartTransport(func(*http.Request) (*http.Response, error) {
					calls++
					return nil, errors.New("queued caller reached transport")
				})
				f.indexDecodes <- struct{}{}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if deadline {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 2*time.Second)
					defer stop()
				}
				done := make(chan error, 1)
				go func() {
					_, err := f.fetchHTTPChart(
						ctx,
						prepared,
						helmChart(r.Name, "app-template", "1.0.0"),
						sourcecache.Policy{},
					)
					done <- err
				}()
				synctest.Wait()
				want := context.Canceled
				if deadline {
					time.Sleep(2 * time.Second)
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				if err := <-done; !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) || calls != 0 ||
					len(f.indexDecodes) != 1 {
					t.Fatalf("queued cancellation: requests=%d permits=%d error=%v", calls, len(f.indexDecodes), err)
				}
				<-f.indexDecodes
			})
		})
	}
}

func TestFetchHTTPChart_AdmissionTransferOverlap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) { testFetchHTTPChartAdmissionTransferOverlapCallback(t) })
}

func testFetchHTTPChartAdmissionTimeoutIsolationCallback(t *testing.T) {
	t.Helper()
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	slow := httpRepo("https://slow.invalid")
	slow.Timeout = &metav1.Duration{Duration: 2 * time.Minute}
	healthy := httpRepo("https://healthy.invalid")
	healthy.Timeout = &metav1.Duration{Duration: 5 * time.Second}
	f, inputs := newHTTPFetcher(t, slow)
	entered, release := make(chan struct{}), make(chan struct{})
	healthyCalls := 0
	var admitted time.Time
	tr := chartTransport(func(req *http.Request) (*http.Response, error) {
		return testAdmissionTransport(t, req, testAdmissionTransportInputs{
			chart: &chart, entered: &entered, release: &release, healthyCalls: &healthyCalls, admitted: &admitted,
		})
	})
	testFetchHTTPChartAdmissionTimeoutIsolationCallbackCompletion(
		t,
		&testFetchHTTPChartAdmissionTimeoutIsolationCallbackCompletionInputs{
			f:            &f,
			inputs:       &inputs,
			tr:           &tr,
			slow:         &slow,
			entered:      &entered,
			healthy:      &healthy,
			healthyCalls: &healthyCalls,
			release:      &release,
			admitted:     &admitted,
		},
	)
}

type testAdmissionTransportInputs struct {
	chart        *[]byte
	entered      *chan struct{}
	release      *chan struct{}
	healthyCalls *int
	admitted     *time.Time
}

func testAdmissionTransport(
	t *testing.T,
	req *http.Request,
	inputs testAdmissionTransportInputs,
) (*http.Response, error) {
	t.Helper()
	index := strings.HasSuffix(req.URL.Path, "/index.yaml")
	if req.URL.Host == "slow.invalid" && index {
		close((*inputs.entered))
		select {
		case <-(*inputs.release):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	if req.URL.Host == "healthy.invalid" {
		(*inputs.healthyCalls)++
		if index {
			(*inputs.admitted) = time.Now()
		}
		{
			deadline, ok := req.Context().Deadline()
			assert.ErrorIf(
				t,
				!ok || !deadline.Equal(inputs.admitted.Add(5*time.Second)),
				"healthy deadline=%v present=%t, want admission + five seconds",
				deadline,
				ok,
			)
		}
		if !index {
			select {
			case <-time.After(4 * time.Second):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
	}
	data := (*inputs.chart)
	if index {
		data = []byte(helmRepoIndex(chartDigest((*inputs.chart))))
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(data)),
		Request:    req,
	}, nil
}

func testFetchHTTPChartAdmissionTimeoutIsolationCallbackCompletion(
	t *testing.T,
	captures *testFetchHTTPChartAdmissionTimeoutIsolationCallbackCompletionInputs,
) {
	t.Helper()
	fetch := func(r *manifest.HelmRepository, done chan<- error) {
		prepared := preparedHTTP(t, (*captures.f), (*captures.inputs), r)
		prepared.client.Transport = (*captures.tr)
		_, err := (*captures.f).fetchHTTPChart(
			t.Context(),
			prepared,
			helmChart(r.Name, "app-template", "1.0.0"),
			sourcecache.Policy{},
		)
		done <- err
	}
	slowDone, healthyDone := make(chan error, 1), make(chan error, 1)
	go fetch((*captures.slow), slowDone)
	<-(*captures.entered)
	go fetch((*captures.healthy), healthyDone)
	synctest.Wait()
	time.Sleep(6 * time.Second)
	synctest.Wait()
	assert.ErrorIf(
		t,
		(*captures.healthyCalls) != 0,
		"queued healthy repository made %d requests",
		(*captures.healthyCalls),
	)
	early := false
	select {
	case err := <-healthyDone:
		early = true
		t.Errorf("healthy repository exhausted its operation timeout in the admission queue: %v", err)
	default:
	}
	close((*captures.release))
	if err := <-slowDone; err != nil {
		t.Error(err)
	}
	if !early {
		{
			err := <-healthyDone
			assert.ErrorIf(t, err != nil, "healthy repository after admission: %v", err)
		}
		if (*captures.healthyCalls) != 2 || time.Since((*captures.admitted)) != 4*time.Second {
			t.Errorf(
				"healthy operation: calls=%d elapsed=%v",
				(*captures.healthyCalls),
				time.Since((*captures.admitted)),
			)
		}
	}
	assert.FatalIf(t, len((*captures.f).indexDecodes) != 0, "%s", "index permit leaked")
}

func testFetchHTTPChartAdmissionTransferOverlapCallback(t *testing.T) {
	t.Helper()
	f := New(nil, Config{Cache: cachetest.New(t, "", sourcecache.New)})
	chartStarted, chartRelease := make(chan struct{}), make(chan struct{})
	indexStarted, indexRelease := make(chan struct{}), make(chan struct{})
	tr := chartTransport(func(req *http.Request) (*http.Response, error) {
		index := strings.HasSuffix(req.URL.Path, "/index.yaml")
		if req.URL.Host == "first.invalid" && !index {
			close(chartStarted)
			<-chartRelease
		}
		if req.URL.Host == "second.invalid" && index {
			close(indexStarted)
			<-indexRelease
		}
		data := "chart"
		if index {
			data = helmRepoIndex("")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(data)),
			Request:    req,
		}, nil
	})
	fetch := func(host string, done chan<- error) {
		r := &httpRepository{HelmRepository: httpRepo("https://" + host), client: &http.Client{Transport: tr}}
		_, err := f.fetchHTTPChart(t.Context(), r, helmChart(r.Name, "app-template", "1.0.0"), sourcecache.Policy{})
		done <- err
	}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go fetch("first.invalid", firstDone)
	<-chartStarted
	if len(f.indexDecodes) != 0 {
		t.Error("chart transfer retained the index permit")
	}
	go fetch("second.invalid", secondDone)
	<-indexStarted
	close(chartRelease)
	if err := <-firstDone; err != nil {
		t.Error(err)
	}
	if len(f.indexDecodes) != 1 {
		t.Error("first fill's deferred release stole the second index permit")
	}
	close(indexRelease)
	if err := <-secondDone; err != nil {
		t.Error(err)
	}
	assert.FatalIf(t, len(f.indexDecodes) != 0, "%s", "overlapping transfer leaked a permit")
}

type testFetchHTTPChartAdmissionTimeoutIsolationCallbackCompletionInputs struct {
	f            **Fetcher
	inputs       *testInputs
	tr           *chartTransport
	slow         **manifest.HelmRepository
	entered      *chan struct{}
	healthy      **manifest.HelmRepository
	healthyCalls *int
	release      *chan struct{}
	admitted     *time.Time
}
