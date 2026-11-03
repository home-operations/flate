package helmchart

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetchHTTPChart_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"index", "chart"} {
		for _, authenticated := range []bool{false, true} {
			for _, customTLS := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/auth-%t/tls-%t", route, authenticated, customTLS), func(t *testing.T) {
					testFetchHTTPChartNetworkRoutesCase(
						t,

						&route,
						&authenticated,
						&customTLS,
					)
				},
				)
			}
		}
	}
}

func preparedHTTP(t *testing.T, f *Fetcher, fInputs testInputs, r *manifest.HelmRepository) *httpRepository {
	t.Helper()
	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
	assert.NoError(t, err)

	return prepared
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetchHTTPChart_BasicAuth_SnapshotRotation(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	var fail atomic.Bool
	var hits, lookups atomic.Int32
	var password atomic.Value
	password.Store("first")
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetchHTTPChartBasicAuthSnapshotRotationCallback(
			w,
			r,
			testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs{
				t:             &t,
				chartBytes:    &chartBytes,
				fail:          &fail,
				hits:          &hits,
				password:      &password,
				proxyPassword: &proxyPassword,
			},
		)
	},
	)
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	r := httpRepo(srv.URL)
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "creds"}
	root := t.TempDir()
	ca := testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			testFetchHTTPChartBasicAuthSnapshotRotationCase(
				t, &testFetchHTTPChartBasicAuthSnapshotRotationCaseInputs{
					fail:          &fail,
					hits:          &hits,
					lookups:       &lookups,
					password:      &password,
					proxyPassword: &proxyPassword,
					proxy:         &proxy,
					r:             &r,
					root:          &root,
					ca:            &ca,
					previous:      &previous,
					wave:          &wave,
				},
			)
		},
		)
	}
	if lookups.Load() != 15 {
		t.Fatalf("chart/index reread material: %d", lookups.Load())
	}
}

type chartTransport func(*http.Request) (*http.Response, error)

func (f chartTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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

func TestFetchHTTPChart_RequestTimeouts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		stage  string
		reopen bool
	}{{"index", false}, {"chart", true}} {
		t.Run(tc.stage, func(t *testing.T) {
			t.Parallel()
			testFetchHTTPChartRequestTimeoutsCase(t, &tc)
		},
		)
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
			sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Second}},
		)
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, manifest.ErrFlux) || calls != 2 ||
			time.Since(start) != 5*time.Second {
			t.Fatalf("nested retries: calls=%d elapsed=%v error=%v", calls, time.Since(start), err)
		}
	})
}

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

type testFetchHTTPChartCaseInputs struct {
	charts    *map[string][]byte
	indexHits *atomic.Int32
	chartHits *atomic.Int32
	f         **Fetcher
	fInputs   *testInputs
}

func testFetchHTTPChartCase(t *testing.T, testCaseInput testFetchHTTPChartCaseInputs) {
	t.Helper()

	var consumers sync.WaitGroup
	for name, data := range *testCaseInput.charts {
		consumers.Go(func() {
			art, err := (*testCaseInput.f).Fetch(
				t.Context(),
				helmChart("repo", name, "1.0.0"), testCaseInput.fInputs.
					readSet(helmChart("repo", name, "1.0.0")), sourcecache.Policy{},
			)
			if err != nil {
				t.Errorf("Fetch %s: %v", name, err)
				return
			}
			if art.Content == "" || art.Content != sourcecache.AcquisitionIdentity(art.FS) {
				t.Errorf("%s content generation = %q", name, art.Content)
			}
			assert.ErrorIf(
				t,
				art.Kind != manifest.KindHelmChart || art.Digest != chartDigest(data),
				"%s artifact = %+v",
				name,
				art,
			)
			{
				_, err := fs.Stat(art.FS, "chart.tgz")
				assert.ErrorIf(t, err != nil, "%s chart.tgz: %v", name, err)
			}
		})
	}
	consumers.Wait()
	t.Logf("index requests=%d chart requests=%d", testCaseInput.indexHits.Load(), testCaseInput.chartHits.Load())
	if testCaseInput.indexHits.Load() != 1 ||
		testCaseInput.chartHits.Load() != int32(len((*testCaseInput.charts))) {
		t.Errorf(
			"requests: index=%d charts=%d, want 1/%d",
			testCaseInput.indexHits.Load(),
			testCaseInput.chartHits.Load(),
			len((*testCaseInput.charts)),
		)
	}
}

func testFetchIndexInvalidResponseDoesNotPublishCase(
	t *testing.T, tc *struct {
		name string
		data string
	},
) {
	t.Helper()

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			_, _ = io.WriteString(w, tc.data)
			return
		}
		_, _ = io.WriteString(w, helmRepoIndex(""))
	}))
	t.Cleanup(srv.Close)
	r := httpRepo(srv.URL)
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	fInputs := testInputs{}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})
	{
		_, fetchIndexErr := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), sourcecache.Policy{})
		assert.FatalIf(t, fetchIndexErr == nil, "%s", "accepted invalid index")
	}
	{
		_, ok := f.indexCache.Load("sha256:" + chartDigest([]byte(tc.data)))
		assert.FatalIf(t, ok, "%s", "invalid index entered parsed memo")
	}

	idx, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), sourcecache.Policy{})
	assert.FatalIf(t, err != nil, "retry within repository interval: %v", err)
	{
		_, getErr3 := idx.Get("app-template", "1.0.0")
		assert.NoError(t, getErr3)
	}
	{
		closeErr := c.Close()
		assert.NoError(t, closeErr)
	}

	c, err = sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	warmInputs := testInputs{}
	warm := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	cached, err := warm.fetchIndex(t.Context(), preparedHTTP(t, warm, warmInputs, r), sourcecache.Policy{})
	assert.FatalIf(t, err != nil, "disk-cached index: %v", err)
	{
		memo, ok := warm.indexCache.Load(
			"sha256:" + chartDigest([]byte(helmRepoIndex(""))),
		)
		assert.FatalIf(t, !ok ||
			memo != cached, "%s", "disk-cached index did not populate digest-bound memo")
	}

	{
		got := attempts.Load()
		assert.FatalIf(t, got != 2, "index fetched %d times, want 2", got)
	}
}

type testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs struct {
	t             **testing.T
	chartBytes    *[]byte
	fail          *atomic.Bool
	hits          *atomic.Int32
	password      *atomic.Value
	proxyPassword *atomic.Value
}

func testFetchHTTPChartBasicAuthSnapshotRotationCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetchHTTPChartBasicAuthSnapshotRotationCallbackInputs,
) {
	if r.URL.IsAbs() &&
		r.Header.Get(
			"Proxy-Authorization",
		) != "Basic "+base64.StdEncoding.EncodeToString(
			[]byte("proxy:"+testCaseInput.proxyPassword.Load().(string)),
		) {
		(*testCaseInput.t).
			Error("transport used a different proxy snapshot")
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	testCaseInput.hits.
		Add(1)
	user, pass, _ := r.BasicAuth()
	if user != "alice" || pass != testCaseInput.password.Load().(string) {
		(*testCaseInput.t).
			Error("chart/index credential snapshot changed")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if testCaseInput.fail.Load() {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			(*testCaseInput.t).
				Error(err)
			return
		}
		if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
			(*testCaseInput.t).
				Error(err)
		}
		_ = conn.Close()
		return
	}
	if strings.HasSuffix(r.URL.Path, "index.yaml") {
		_, _ = io.WriteString(w, helmRepoIndex(chartDigest((*testCaseInput.chartBytes))))
	} else {
		_, _ = w.Write((*testCaseInput.chartBytes))
	}
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
		index := strings.HasSuffix(req.URL.Path, "/index.yaml")
		if req.URL.Host == "slow.invalid" && index {
			close(entered)
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		if req.URL.Host == "healthy.invalid" {
			healthyCalls++
			if index {
				admitted = time.Now()
			}
			{
				deadline, ok := req.Context().Deadline()
				assert.ErrorIf(
					t,
					!ok || !deadline.Equal(admitted.Add(5*time.Second)),
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
		data := chart
		if index {
			data = []byte(helmRepoIndex(chartDigest(chart)))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(data)),
			Request:    req,
		}, nil
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
