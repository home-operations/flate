package helmchart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

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
