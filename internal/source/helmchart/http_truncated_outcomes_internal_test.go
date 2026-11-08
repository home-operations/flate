package helmchart

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

func testFetchHTTPChartTruncatedOutcomeReplayCase(
	t *testing.T, status *int,
	target *string,
	attempts *int,
) {
	t.Helper()
	var indexHits, chartHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		testFetchHTTPChartTruncatedOutcomeReplayCaseCallback(
			w,
			req,
			testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs{
				status:    &status,
				target:    &target,
				indexHits: &indexHits,
				chartHits: &chartHits,
			},
		)
	},
	))
	defer srv.Close()
	r := httpRepo(srv.URL)
	lookup := func(_, _ string) *manifest.HelmRepository { return r }
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	fInputs := testInputs{repos: lookup}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	hc := helmChart("repo", "app-template", "1.0.0")
	retry := sourcecache.RetryConfig{
		Attempts: (*attempts),
		MinWait:  time.Nanosecond,
		MaxWait:  time.Nanosecond,
	}
	_, err = fetchtest.Fetch(t.Context(), f.Fetch, hc, fInputs.readSet(hc), &sourcecache.Policy{Retry: retry}, nil)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(
		t,
		!ok || !terminal.Attempted || !errors.Is(err, manifest.ErrFlux),
		"initial terminal=%+v, error=%v",
		terminal,
		err,
	)
	if (*status) == 0 {
		{
			transport, innerOk := errors.AsType[*url.Error](
				err,
			)
			assert.ErrorIf(t, !innerOk || transport.Op != "Get" || transport.URL != srv.URL+(*target) ||
				!errors.Is(
					transport.Err,
					io.ErrUnexpectedEOF,
				), "initial transport=%+v, error=%v", transport, err)
		}
	} else {
		response, innerOk2 := errors.AsType[*fetcherr.ResponseError](err)
		assert.ErrorIf(
			t,
			!innerOk2 || response.StatusCode != (*status) || response.Op != http.MethodGet ||
				!strings.Contains(response.Error(), srv.URL+(*target)),
			"initial response=%+v, error=%v",
			response,
			err,
		)
	}
	wantIndex, wantChart := int32((*attempts)), int32(0)
	if (*target) != "/index.yaml" {
		wantIndex, wantChart = 1, int32((*attempts))
	}
	if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
		t.Fatalf(
			"initial requests: index=%d chart=%d, want index=%d chart=%d",
			indexHits.Load(),
			chartHits.Load(),
			wantIndex,
			wantChart,
		)
	}
	{
		closeErr2 := c.Close()
		assert.NoError(t, closeErr2)
	}

	warmCache, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = warmCache.Close() })
	assert.FatalIf(t, warmCache == c, "%s", "reopened cache reused the closed owner")

	warmInputs := testInputs{repos: lookup}
	warm := New(nil, Config{Network: newTestNetwork(t), Cache: warmCache})

	_, err = fetchtest.Fetch(
		t.Context(),
		warm.Fetch,
		hc,
		warmInputs.readSet(hc),
		&sourcecache.Policy{Retry: retry},
		nil,
	)
	terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
	assert.ErrorIf(
		t,
		!ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux),
		"replayed terminal=%+v, error=%v",
		terminal,
		err,
	)
	if (*status) == 0 {
		{
			transport, ok := errors.AsType[*url.Error](
				err,
			)
			assert.ErrorIf(t, !ok || transport.Op != "Get" || transport.URL != "" ||
				!errors.Is(
					transport.Err,
					io.ErrUnexpectedEOF,
				), "replayed transport=%+v, error=%v", transport, err)
		}
	} else {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		assert.ErrorIf(
			t,
			!ok || response.StatusCode != (*status) || response.Op != http.MethodGet ||
				strings.Contains(response.Error(), srv.URL),
			"replayed response=%+v, error=%v",
			response,
			err,
		)
	}
	if indexHits.Load() != wantIndex || chartHits.Load() != wantChart {
		t.Errorf(
			"replayed requests: index=%d chart=%d, want index=%d chart=%d",
			indexHits.Load(),
			chartHits.Load(),
			wantIndex,
			wantChart,
		)
	}
}

type testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs struct {
	status    **int
	target    **string
	indexHits *atomic.Int32
	chartHits *atomic.Int32
}

func testFetchHTTPChartTruncatedOutcomeReplayCaseCallback(
	w http.ResponseWriter,
	req *http.Request,
	testCaseInput testFetchHTTPChartTruncatedOutcomeReplayCaseCallbackInputs,
) {
	if req.URL.Path == "/index.yaml" {
		testCaseInput.indexHits.
			Add(1)
		if (*(*testCaseInput.target)) != "/index.yaml" {
			_, _ = io.WriteString(w, helmRepoIndex(""))
			return
		}
	} else {
		testCaseInput.chartHits.
			Add(1)
	}
	if (*(*testCaseInput.status)) != 0 {
		w.WriteHeader((*(*testCaseInput.status)))
		return
	}
	w.Header().Set("Content-Length", "20")
	_, _ = io.WriteString(w, "partial")
}
