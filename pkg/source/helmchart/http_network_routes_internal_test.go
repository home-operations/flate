package helmchart

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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

func testFetchHTTPChartNetworkRoutesCase(t *testing.T, route *string,
	authenticated *bool,
	customTLS *bool,
) {
	t.Helper()
	var network source.Network
	var indexHits, chartHits, dials atomic.Int32
	var cause error
	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			dials.Add(1)
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	var target string
	latch := func() error {
		failing.Store(true)
		tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
		if err != nil {
			return err
		}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
		_, err = (&http.Client{Transport: tr}).Do(req)
		failing.Store(false)
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		if !ok || !terminal.Attempted {
			return fmt.Errorf("seed: %w", err)
		}
		cause = terminal.Err
		return nil
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetchHTTPChartNetworkRoutesCaseCallback(
			w,
			r,
			testFetchHTTPChartNetworkRoutesCaseCallbackInputs{
				t:             &t,
				authenticated: &authenticated,
				indexHits:     &indexHits,
				chartHits:     &chartHits,
				latch:         &latch,
			},
		)
	},
	)
	var srv *httptest.Server
	if *customTLS {
		srv = httptest.NewTLSServer(handler)
	} else {
		srv = httptest.NewServer(handler)
	}
	t.Cleanup(srv.Close)
	target = strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	r := httpRepo(target)
	secret := &manifest.Secret{StringData: map[string]any{"username": "user", "password": "pass"}}
	if *authenticated {
		r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	}
	if *customTLS {
		r.CertSecretRef = &manifest.LocalObjectReference{Name: "auth"}
		secret.StringData["ca.crt"] = string(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}),
		)
	}
	c := cachetest.New(t, "", sourcecache.New)
	fInputs := testInputs{
		secrets: func(_, _ string) *manifest.Secret { return secret },
		repos:   func(_, _ string) *manifest.HelmRepository { return r },
	}
	f := New(nil, Config{Cache: c, Network: &network})

	if (*route) == "index" {
		{
			err := latch()
			assert.NoError(t, err)
		}
	}
	_, err := f.Fetch(
		t.Context(),
		helmChart("repo", "chart", "1.0.0"),
		fInputs.readSet(helmChart("repo", "chart", "1.0.0")),
		sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	wantIndex := int32(0)
	if (*route) == "chart" {
		wantIndex = 1
	}
	if !ok || terminal.Attempted || !errors.Is(err, cause) || dials.Load() == 0 ||
		indexHits.Load() != wantIndex ||
		chartHits.Load() != 0 {
		t.Fatalf(
			"error=%v terminal=%+v dials=%d index=%d chart=%d",
			err,
			terminal,
			dials.Load(),
			indexHits.Load(),
			chartHits.Load(),
		)
	}
}

type testFetchHTTPChartNetworkRoutesCaseCallbackInputs struct {
	t             **testing.T
	authenticated **bool
	indexHits     *atomic.Int32
	chartHits     *atomic.Int32
	latch         *func() error
}

func testFetchHTTPChartNetworkRoutesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetchHTTPChartNetworkRoutesCaseCallbackInputs,
) {
	if *(*testCaseInput.authenticated) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "user" || password != "pass" {
			(*testCaseInput.t).
				Error("missing repository auth")
		}
	}
	if r.URL.Path != "/index.yaml" {
		testCaseInput.chartHits.
			Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	testCaseInput.indexHits.
		Add(1)
	if err := (*testCaseInput.latch)(); err != nil {
		(*testCaseInput.t).
			Error(err)
	}
	_, _ = io.WriteString(
		w,
		"apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    apiVersion: v2\n    urls: [chart.tgz]\n",
	)
}
