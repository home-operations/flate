package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func TestFetcher_OCIHelmRepositoryCredentials(t *testing.T) {
	var failing atomic.Bool
	testutil.StubDNS(t, func(context.Context, string) error {
		if failing.Load() {
			return os.ErrDeadlineExceeded
		}
		return nil
	})
	const user, pass = "alice", "hunter2"
	const chart = "name: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": chart})
	art.manifest = mustManifestJSON(t,
		manifestBlob{data: art.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: art.layer, mediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip"},
	)
	upstream := startFakeRegistry(t, art.manifest, art.config, art.layer)
	var failNext atomic.Bool
	var failures atomic.Int32
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if failNext.CompareAndSwap(true, false) {
			failures.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	targetURL := strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
	host := mustURL(t, targetURL).Host
	credentials := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, credentials)
	cache := cachetest.New(t, "", sourcecache.New)
	var network source.Network
	ociFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{".dockerconfigjson": configJSON}}
	}}
	ociFetcher := &Fetcher{
		Cache: cache, Network: &network,
	}
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "ns",
		URL: "oci://" + host + "/charts", Type: sourcev1.HelmRepositoryTypeOCI, Insecure: true,
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
		Interval:  metav1.Duration{Duration: time.Hour},
	}
	chartInput := manifest.NewHelmChartSource(
		sourcev1.HelmChart{
			Name:      "app",
			Namespace: repo.Namespace,
			Spec: sourcev1.HelmChartSpec{
				Chart:     "app",
				Version:   "1.0.0",
				Interval:  repo.Interval,
				SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repo.Name},
			},
		},
	)
	chartsInputs := testInputs{
		secrets: ociFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return repo },
	}
	charts := helmchart.New(ociFetcher, helmchart.Config{Cache: cache, Network: &network})

	got, err := charts.Fetch(t.Context(), chartInput, chartsInputs.readSet(chartInput), sourcecache.Policy{})
	assert.NoError(t, err)

	if got.Kind != manifest.KindHelmChart {
		t.Errorf("artifact Kind = %q, want %q", got.Kind, manifest.KindHelmChart)
	}
	{
		b := mustReadFile(t, got.FS, "Chart.yaml")
		assert.ErrorIf(t, b != chart, "Chart.yaml = %q, want %q", b, chart)
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	helmPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
	testutil.WriteFileAt(t, helmPath, configJSON)
	helmRepo := *repo
	helmRepo.SecretRef = nil
	helmInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return &helmRepo }}
	helmCharts := helmchart.New(
		&Fetcher{Cache: cache, Network: &network},
		helmchart.Config{Cache: cache, Network: &network},
	)
	helmArtifact, err := helmCharts.Fetch(t.Context(), chartInput, helmInputs.readSet(chartInput), sourcecache.Policy{})
	assert.NoError(t, err)
	assert.Equal(t, helmArtifact.Kind, manifest.KindHelmChart)
	assert.Equal(t, mustReadFile(t, helmArtifact.FS, "Chart.yaml"), chart)
	initialKey := got.Key
	configPath := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, configPath, configJSON)
	fallbackFetcherInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	fallbackFetcher := &Fetcher{Cache: cache, Network: &network, RegistryConfig: configPath}
	fallbackChartsInputs := testInputs{
		secrets: fallbackFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return repo },
	}
	fallbackCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})

	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	_, err = fallbackCharts.Fetch(
		t.Context(),
		chartInput,
		fallbackChartsInputs.readSet(chartInput),
		sourcecache.Policy{Retry: retry},
	)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing fallback: %v", err)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		_, retryAmbientErr := missing.RetryAmbient(cancelled)
		assert.FatalIf(t, !errors.Is(retryAmbientErr, context.Canceled), "fallback context: %v", retryAmbientErr)
	}
	failNext.Store(true)
	got, err = missing.RetryAmbient(t.Context())
	if err != nil || failures.Load() != 1 {
		t.Fatalf("fallback recovery: %v, failures=%d", err, failures.Load())
	}
	assert.FatalIf(
		t,
		got.Key == initialKey || got.Kind != manifest.KindHelmChart || mustReadFile(t, got.FS, "Chart.yaml") != chart,
		"fallback artifact=%+v",
		got,
	)
	beforeWarm := requests.Load()
	warm, err := missing.RetryAmbient(t.Context())
	if err != nil || warm == nil || warm.Key != got.Key || warm.Kind != manifest.KindHelmChart ||
		requests.Load() != beforeWarm {
		t.Fatalf(
			"fallback lost freshness or chart identity: artifact=%+v, err=%v, new requests=%d",
			warm,
			err,
			requests.Load()-beforeWarm,
		)
	}
	refreshRepo := *repo
	refreshRepo.Interval = metav1.Duration{}
	refreshInput := *chartInput
	refreshInput.Interval = refreshRepo.Interval
	refreshChartsInputs := testInputs{
		secrets: fallbackFetcherInputs.secrets,
		repos:   func(_, _ string) *manifest.HelmRepository { return &refreshRepo },
	}
	refreshCharts := helmchart.New(fallbackFetcher, helmchart.Config{Cache: cache, Network: &network})
	_, err = refreshCharts.Fetch(
		t.Context(),
		&refreshInput,
		refreshChartsInputs.readSet(&refreshInput),
		sourcecache.Policy{Retry: retry},
	)
	missing, ok = errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "refresh fallback: %v", err)
	failing.Store(true)
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)

	req, _ := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		strings.Replace(srv.URL, "127.0.0.1", "example.com", 1),
		http.NoBody,
	)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "seed: %v", err)
	cause := terminal.Err
	before := requests.Load()
	_, err = missing.RetryAmbient(t.Context())
	if terminal, ok := errors.AsType[*sourcecache.TerminalError](
		err,
	); !ok || terminal.Attempted || !errors.Is(err, cause) ||
		requests.Load() != before {
		t.Fatalf("suppressed fallback: %v, additional requests=%d", err, requests.Load()-before)
	}
}

func testFetcherHelmConfigAvailabilityCase(t *testing.T, mode *string,
) {
	t.Helper()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	home := t.TempDir()
	t.Setenv("HELM_CONFIG_HOME", home)
	t.Setenv("HELM_REGISTRY_CONFIG", "")
	path := filepath.Join(home, "registry", "config.json")
	wantUser := ""
	switch *mode {
	case "open failure":
		testutil.WriteFile(t, home, "registry", "regular file")
	case "read failure":
		assert.NoError(t, os.MkdirAll(path, 0o755))
	case "malformed":
		testutil.WriteFileAt(t, path, `{`)
	case "default", "override":
		wantUser = "default"
		testutil.WriteFileAt(
			t,
			path,
			`{"auths":{"https://index.docker.io/v1/":{"username":"default","password":"default"}}}`,
		)
		if (*mode) == "override" {
			wantUser = "override"
			path = filepath.Join(t.TempDir(), "config.json")
			t.Setenv("HELM_REGISTRY_CONFIG", path)
			testutil.WriteFileAt(
				t,
				path,
				`{"auths":{"https://index.docker.io/v1/":{"username":"override","password":"override"}}}`,
			)
		}
	}
	selected, err := newTestNetwork(t).RegistryCredentials(t.Context(), "", "registry-1.docker.io")
	if (*mode) == "malformed" || (*mode) == "open failure" ||
		(*mode) == "read failure" {
		assert.FatalIf(t, err == nil, "readable malformed Helm config = %v", err)
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, *selected, auth.Credential{Username: wantUser, Password: wantUser})
}
