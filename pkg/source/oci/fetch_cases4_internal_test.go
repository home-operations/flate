package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func testFetcherNetworkRoutesCase(t *testing.T, route *string,
) {
	t.Helper()
	var network source.Network
	var bypass, requests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		bypass.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(target.Close)
	testutil.StubDNS(t, func(context.Context, string) error { return os.ErrDeadlineExceeded })
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
	assert.NoError(t, err)

	req, _ := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		strings.Replace(target.URL, "127.0.0.1", "example.com", 1),
		nil,
	)
	_, err = (&http.Client{Transport: tr}).Do(req)
	first, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !first.Attempted, "seed: %v", err)
	cause := first.Err
	art := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
	registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherNetworkRoutesCaseCallback(
			w,
			r,
			testFetcherNetworkRoutesCaseCallbackInputs{route: &route, requests: &requests, target: &target, art: &art},
		)
	},
	))
	t.Cleanup(registry.Close)
	registryURL := registry.URL
	if (*route) == "registry" || (*route) == "fallback" {
		registryURL = strings.Replace(target.URL, "127.0.0.1", "example.com", 1)
	}
	repo := &manifest.OCIRepository{
		Name:      "network",
		Namespace: "test",
		URL:       "oci://" + mustURL(t, registryURL).Host + "/test/chart",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
	}
	if (*route) == "tags" {
		repo.Reference = &sourcev1.OCIRepositoryRef{SemVer: ">=1"}
	}
	if (*route) == "copy" {
		repo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	}
	fInputs := testInputs{}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: &network}
	retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
	if (*route) == "fallback" {
		repo.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
		fInputs.secrets = func(_, _ string) *manifest.Secret { return nil }
		f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
		testutil.WriteFileAt(
			t,
			f.RegistryConfig,
			fmt.Sprintf(`{"auths":{%q:{"auth":"dXNlcjpwYXNz"}}}`, mustURL(t, registryURL).Host),
		)
		_, err = f.Fetch(
			t.Context(),
			repo,
			fInputs.readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
		)
		missing, innerOk := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !innerOk || missing.RetryAmbient == nil, "fallback: %v", err)
		_, err = missing.RetryAmbient(t.Context())
	} else {
		_, err = f.Fetch(
			t.Context(),
			repo,
			fInputs.readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: retry},
		)
	}
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	if !ok || terminal.Attempted || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) ||
		bypass.Load() != 0 {
		t.Fatalf("route=%s error=%v terminal=%+v bypass=%d", (*route), err, terminal, bypass.Load())
	}
	want := int32(1)
	if (*route) == "layer" || (*route) == "copy" {
		want = 3
	}
	if (*route) == "manifest" {
		want = 2
	}
	if (*route) == "registry" || (*route) == "fallback" {
		want = 0
	}
	{
		got := requests.Load()
		assert.FatalIf(t, got != want, "registry requests=%d want=%d", got, want)
	}
}

func testFetcherRevalidationSelectedTagMetadataCase(
	t *testing.T, operation *string,
) {
	t.Helper()

	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	var m ocispec.Manifest
	assert.NoError(t, json.Unmarshal(artifact.manifest, &m))
	m.Annotations = map[string]string{"content.annotation": "retained"}
	var err error
	artifact.manifest, err = json.Marshal(m)
	assert.NoError(t, err)
	artifact.manifestDigest = sha256Digest(artifact.manifest)
	srv, _ := newMutableFakeRegistry(t, artifact)
	var newer atomic.Bool
	var tags, heads, manifests, layers atomic.Int32
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherRevalidationSelectedTagMetadataCaseCallback(
			w,
			r,
			testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs{
				newer:     &newer,
				tags:      &tags,
				heads:     &heads,
				manifests: &manifests,
				layers:    &layers,
				handler:   &handler,
			},
		)
	},
	)
	srv.StartTLS()
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name: "metadata", Namespace: "test",
		URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
		Reference:     &sourcev1.OCIRepositoryRef{SemVer: ">=1.0.0"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	first, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.NoError(t, err)
	firstMetadata := map[string]string{
		"content.annotation": "retained",
		"flate.revision":     "v1.0.0@" + artifact.manifestDigest,
		"flate.resolved-tag": "v1.0.0",
	}
	assert.Diff(t, first.Metadata, firstMetadata)
	newer.Store(true)
	for _, phase := range []string{"newer tag", "reopened"} {
		if phase == "reopened" {
			assert.NoError(t, f.Cache.Close())
			f.Cache = cachetest.New(t, root, sourcecache.New)
		}
		beforeTags, beforeHeads, beforeManifests, beforeLayers := tags.Load(), heads.Load(), manifests.Load(), layers.Load()
		got, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		assert.NoError(t, err)
		if tags.Load()-beforeTags != 1 || heads.Load()-beforeHeads != 1 ||
			manifests.Load() != beforeManifests ||
			layers.Load() != beforeLayers {
			t.Fatalf(
				"%s: tag lists/HEAD/manifest GET/layer GET=%d/%d/%d/%d",
				phase,
				tags.Load()-beforeTags,
				heads.Load()-beforeHeads,
				manifests.Load()-beforeManifests,
				layers.Load()-beforeLayers,
			)
		}
		assert.Equal(t, got.Key, first.Key)
		assert.Equal(t, sourcecache.AcquisitionIdentity(got.FS), sourcecache.AcquisitionIdentity(first.FS))
		assert.FatalIf(t, first.Content == "", "%s", "acquired content has no generation")

		assert.Equal(t, first.Content, sourcecache.AcquisitionIdentity(first.FS))
		assert.Equal(t, got.Content, first.Content)
		assert.Equal(t, got.Digest, first.Digest)
		assert.Equal(t, got.Size, first.Size)
		assert.Equal(t, got.Revision, "v1.1.0@"+artifact.manifestDigest)
		assert.Diff(
			t,
			got.Metadata,
			map[string]string{
				"content.annotation": "retained",
				"flate.revision":     got.Revision,
				"flate.resolved-tag": "v1.1.0",
			},
		)
		assert.Diff(t, first.Metadata, firstMetadata)
		if (*operation) == sourcev1.OCILayerExtract {
			assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "version: v1\n")
		} else {
			body, err := fs.ReadFile(got.FS, source.OCICopiedLayerFilename)
			assert.NoError(t, err)
			assert.Diff(t, body, artifact.layer)
		}
	}
}

func testFetcherChartMediaAfterProvenanceCase(
	t *testing.T, media *string,
) {
	t.Helper()

	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: app\nversion: 1.0.0\n"})
	art.config = []byte(`{"name":"app","version":"1.0.0","apiVersion":"v2"}`)
	data := mustManifestJSON(
		t,
		manifestBlob{data: art.config, mediaType: registry.ConfigMediaType},
		manifestBlob{data: art.layer, mediaType: (*media)},
	)
	prov := []byte("plain provenance")
	var m ocispec.Manifest
	{
		err := json.Unmarshal(data, &m)
		assert.NoError(t, err)
	}

	m.Layers = append(
		[]ocispec.Descriptor{
			{MediaType: registry.ProvLayerMediaType, Digest: digest.FromBytes(prov), Size: int64(len(prov))},
		},
		m.Layers...)
	data, err := json.Marshal(m)
	assert.NoError(t, err)

	upstream := startFakeRegistry(t, data, art.config, art.layer)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/blobs/"+digest.FromBytes(prov).String()) {
			_, _ = w.Write(prov)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "oci://" + mustURL(t, srv.URL).Host
	repo := &manifest.OCIRepository{
		Name:      "app",
		Namespace: "ns",
		URL:       url + "/charts/app",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
	}
	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	network := newTestNetwork(t)
	f := &Fetcher{Network: network, Cache: cache}
	got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.ErrorIf(
		t,
		got != nil || !errors.Is(err, manifest.ErrFlux),
		"default OCI extract must fail on first provenance layer: artifact=%v error=%v",
		got,
		err,
	)
	copyRepo := *repo
	copyRepo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	got, err = f.Fetch(t.Context(), &copyRepo, (testInputs{}).readSet(&copyRepo), sourcecache.Policy{})
	assert.NoError(t, err)

	data, err = fs.ReadFile(got.FS, source.OCICopiedLayerFilename)
	assert.ErrorIf(
		t,
		err != nil || !bytes.Equal(data, prov),
		"default OCI copy=%q error=%v; want exact provenance bytes",
		data,
		err,
	)
	r := &manifest.HelmRepository{
		Name:      "charts",
		Namespace: "ns",
		URL:       url + "/charts",
		Type:      sourcev1.HelmRepositoryTypeOCI,
		Insecure:  true,
	}
	hc := manifest.NewHelmChartSource(
		sourcev1.HelmChart{
			Name:      "app",
			Namespace: r.Namespace,
			Spec: sourcev1.HelmChartSpec{
				Chart:   "app",
				Version: "1.0.0",
				SourceRef: sourcev1.LocalHelmChartSourceReference{
					Kind: manifest.KindHelmRepository,
					Name: r.Name,
				},
			},
		},
	)
	reads := (testInputs{repos: func(string, string) *manifest.HelmRepository { return r }}).readSet(hc)
	charts := helmchart.New(f, helmchart.Config{Cache: cache, Network: network})
	chart, err := charts.Fetch(t.Context(), hc, reads, sourcecache.Policy{})
	assert.FatalIf(
		t,
		err != nil || chart == nil,
		"chart acquisition after same-spec OCI failure: artifact=%v error=%v",
		chart,
		err,
	)
	{
		got := mustReadFile(t, chart.FS, "Chart.yaml")
		assert.FatalIf(t, got != "apiVersion: v2\nname: app\nversion: 1.0.0\n", "chart content=%q", got)
	}
	if chart.Kind != manifest.KindHelmChart {
		t.Fatalf("chart kind=%s", chart.Kind)
	}
}

func testFetcherEmptyArtifactCase(t *testing.T, operation *string,
) {
	t.Helper()

	art := newFakeOCIArtifact(t, map[string]string{"file": "x"})
	var m ocispec.Manifest
	{
		err := json.Unmarshal(art.manifest, &m)
		assert.NoError(t, err)
	}

	m.Layers = nil
	data, err := json.Marshal(m)
	assert.NoError(t, err)

	srv := startFakeRegistry(t, data, art.config, art.layer)
	repo := &manifest.OCIRepository{
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	for _, media := range []string{"", registry.ChartLayerMediaType} {
		t.Run("media="+media, func(t *testing.T) {
			t.Parallel()
			selected := *repo
			selected.LayerSelector = &sourcev1.OCILayerSelector{Operation: (*operation), MediaType: media}
			for range 2 {
				got, err := f.Fetch(
					t.Context(),
					&selected,
					(testInputs{}).readSet(&selected),
					sourcecache.Policy{},
				)
				assert.FatalIf(
					t,
					got != nil || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, manifest.ErrInput),
					"empty %s: artifact=%v error=%v; want nil artifact, ErrInput and ErrFlux",
					(*operation),
					got,
					err,
				)
			}
		})
	}
}

func testFetcherSourceTimeoutCase(t *testing.T, tc *struct {
	name    string
	timeout time.Duration
},
) {
	t.Helper()

	want := tc.timeout
	if want == 0 {
		want = time.Minute
	}
	r := &manifest.OCIRepository{
		URL:       "oci://registry.invalid/repository",
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	if tc.timeout != 0 {
		r.Timeout = &metav1.Duration{Duration: tc.timeout}
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", sourcecache.New)}
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), r.RegistryAccess())
	assert.NoError(t, err)

	stop := errors.New("controlled registry request")
	var calls atomic.Int32
	request.client.Client = &http.Client{
		Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			deadline, ok := req.Context().Deadline()
			remaining := time.Until(deadline)
			assert.ErrorIf(
				t,
				!ok || remaining <= want-time.Second || remaining > want,
				"registry request deadline: present=%t remaining=%v, want %v",
				ok,
				remaining,
				want,
			)
			return nil, stop
		}),
	}
	_, err = (&operation{Fetcher: f}).fetchLayer(t.Context(), r, request, sourcecache.Policy{})
	if calls.Load() == 0 || !errors.Is(err, stop) {
		t.Fatalf("controlled registry request not reached: calls=%d error=%v", calls.Load(), err)
	}
}

func testFetcherPullTimeoutCase(t *testing.T, tc *struct {
	stage  string
	reopen bool
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testFetcherPullTimeoutCaseCallback(t, testFetcherPullTimeoutCaseCallbackInputs{tc: &tc})
	},
	)
}

type testFetcherNetworkRoutesCaseCallbackInputs struct {
	route    **string
	requests *atomic.Int32
	target   **httptest.Server
	art      *fakeOCIArtifact
}

func testFetcherNetworkRoutesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherNetworkRoutesCaseCallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	switch {
	case (*(*testCaseInput.route)) == "token":
		w.Header().
			Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="registry"`, strings.Replace((*testCaseInput.target).URL, "127.0.0.1", "example.com", 1)))
		w.WriteHeader(http.StatusUnauthorized)
	case (*(*testCaseInput.route)) == "redirect", (*(*testCaseInput.route)) ==
		"tags" && strings.HasSuffix(r.URL.Path, "/tags/list"), (*(*testCaseInput.route)) ==
		"manifest" && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/"), strings.Contains(r.URL.Path, "/blobs/"):
		http.Redirect(
			w,
			r,
			strings.Replace((*testCaseInput.target).URL, "127.0.0.1", "example.com", 1),
			http.StatusTemporaryRedirect,
		)
	default:
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", testCaseInput.art.manifestDigest)
		w.Header().Set("Content-Length", strconv.Itoa(len(testCaseInput.art.manifest)))
		if r.Method != http.MethodHead {
			_, _ = w.Write(testCaseInput.art.manifest)
		}
	}
}

type testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs struct {
	newer     *atomic.Bool
	tags      *atomic.Int32
	heads     *atomic.Int32
	manifests *atomic.Int32
	layers    *atomic.Int32
	handler   *http.Handler
}

func testFetcherRevalidationSelectedTagMetadataCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs,
) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		testCaseInput.tags.
			Add(1)
		available := []string{"v1.0.0"}
		if testCaseInput.newer.Load() {
			available = append(available, "v1.1.0")
		}
		w.Header().Set("Content-Type", "application/json")
		//nolint:errchkjson // Fixture payloads are encodable; client cancellation may fail their response writes.
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "app", "tags": available})
		return
	case strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead:
		testCaseInput.heads.
			Add(1)
	case strings.Contains(r.URL.Path, "/manifests/"):
		testCaseInput.manifests.
			Add(1)
	case strings.Contains(r.URL.Path, "/blobs/"):
		testCaseInput.layers.
			Add(1)
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}

type testFetcherPullTimeoutCaseCallbackInputs struct {
	tc **struct {
		stage  string
		reopen bool
	}
}

func testFetcherPullTimeoutCaseCallback(t *testing.T, testCaseInput testFetcherPullTimeoutCaseCallbackInputs) {
	t.Helper()
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	r := &manifest.OCIRepository{
		URL:       "oci://registry.invalid/repository",
		Reference: &sourcev1.OCIRepositoryRef{SemVer: "*"},
		Timeout:   &metav1.Duration{Duration: 5 * time.Second},
	}
	request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), r.RegistryAccess())
	assert.NoError(t, err)

	blocked := true
	start := time.Now()
	deadline := start.Add(5 * time.Second)
	calls := 0
	request.client.Client = &http.Client{
		Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
			calls++
			{
				got, ok := req.Context().Deadline()
				assert.ErrorIf(
					t,
					!ok || !got.Equal(deadline),
					"pull deadline=%v present=%t, want %v",
					got,
					ok,
					deadline,
				)
			}
			stage := "manifest"
			data := artifact.manifest
			header := http.Header{
				"Content-Type":          {ocispec.MediaTypeImageManifest},
				"Docker-Content-Digest": {artifact.manifestDigest},
			}
			switch {
			case strings.HasSuffix(req.URL.Path, "/tags/list"):
				stage = "tags"
				data = []byte(`{"name":"repository","tags":["v1.0.0"]}`)
				header.Set("Content-Type", "application/json")
			case strings.HasSuffix(req.URL.Path, artifact.layerDigest):
				stage = "layer"
				data = artifact.layer
				header.Set("Content-Type", "application/vnd.cncf.flux.content.v1.tar+gzip")
				header.Set("Docker-Content-Digest", artifact.layerDigest)
			}
			if stage == "tags" || stage == "layer" && !blocked {
				select {
				case <-time.After(3 * time.Second):
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
			body := io.NopCloser(bytes.NewReader(data))
			if req.Method == http.MethodHead {
				body = http.NoBody
			} else if blocked && stage == (*testCaseInput.tc).stage {
				body = pullTimeoutBody{req.Context()}
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        header,
				ContentLength: int64(len(data)),
				Body:          body,
				Request:       req,
			}, nil
		}),
	}
	testFetcherPullTimeoutCaseCallbackCompletion(
		t,
		&testFetcherPullTimeoutCaseCallbackCompletionInputs{
			err:           &err,
			f:             &f,
			r:             &r,
			request:       &request,
			start:         &start,
			testCaseInput: &testCaseInput,
			root:          &root,
			blocked:       &blocked,
			deadline:      &deadline,
			calls:         &calls,
		},
	)
}

func testFetcherPullTimeoutCaseCallbackCompletion(
	t *testing.T,
	captures *testFetcherPullTimeoutCaseCallbackCompletionInputs,
) {
	t.Helper()
	_, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(), (*captures.r), (*captures.request), sourcecache.Policy{MaxAge: time.Hour},
	)
	if !errors.Is((*captures.err), context.DeadlineExceeded) || time.Since((*captures.start)) != 5*time.Second {
		t.Fatalf(
			"%s timeout: elapsed=%v error=%v",
			(*captures.testCaseInput.tc).stage,
			time.Since((*captures.start)),
			(*captures.err),
		)
	}
	if (*captures.testCaseInput.tc).reopen {
		{
			closeErr5 := (*captures.f).Cache.Close()
			assert.NoError(t, closeErr5)
		}
		(*captures.f) = &Fetcher{
			Network: (*captures.f).Network,
			Cache:   cachetest.New(t, (*captures.root), sourcecache.New),
		}
	}
	(*captures.blocked) = false
	longer := *(*captures.r)
	longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
	(*captures.start) = time.Now()
	(*captures.deadline) = captures.start.Add(10 * time.Second)
	var acquired *store.SourceArtifact

	acquired, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(),
		&longer, (*captures.request), sourcecache.Policy{MaxAge: time.Hour},
	)
	if (*captures.err) != nil || time.Since((*captures.start)) != 6*time.Second {
		t.Fatalf("longer pull: elapsed=%v error=%v", time.Since((*captures.start)), (*captures.err))
	}
	{
		body, readFileErr2 := fs.ReadFile(acquired.FS, "app.yaml")
		assert.FatalIf(
			t,
			readFileErr2 != nil || string(body) != "version: v1\n",
			"pull content=%q error=%v",
			body,
			readFileErr2,
		)
	}
	{
		closeErr6 := (*captures.f).Cache.Close()
		assert.NoError(t, closeErr6)
	}
	(*captures.f) = &Fetcher{Network: (*captures.f).Network, Cache: cachetest.New(t, (*captures.root), sourcecache.New)}
	shorter := longer
	shorter.Timeout = &metav1.Duration{Duration: time.Nanosecond}
	before := (*captures.calls)
	var warm *store.SourceArtifact

	warm, (*captures.err) = (&operation{Fetcher: (*captures.f)}).fetchLayer(
		t.Context(),
		&shorter, (*captures.request), sourcecache.Policy{MaxAge: time.Hour},
	)
	assert.FatalIf(
		t, (*captures.err) !=
			nil || (*captures.calls) != before || warm.Key != acquired.Key, "timeout changed persistent content identity: calls=%d->%d error=%v",
		before, (*captures.calls), (*captures.err),
	)
}

type testFetcherPullTimeoutCaseCallbackCompletionInputs struct {
	err           *error
	f             **Fetcher
	r             **manifest.OCIRepository
	request       *registryAcquisition
	start         *time.Time
	testCaseInput *testFetcherPullTimeoutCaseCallbackInputs
	root          *string
	blocked       *bool
	deadline      *time.Time
	calls         *int
}
