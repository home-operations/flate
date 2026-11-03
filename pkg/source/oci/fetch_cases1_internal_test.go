package oci

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_Freshness_WarmConnections(t *testing.T) {
	t.Parallel()
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	srv, _ := newMutableFakeRegistry(t, artifact)
	var heads, manifests, layers, connections atomic.Int32
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherFreshnessWarmConnectionsCallback(
			w,
			r,
			testFetcherFreshnessWarmConnectionsCallbackInputs{
				heads:     &heads,
				manifests: &manifests,
				layers:    &layers,
				handler:   &handler,
			},
		)
	},
	)
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.StartTLS()
	root := t.TempDir()
	repo := &manifest.OCIRepository{
		Name: "first", Namespace: "test", URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	for _, invocation := range []string{"cold", "reopened"} {
		f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New), Network: newTestNetwork(t)}
		beforeConnections := connections.Load()
		for operation := range 2 {
			beforeHeads, beforeManifests, beforeLayers := heads.Load(), manifests.Load(), layers.Load()
			request := *repo
			request.Name = fmt.Sprintf("source-%d", operation)
			art, err := f.Fetch(
				t.Context(),
				&request,
				(testInputs{}).readSet(&request),
				sourcecache.Policy{MaxAge: request.SourceInterval()},
			)
			assert.NoError(t, err)

			{
				got := mustReadFile(t, art.FS, "app.yaml")
				assert.FatalIf(t, got != "version: v1\n", "content=%q", got)
			}
			h, m, l := heads.Load()-beforeHeads, manifests.Load()-beforeManifests, layers.Load()-beforeLayers
			t.Logf(
				"%s operation=%d: tag_HEAD=%d manifest_GET=%d layer_GET=%d accepted_connections=%d",
				invocation,
				operation,
				h,
				m,
				l,
				connections.Load()-beforeConnections,
			)
			wantLayers := int32(0)
			if invocation == "cold" && operation == 0 {
				wantLayers = 1
			}
			assert.ErrorIf(
				t,
				h != 1 || m != wantLayers || l != wantLayers,
				"%s operation=%d: HEAD/manifest GET/layer GET=%d/%d/%d, want 1/%d/%d",
				invocation,
				operation,
				h,
				m,
				l,
				wantLayers,
				wantLayers,
			)
		}
		{
			got := connections.Load() - beforeConnections
			assert.ErrorIf(
				t,
				got != 1,
				"%s: compatible sequential operations accepted %d connections, want 1",
				invocation,
				got,
			)
		}
		{
			err := f.Cache.Close()
			assert.NoError(t, err)
		}
	}
}

func startFakeRegistry(t *testing.T, manifestBytes, configBytes []byte, layers ...[]byte) *httptest.Server {
	t.Helper()
	blobs := map[string][]byte{sha256Digest(configBytes): configBytes}
	for _, layer := range layers {
		blobs[sha256Digest(layer)] = layer
	}
	manifestDigest := sha256Digest(manifestBytes)

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write(manifestBytes)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, digest, _ := strings.CutLast(r.URL.Path, "/")
			if blob, ok := blobs[digest]; ok {
				_, _ = w.Write(blob)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

type fakeOCIArtifact struct {
	manifest       []byte
	config         []byte
	layer          []byte
	manifestDigest string
	configDigest   string
	layerDigest    string
}

func newFakeOCIArtifact(t *testing.T, files map[string]string) fakeOCIArtifact {
	t.Helper()
	layer := testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)
	config := []byte(`{}`)
	manifest := mustManifestJSON(t,
		manifestBlob{data: config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layer, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)
	return fakeOCIArtifact{
		manifest:       manifest,
		config:         config,
		layer:          layer,
		manifestDigest: sha256Digest(manifest),
		configDigest:   sha256Digest(config),
		layerDigest:    sha256Digest(layer),
	}
}

func newMutableFakeRegistry(t *testing.T, artifacts ...fakeOCIArtifact) (*httptest.Server, func(int)) {
	t.Helper()
	assert.FatalIf(t, len(artifacts) == 0, "%s", "no OCI artifacts")

	var (
		mu      sync.RWMutex
		current int
		blobs   = map[string][]byte{}
	)
	for _, art := range artifacts {
		blobs[art.configDigest] = art.config
		blobs[art.layerDigest] = art.layer
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			mu.RLock()
			art := artifacts[current]
			mu.RUnlock()
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", art.manifestDigest)
			_, _ = w.Write(art.manifest)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, d, _ := strings.CutLast(r.URL.Path, "/")
			if body, ok := blobs[d]; ok {
				_, _ = w.Write(body)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewUnstartedServer(mux)
	t.Cleanup(srv.Close)
	setArtifact := func(i int) {
		mu.Lock()
		defer mu.Unlock()
		current = i
	}
	return srv, setArtifact
}

type manifestBlob struct {
	data      []byte
	mediaType string
}

func mustManifestJSON(t *testing.T, config manifestBlob, layers ...manifestBlob) []byte {
	t.Helper()
	m := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: config.mediaType,
			Digest:    digest.Digest(sha256Digest(config.data)),
			Size:      int64(len(config.data)),
		},
	}
	for _, layer := range layers {
		m.Layers = append(m.Layers, ocispec.Descriptor{
			MediaType: layer.mediaType,
			Digest:    digest.Digest(sha256Digest(layer.data)),
			Size:      int64(len(layer.data)),
		})
	}
	b, err := json.Marshal(m)
	assert.NoError(t, err)

	return b
}

func sha256Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	assert.NoError(t, err)

	return u
}

func mustReadFile(t *testing.T, input fs.FS, path string) string {
	t.Helper()
	b, err := fs.ReadFile(input, path)
	assert.NoError(t, err)

	return string(b)
}

//nolint:paralleltest // Subtests share their parent acquisition fixture and observe its state in order.
func TestFetcher_CacheIsolatesSources(t *testing.T) {
	t.Parallel()

	c := cachetest.New(t, t.TempDir(), sourcecache.New)
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	var keys []string
	for _, body := range []string{"first", "second"} {
		v := newFakeOCIArtifact(t, map[string]string{"app.yaml": body})
		srv := startFakeRegistry(t, v.manifest, v.config, v.layer)
		repo := &manifest.OCIRepository{
			URL:       "oci://" + mustURL(t, srv.URL).Host + "/app",
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Digest: v.manifestDigest},
		}
		art, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		assert.NoError(t, err)

		{
			got := mustReadFile(t, art.FS, "app.yaml")
			assert.FatalIf(t, got != body, "source collision: %q, want %q", got, body)
		}
		keys = append(keys, art.Key)
		if body != "first" {
			continue
		}
		request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), repo.RegistryAccess())
		assert.NoError(t, err)

		identities := map[string]string{art.Key: "original"}
		for _, tc := range []struct {
			name   string
			change func(*manifest.OCIRepository, *Fetcher)
		}{
			{"ref", func(r *manifest.OCIRepository, _ *Fetcher) { r.Reference = &sourcev1.OCIRepositoryRef{Tag: "latest"} }},
			{"ignore", func(r *manifest.OCIRepository, _ *Fetcher) { r.Ignore = new("*.tmp") }},
			{"insecure", func(r *manifest.OCIRepository, _ *Fetcher) { r.Insecure = false }},
			{"layer selector", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.LayerSelector = &manifest.OCILayerSelector{Operation: sourcev1.OCILayerExtract}
			}},
			{"registry config", func(_ *manifest.OCIRepository, f *Fetcher) { f.RegistryConfig = "other-config.json" }},
			{"secret", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			}},
			{"auth namespace", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				r.Namespace = "other"
			}},
			{"cert", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.CertSecretRef = &manifest.LocalObjectReference{Name: "cert"}
			}},
			{"proxy", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				testFetcherCacheIsolatesSourcesCase(
					t,
					testFetcherCacheIsolatesSourcesCaseInputs{
						c:          &c,
						body:       &body,
						repo:       &repo,
						request:    &request,
						identities: &identities,
						tc:         &tc,
					},
				)
			},
			)
		}
	}
	assert.FatalIf(t, keys[0] == keys[1], "%s", "distinct sources share their published identity")
}

func TestFetcher_LayerDigestMismatchPublishesNothing(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, corruption := range []string{"digest", "gzip header", "gzip trailer"} {
			t.Run(backing+"/"+corruption, func(t *testing.T) {
				t.Parallel()
				testFetcherLayerDigestMismatchPublishesNothingCase(
					t,

					&backing,
					&corruption,
				)
			},
			)
		}
	}
}

//nolint:paralleltest // Subtests share their parent acquisition fixture and observe its state in order.
func TestFetcher_FailureSourceAndOperation(t *testing.T) {
	t.Parallel()

	for _, layerOperation := range []string{sourcev1.OCILayerCopy, sourcev1.OCILayerExtract} {
		for _, tc := range []struct {
			stage    string
			status   int
			attempts int32
			replay   bool
		}{
			{"list tags", 400, 1, false},
			{"resolve", 400, 1, false},
			{"fetch manifest", 400, 1, false},
			{"fetch layer", 400, 1, false},
			{"manifest body", 400, 1, false},
			{"layer header", 400, 1, false},
			{"layer body", 400, 1, false},
			{"neutral body", 400, 1, false},
			{"canceled body", 400, 1, false},
			{"list tags", 429, 3, true},
			{"resolve", 500, 3, true},
			{"fetch manifest", 599, 3, true},
			{"fetch layer", 600, 1, false},
			{"resolve", 404, 1, false},
		} {
			stage := tc.stage
			if stage == "layer header" && layerOperation == sourcev1.OCILayerCopy {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s/%d", layerOperation, stage, tc.status), func(t *testing.T) {
				testFetcherFailureSourceAndOperationCase(
					t,

					&layerOperation,
					&tc,
					&stage,
				)
			},
			)
		}
	}
}

type registryRoundTripper func(*http.Request) (*http.Response, error)

func (rt registryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}

func TestFetcher_LayerSelectionAndIgnoreFlavor(t *testing.T) {
	t.Parallel()
	art := newFakeOCIArtifact(t, map[string]string{"file.yaml": "x", ".git/config": "git", "image.png": "image"})
	second := newFakeOCIArtifact(t, map[string]string{"file.yaml": "second"})
	archive := manifestBlob{data: art.layer, mediaType: registry.ChartLayerMediaType}
	other := manifestBlob{data: second.layer, mediaType: registry.ChartLayerMediaType}
	prov := manifestBlob{data: []byte("plain provenance"), mediaType: registry.ProvLayerMediaType}
	for _, tc := range []struct {
		name     string
		layers   []manifestBlob
		selector *sourcev1.OCILayerSelector
		digest   bool
		wantCopy []byte
		wantErr  bool
	}{
		{name: "defaults", layers: []manifestBlob{archive}},
		{name: "digest selectors", layers: []manifestBlob{archive}, digest: true},
		{name: "unmatched extract", layers: []manifestBlob{archive}, selector: &sourcev1.OCILayerSelector{MediaType: "unmatched/type"}, wantErr: true},
		{name: "unmatched copy", layers: []manifestBlob{archive}, selector: &sourcev1.OCILayerSelector{MediaType: "unmatched/type", Operation: sourcev1.OCILayerCopy}, wantErr: true},
		{name: "provenance only extract", layers: []manifestBlob{prov}, wantErr: true},
		{name: "provenance only copy", layers: []manifestBlob{prov}, selector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "provenance first extract", layers: []manifestBlob{prov, archive}, wantErr: true},
		{name: "provenance first copy", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "empty media extract", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{}, wantErr: true},
		{name: "empty media copy", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{MediaType: "", Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "first matching extract", layers: []manifestBlob{prov, archive, other}, selector: &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType}},
		{name: "first matching copy", layers: []manifestBlob{prov, archive, other}, selector: &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType, Operation: sourcev1.OCILayerCopy}, wantCopy: art.layer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherLayerSelectionAndIgnoreFlavorCase(
				t,
				testFetcherLayerSelectionAndIgnoreFlavorCaseInputs{art: &art, second: &second, prov: &prov, tc: &tc},
			)
		},
		)
	}
}

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"registry", "token", "redirect", "tags", "manifest", "layer", "copy", "fallback"} {
		t.Run(
			route,
			func(t *testing.T) {
				testFetcherNetworkRoutesCase(t, &route)
			},
		)
	}
}

func TestFetcher_Revalidation_SelectedTagMetadata(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{sourcev1.OCILayerExtract, sourcev1.OCILayerCopy} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testFetcherRevalidationSelectedTagMetadataCase(
				t,
				&operation,
			)
		},
		)
	}
}

func TestFetcher_ChartMediaSelector(t *testing.T) {
	t.Parallel()
	for _, media := range []string{registry.ChartLayerMediaType, "application/tar+gzip"} {
		t.Run(media, func(t *testing.T) {
			t.Parallel()
			art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: app\nversion: 1.0.0\n"})
			art.config = []byte(`{"name":"app","version":"1.0.0","apiVersion":"v2"}`)
			data := mustManifestJSON(t,
				manifestBlob{data: art.config, mediaType: registry.ConfigMediaType},
				manifestBlob{data: art.layer, mediaType: media},
			)
			srv := startFakeRegistry(t, data, art.config, art.layer)
			repo := &manifest.OCIRepository{
				URL:       "oci://" + mustURL(t, srv.URL).Host + "/charts/app",
				Insecure:  true,
				Reference: &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
			}
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
			assert.FatalIf(t, err != nil || got == nil, "default request: artifact=%v error=%v", got, err)
			{
				_, readFileErr := fs.ReadFile(got.FS, "Chart.yaml")
				assert.NoError(t, readFileErr)
			}

			explicit := *repo
			explicit.LayerSelector = &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType}
			got, err = f.Fetch(t.Context(), &explicit, (testInputs{}).readSet(&explicit), sourcecache.Policy{})
			if media == registry.ChartLayerMediaType {
				assert.FatalIf(t, err != nil || got == nil, "explicit chart selector: artifact=%v error=%v", got, err)
			} else {
				assert.FatalIf(
					t,
					got != nil || !errors.Is(err, manifest.ErrFlux),
					"explicit chart selector must reject a different media type: artifact=%v error=%v",
					got,
					err,
				)
			}
		})
	}
}

func TestFetcher_ChartMediaAfterProvenance(t *testing.T) {
	t.Parallel()
	for _, media := range []string{registry.ChartLayerMediaType, "application/tar+gzip"} {
		t.Run(media, func(t *testing.T) {
			t.Parallel()
			testFetcherChartMediaAfterProvenanceCase(t, &media)
		},
		)
	}
}

func TestFetcher_ChartMediaCacheIsolation(t *testing.T) {
	t.Parallel()
	generic := newFakeOCIArtifact(t, map[string]string{"file.yaml": "generic"})
	chart := newFakeOCIArtifact(t, map[string]string{"file.yaml": "chart"})
	data := mustManifestJSON(
		t,
		manifestBlob{data: generic.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: generic.layer, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
		manifestBlob{data: chart.layer, mediaType: registry.ChartLayerMediaType},
	)
	srv := startFakeRegistry(t, data, generic.config, generic.layer, chart.layer)
	repo := &manifest.OCIRepository{
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/artifact",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	gotGeneric, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.NoError(t, err)

	gotChart, err := f.FetchChart(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.NoError(t, err)

	assert.Equal(t, mustReadFile(t, gotGeneric.FS, "file.yaml"), "generic")
	assert.Equal(t, mustReadFile(t, gotChart.FS, "file.yaml"), "chart")
}

func TestFetcher_EmptyArtifact(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{sourcev1.OCILayerCopy, sourcev1.OCILayerExtract} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testFetcherEmptyArtifactCase(t, &operation)
		},
		)
	}
}

func TestFetcher_SourceTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "default"},
		{name: "shorter", timeout: 10 * time.Second},
		{name: "longer", timeout: 180 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherSourceTimeoutCase(t, &tc)
		},
		)
	}
}

type pullTimeoutBody struct{ ctx context.Context } //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

func (b pullTimeoutBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (pullTimeoutBody) Close() error { return nil }

func TestFetcher_PullTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		stage  string
		reopen bool
	}{{"tags", false}, {"manifest", false}, {"layer", true}} {
		t.Run(tc.stage, func(t *testing.T) {
			t.Parallel()
			testFetcherPullTimeoutCase(t, &tc)
		},
		)
	}
}

type testFetcherFreshnessWarmConnectionsCallbackInputs struct {
	heads     *atomic.Int32
	manifests *atomic.Int32
	layers    *atomic.Int32
	handler   *http.Handler
}

func testFetcherFreshnessWarmConnectionsCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherFreshnessWarmConnectionsCallbackInputs,
) {
	switch {
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
