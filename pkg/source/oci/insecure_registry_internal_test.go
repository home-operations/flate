package oci

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/registry"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherInsecureRegistryErrorsCase(t *testing.T, mode *string,
) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var connections, plain atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	srv.Listener = registryListener{Listener: srv.Listener, firstRead: func(p []byte) {
		if p[0] != 0x16 {
			plain.Add(1)
		}
		if (*mode) == "cancel mismatch" || (*mode) == "stalled HTTPS" {
			close(entered)
			<-release
		}
	}}
	srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
			if (*mode) == "reset" {
				_ = c.Close()
			}
		}
	}
	if (*mode) == "TLS" {
		srv.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return nil, errors.New("TLS handshake refused")
		}}
	}
	if (*mode) == "cancel mismatch" {
		srv.Start()
	} else {
		srv.StartTLS()
	}
	t.Cleanup(srv.Close)
	repo := &manifest.OCIRepository{
		Name:      "failure",
		Namespace: "ns",
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
	result := make(chan error, 1)
	go func() {
		got, err := f.Fetch(ctx, repo, (testInputs{}).readSet(repo), &sourcecache.Policy{})
		if got != nil {
			t.Error("failed acquisition returned an artifact")
		}
		result <- err
	}()
	testFetcherInsecureRegistryErrorsCaseCompletion(
		t,
		&testFetcherInsecureRegistryErrorsCaseCompletionInputs{
			mode:        &mode,
			entered:     &entered,
			cancel:      &cancel,
			unblock:     &unblock,
			release:     &release,
			result:      &result,
			plain:       &plain,
			connections: &connections,
		},
	)
}

func testFetcherInsecureRegistryErrorsCaseCompletion(
	t *testing.T,
	captures *testFetcherInsecureRegistryErrorsCaseCompletionInputs,
) {
	t.Helper()
	if (*(*captures.mode)) == "cancel mismatch" || (*(*captures.mode)) == "stalled HTTPS" {
		<-(*captures.entered)
		(*captures.cancel)()
		captures.unblock.
			Do(func() { close((*captures.release)) })
	}
	err := <-(*captures.result)
	if err == nil || captures.plain.Load() != 0 || captures.connections.Load() != 1 {
		t.Fatalf("error=%v HTTP attempts=%d connections=%d", err, captures.plain.Load(), captures.connections.Load())
	}
	switch *(*captures.mode) {
	case "response":
		response, ok := errors.AsType[*errcode.ErrorResponse](err)
		assert.FatalIf(t, !ok || response.StatusCode != http.StatusBadRequest, "lost response error: %v", err)
	case "cancel mismatch", "stalled HTTPS":
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "lost cancellation: %v", err)
	case "TLS":

		_, ok := errors.AsType[*net.OpError](err)
		assert.FatalIf(t, !ok || errors.Is(err, http.ErrSchemeMismatch), "lost TLS failure: %v", err)

	case "reset":

		_, ok := errors.AsType[*url.Error](err)
		assert.FatalIf(t, !ok || errors.Is(err, http.ErrSchemeMismatch), "lost reset failure: %v", err)
	}
}

type testFetcherInsecureRegistryErrorsCaseCompletionInputs struct {
	mode        **string
	entered     *chan struct{}
	cancel      *context.CancelFunc
	unblock     *sync.Once
	release     *chan struct{}
	result      *chan error
	plain       *atomic.Int32
	connections *atomic.Int32
}

func testFetcherInsecureRegistryCase(t *testing.T, chart *bool) {
	t.Helper()
	const body = "apiVersion: v2\nname: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": body})
	art.manifest = mustManifestJSON(
		t,
		manifestBlob{
			data:      art.config,
			mediaType: registry.ConfigMediaType,
		},
		manifestBlob{
			data:      art.layer,
			mediaType: registry.ChartLayerMediaType,
		},
	)
	art.manifestDigest = sha256Digest(art.manifest)
	newMutableFakeRegistryResult := newMutableFakeRegistry(t, art)
	srv := newMutableFakeRegistryResult.server
	var tags, heads, manifests, layers atomic.Int32
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherInsecureRegistryResponse(
			w,
			r,
			&testFetcherInsecureRegistryResponseInputs{
				handler:   &handler,
				heads:     &heads,
				layers:    &layers,
				manifests: &manifests,
				t:         &t,
				tags:      &tags,
			},
		)
	})
	srv.Start()
	fetch := func(
		ctx context.Context,
		f *Fetcher,
		insecure bool,
		accounting *manifest.AcquisitionWork,
	) (*store.SourceArtifact, error) {
		return testFetcherInsecureRegistryFetch(
			ctx,
			f,
			accounting,
			&testFetcherInsecureRegistryFetchInputs{chart: &chart, insecure: &insecure, srv: &srv, t: &t},
		)
	}
	key := manifest.WorkKey{
		Tree:   manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead},
		Owner:  testutil.HelmReleaseID("ns", "consumer"),
		Source: manifest.BootstrapSourceID,
	}
	assertWork := func(work *manifest.WorkRecorder, input string, want map[string]manifest.WorkCounts) {
		assertInsecureRegistryWork(work, input, want, &assertInsecureRegistryWorkInputs{key: &key, t: &t})
	}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
	failedWork := manifest.WorkRecorder{Key: key}
	failed, err := fetch(t.Context(), f, false, &manifest.AcquisitionWork{Scope: failedWork.Scope()})
	if failed != nil || !errors.Is(err, http.ErrSchemeMismatch) || tags.Load() != 0 {
		t.Fatalf("secure default: artifact=%v error=%v tags=%d", failed, err, tags.Load())
	}
	assertWork(&failedWork, "", map[string]manifest.WorkCounts{
		string(manifest.WorkAcquisition): {Attempts: 1, Failures: 1},
		string(manifest.WorkCache):       {Attempts: 1, Failures: 1},
		string(manifest.WorkPreparation): {RawKeyPrepares: 1},
		string(manifest.WorkValidation):  {Attempts: 1, Failures: 1},
	})
	var first *store.SourceArtifact
	for range 2 {
		f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
		beforeTags, beforeHeads, beforeManifests, beforeLayers := tags.Load(), heads.Load(), manifests.Load(), layers.Load()
		coldWork := manifest.WorkRecorder{Key: key}
		got, err := fetch(t.Context(), f, true, &manifest.AcquisitionWork{Scope: coldWork.Scope()})
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || mustReadFile(t, got.FS, "Chart.yaml") != body {
			t.Fatalf("artifact content=%v", got)
		}
		wantKind := manifest.KindOCIRepository
		if *chart {
			wantKind = manifest.KindHelmChart
		}
		if got.Kind != wantKind ||
			tags.Load()-beforeTags != 1 ||
			heads.Load()-beforeHeads != 1 ||
			manifests.Load()-beforeManifests != 1 ||
			layers.Load()-beforeLayers != 1 {
			t.Fatalf(
				"kind=%s; tags/HEAD/manifest/layer=%d/%d/%d/%d",
				got.Kind,
				tags.Load()-beforeTags,
				heads.Load()-beforeHeads,
				manifests.Load()-beforeManifests,
				layers.Load()-beforeLayers,
			)
		}
		if first != nil &&
			(got.Key != first.Key ||
				got.Content != first.Content ||
				got.Digest != first.Digest ||
				got.Revision != first.Revision) {
			t.Fatalf("independent acquisition identity changed: first=%+v second=%+v", first, got)
		}
		assertWork(&coldWork, got.Key, map[string]manifest.WorkCounts{
			string(manifest.WorkAcquisition): {Attempts: 2, Completed: 1, Failures: 1, RemoteTransfers: 1},
			string(manifest.WorkCache):       {Attempts: 2, Completed: 1, Failures: 1},
			string(manifest.WorkPreparation): {RawKeyPrepares: 2},
			string(manifest.WorkValidation):  {Attempts: 4, Completed: 3, Failures: 1},
		})
		first = got
		warmWork := manifest.WorkRecorder{Key: key}
		warm, err := fetch(t.Context(), f, true, &manifest.AcquisitionWork{Scope: warmWork.Scope()})
		if err != nil ||
			warm.Key != got.Key ||
			tags.Load()-beforeTags != 1 ||
			heads.Load()-beforeHeads != 1 ||
			manifests.Load()-beforeManifests != 1 ||
			layers.Load()-beforeLayers != 1 {
			t.Fatalf("warm acquisition requested content: artifact=%v error=%v", warm, err)
		}
		assertWork(&warmWork, got.Key, map[string]manifest.WorkCounts{
			string(manifest.WorkCache):       {Attempts: 1, Completed: 1, CacheService: 1},
			string(manifest.WorkPreparation): {RawKeyPrepares: 1},
		})
	}
}

type testFetcherInsecureRegistryFetchInputs struct {
	chart    **bool
	insecure *bool
	srv      **httptest.Server
	t        **testing.T
}

func testFetcherInsecureRegistryFetch(
	ctx context.Context,
	f *Fetcher,
	accounting *manifest.AcquisitionWork,
	captures *testFetcherInsecureRegistryFetchInputs,
) (*store.SourceArtifact, error) {
	if !(*(*captures.chart)) {
		repo := &manifest.OCIRepository{
			Name:      "app",
			Namespace: "ns",
			URL: "oci://" + mustURL(
				(*captures.t),
				(*captures.srv).URL,
			).Host + "/charts/app",
			Insecure: *captures.insecure,
			Reference: &sourcev1.OCIRepositoryRef{
				SemVer: "1.x",
			},
		}
		return f.Fetch(
			ctx,
			repo,
			(testInputs{}).readSet(repo),
			&sourcecache.Policy{MaxAge: time.Hour, Work: accounting},
		)
	}
	repo := &manifest.HelmRepository{
		Name:      "charts",
		Namespace: "ns",
		URL: "oci://" + mustURL(
			(*captures.t),
			(*captures.srv).URL,
		).Host + "/charts",
		Type:     sourcev1.HelmRepositoryTypeOCI,
		Insecure: *captures.insecure,
		Interval: metav1.Duration{
			Duration: time.Hour,
		},
	}
	hc := manifest.NewHelmChartSource(
		sourcev1.HelmChart{
			Name:      "app",
			Namespace: "ns",
			Spec: sourcev1.HelmChartSpec{
				Chart:    "app",
				Version:  "1.x",
				Interval: repo.Interval,
				SourceRef: sourcev1.LocalHelmChartSourceReference{
					Kind: manifest.KindHelmRepository,
					Name: repo.Name,
				},
			},
		},
	)
	reads := (testInputs{repos: func(string, string) *manifest.HelmRepository { return repo }}).readSet(
		hc,
	)
	return helmchart.New(
		f,
		helmchart.Config{
			Cache:   f.Cache,
			Network: f.Network,
		},
	).Fetch(
		ctx,
		hc,
		reads,
		&sourcecache.Policy{
			MaxAge: time.Hour,
			Work:   accounting,
		},
	)
}

type assertInsecureRegistryWorkInputs struct {
	key *manifest.WorkKey
	t   **testing.T
}

func assertInsecureRegistryWork(
	work *manifest.WorkRecorder,
	input string,
	want map[string]manifest.WorkCounts,
	captures *assertInsecureRegistryWorkInputs,
) {
	(*captures.t).Helper()
	counts := map[string]manifest.WorkCounts{}
	{
		records := work.Scope().Snapshot(0).Normalize()
		for recordIndex := range records {
			record := &records[recordIndex]
			assert.Equal((*captures.t), record.Key.Tree, captures.key.Tree)
			assert.Equal((*captures.t), record.Key.Owner, captures.key.Owner)
			assert.Equal(
				(*captures.t),
				record.Key.Source,
				manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindOCIRepository,
					Namespace: "ns",
					Name:      "app",
				},
			)
			if record.Key.Phase == manifest.WorkAcquisition || record.Key.Phase == manifest.WorkCache {
				if record.Key.Input == "" {
					(*captures.t).Fatal("OCI work lost its acquisition identity")
				}
				if input != "" {
					assert.Equal((*captures.t), record.Key.Input, input)
				}
			}
			sum := counts[string(record.Key.Phase)]
			sum.Add(&record.Counts)
			counts[string(record.Key.Phase)] = sum
		}
	}
	assert.Diff((*captures.t), counts, want)
}

type testFetcherInsecureRegistryResponseInputs struct {
	handler   *http.Handler
	heads     *atomic.Int32
	layers    *atomic.Int32
	manifests *atomic.Int32
	t         **testing.T
	tags      *atomic.Int32
}

func testFetcherInsecureRegistryResponse(
	w http.ResponseWriter,
	r *http.Request,
	captures *testFetcherInsecureRegistryResponseInputs,
) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		captures.tags.Add(1)
		_, _ = io.WriteString(w, `{"name":"charts/app","tags":["1.0.0"]}`)
		return
	case strings.Contains(r.URL.Path, "/manifests/"):
		if captures.tags.Load() == 0 {
			(*captures.t).Error("content requested before semver tags")
		}
		if r.Method == http.MethodHead {
			captures.heads.Add(1)
		} else {
			captures.manifests.Add(1)
		}
	case strings.Contains(r.URL.Path, "/blobs/"):
		captures.layers.Add(1)
	}
	(*captures.handler).ServeHTTP(w, r)
}
