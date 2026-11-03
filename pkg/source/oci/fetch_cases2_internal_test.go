package oci

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/store"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/registry"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func testFetcherInsecureRegistryCase(t *testing.T, chart *bool,
) {
	t.Helper()

	const body = "apiVersion: v2\nname: app\nversion: 1.0.0\n"
	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": body})
	art.manifest = mustManifestJSON(
		t,
		manifestBlob{data: art.config, mediaType: registry.ConfigMediaType},
		manifestBlob{data: art.layer, mediaType: registry.ChartLayerMediaType},
	)
	art.manifestDigest = sha256Digest(art.manifest)
	srv, _ := newMutableFakeRegistry(t, art)
	var tags, heads, manifests, layers atomic.Int32
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherInsecureRegistryCaseCallback(
			w,
			r,
			testFetcherInsecureRegistryCaseCallbackInputs{
				t:         &t,
				tags:      &tags,
				heads:     &heads,
				manifests: &manifests,
				layers:    &layers,
				handler:   &handler,
			},
		)
	},
	)
	srv.Start()
	fetch := func(f *Fetcher, insecure bool) (*store.SourceArtifact, error) {
		if !(*chart) {
			repo := &manifest.OCIRepository{
				Name:      "app",
				Namespace: "ns",
				URL:       "oci://" + mustURL(t, srv.URL).Host + "/charts/app",
				Insecure:  insecure,
				Reference: &sourcev1.OCIRepositoryRef{SemVer: "1.x"},
			}
			return f.Fetch(
				t.Context(),
				repo,
				(testInputs{}).readSet(repo),
				sourcecache.Policy{MaxAge: time.Hour},
			)
		}
		repo := &manifest.HelmRepository{
			Name:      "charts",
			Namespace: "ns",
			URL:       "oci://" + mustURL(t, srv.URL).Host + "/charts",
			Type:      sourcev1.HelmRepositoryTypeOCI,
			Insecure:  insecure,
			Interval:  metav1.Duration{Duration: time.Hour},
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
		reads := (testInputs{repos: func(string, string) *manifest.HelmRepository { return repo }}).readSet(hc)
		return helmchart.New(f, helmchart.Config{Cache: f.Cache, Network: f.Network}).
			Fetch(t.Context(), hc, reads, sourcecache.Policy{MaxAge: time.Hour})
	}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
	failed, err := fetch(f, false)
	if failed != nil || !errors.Is(err, http.ErrSchemeMismatch) || tags.Load() != 0 {
		t.Fatalf("secure default: artifact=%v error=%v tags=%d", failed, err, tags.Load())
	}
	var first *store.SourceArtifact
	for range 2 {
		f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
		beforeTags, beforeHeads, beforeManifests, beforeLayers := tags.Load(), heads.Load(), manifests.Load(), layers.Load()
		got, err := fetch(f, true)
		assert.NoError(t, err)

		assert.FatalIf(
			t,
			got == nil || mustReadFile(t, got.FS, "Chart.yaml") != body,
			"artifact content=%v",
			got,
		)
		wantKind := manifest.KindOCIRepository
		if *chart {
			wantKind = manifest.KindHelmChart
		}
		if got.Kind != wantKind || tags.Load()-beforeTags != 1 || heads.Load()-beforeHeads != 1 ||
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
		assert.FatalIf(t, first != nil &&
			(got.Key != first.Key || got.Content != first.Content || got.Digest != first.Digest || got.Revision != first.Revision), "independent acquisition identity changed: first=%+v second=%+v", first, got)
		first = got
		warm, err := fetch(f, true)
		assert.FatalIf(
			t,
			err != nil || warm.Key != got.Key || tags.Load()-beforeTags != 1 || heads.Load()-beforeHeads != 1 ||
				manifests.Load()-beforeManifests != 1 ||
				layers.Load()-beforeLayers != 1,
			"warm acquisition requested content: artifact=%v error=%v",
			warm,
			err,
		)
	}
}

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
		got, err := f.Fetch(ctx, repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
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
		{
			_, ok := errors.AsType[*net.OpError](err)
			assert.FatalIf(t, !ok || errors.Is(err, http.ErrSchemeMismatch), "lost TLS failure: %v", err)
		}
	case "reset":
		{
			_, ok := errors.AsType[*url.Error](err)
			assert.FatalIf(t, !ok || errors.Is(err, http.ErrSchemeMismatch), "lost reset failure: %v", err)
		}
	}
}

func testFetcherFreshnessCase(t *testing.T, tt *struct {
	name        string
	interval    time.Duration
	pinned      bool
	mixed       bool
	advance     bool
	offline     bool
	failRefresh bool
	blockBlobs  bool
},
) {
	t.Helper()

	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := newMutableFakeRegistry(t, v1, v2)
	var resolves, pulls atomic.Int32
	var blockBlobs atomic.Bool
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherFreshnessCaseCallback(
			w,
			r,
			testFetcherFreshnessCaseCallbackInputs{
				resolves:   &resolves,
				pulls:      &pulls,
				blockBlobs: &blockBlobs,
				handler:    &handler,
			},
		)
	},
	)
	srv.StartTLS()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name: "freshness", Namespace: "test",
		URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
		Interval:  &metav1.Duration{Duration: time.Hour},
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	if tt.pinned {
		repo.Reference = &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest}
		if tt.mixed {
			repo.Reference.Tag = "latest"
			repo.Reference.SemVer = "1.x"
		}
	}
	first, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: tt.interval},
	)
	assert.NoError(t, err)

	{
		got := mustReadFile(t, first.FS, "app.yaml")
		assert.FatalIf(t, got != "version: v1\n", "first content = %q", got)
	}
	beforeResolve, beforePull := resolves.Load(), pulls.Load()
	assert.FatalIf(
		t,
		beforeResolve == 0 || beforePull == 0,
		"first fetch: resolves=%d pulls=%d",
		beforeResolve,
		beforePull,
	)
	if tt.advance {
		setArtifact(1)
	}
	blockBlobs.Store(tt.blockBlobs)
	if tt.offline {
		srv.Close()
	}
	shared := &Fetcher{Network: newTestNetwork(t), Cache: f.Cache}
	second, err := shared.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: tt.interval},
	)
	if tt.failRefresh {
		assert.FatalIf(t, err == nil, "%s", "expected refresh to fail after registry shutdown")

		{
			got := mustReadFile(t, first.FS, "app.yaml")
			assert.FatalIf(t, got != "version: v1\n", "failed refresh damaged held content: %q", got)
		}
		return
	}
	assert.NoError(t, err)

	want := "version: v1\n"
	if tt.advance && !tt.offline {
		want = "version: v2\n"
		if second.Digest == first.Digest || second.Revision == first.Revision ||
			resolves.Load() <= beforeResolve ||
			pulls.Load() <= beforePull {
			t.Fatalf(
				"mutable tag was not refreshed: digest=%s revision=%s resolves=%d pulls=%d",
				second.Digest,
				second.Revision,
				resolves.Load()-beforeResolve,
				pulls.Load()-beforePull,
			)
		}
	} else if second.Digest != first.Digest || second.Revision != first.Revision || resolves.Load() != beforeResolve || pulls.Load() != beforePull {
		t.Fatalf(
			"cache hit changed digest/revision or requested content: resolves=%d pulls=%d",
			resolves.Load()-beforeResolve,
			pulls.Load()-beforePull,
		)
	}
	if second.Key != first.Key {
		t.Fatalf("acquisition key changed: %q -> %q", first.Key, second.Key)
	}
	{
		got := mustReadFile(t, second.FS, "app.yaml")
		assert.FatalIf(t, got != want, "second content = %q, want %q", got, want)
	}
}

func testFetcherRevalidationCase(t *testing.T, operation *string,
) {
	t.Helper()

	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := newMutableFakeRegistry(t, v1, v2)
	var heads, manifests, layers atomic.Int32
	var failResolve atomic.Bool
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherRevalidationCaseCallback(
			w,
			r,
			testFetcherRevalidationCaseCallbackInputs{
				heads:       &heads,
				manifests:   &manifests,
				layers:      &layers,
				failResolve: &failResolve,
				handler:     &handler,
			},
		)
	},
	)
	srv.StartTLS()
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name:          "revalidate",
		Namespace:     "test",
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	var identity string
	for _, phase := range []string{"initial", "unchanged", "reopened", "changed", "missing", "truncated", "resolve failure"} {
		if phase == "missing" || phase == "truncated" {
			payload := v2.layer
			payload = revalidationPayload(t, payload, operation)
			found := 0
			err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				return testFetcherRevalidationCaseCallback2(
					path,
					entry,
					err,
					testFetcherRevalidationCaseCallback2Inputs{phase: &phase, payload: &payload, found: &found},
				)
			},
			)
			assert.FatalIf(t, err != nil || found != 1, "%s backing: found=%d error=%v", phase, found, err)
		}
		if phase == "reopened" || phase == "missing" || phase == "truncated" {
			{
				err := f.Cache.Close()
				assert.NoError(t, err)
			}

			f.Cache = cachetest.New(t, root, sourcecache.New)
		}
		if phase == "changed" {
			setArtifact(1)
		}
		failResolve.Store(phase == "resolve failure")
		beforeHeads, beforeManifests, beforeLayers := heads.Load(), manifests.Load(), layers.Load()
		art, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		if phase == "resolve failure" {
			assert.FatalIf(
				t,
				err == nil || !errors.Is(err, manifest.ErrFlux) || art != nil,
				"stale revalidation result: artifact=%+v error=%v",
				art,
				err,
			)
			assert.FatalIf(t, heads.Load()-beforeHeads != 1 || manifests.Load() != beforeManifests ||
				layers.Load() != beforeLayers, "%s", "failed resolve requested content")

			continue
		}
		assert.NoError(t, err)

		wantLayers := int32(0)
		if phase == "initial" || phase == "changed" || phase == "missing" || phase == "truncated" {
			wantLayers = 1
		}
		h, m, l := heads.Load()-beforeHeads, manifests.Load()-beforeManifests, layers.Load()-beforeLayers
		assert.FatalIf(
			t,
			h != 1 || m != wantLayers || l != wantLayers,
			"%s HEAD/manifest GET/layer GET=%d/%d/%d, want 1/%d/%d",
			phase,
			h,
			m,
			l,
			wantLayers,
			wantLayers,
		)
		t.Logf("%s HEAD/manifest GET/layer GET=%d/%d/%d", phase, h, m, l)
		got := sourcecache.AcquisitionIdentity(art.FS)
		checkRevalidationIdentity(t, phase, &identity, got)

		checkRevalidationOutput(t, revalidationOutput{
			operation: *operation, phase: phase, v1: &v1, v2: &v2, art: art,
		})
	}
}

type testFetcherCacheIsolatesSourcesCaseInputs struct {
	c          **sourcecache.Cache
	body       *string
	repo       **manifest.OCIRepository
	request    *registryAcquisition
	identities *map[string]string
	tc         *struct {
		name   string
		change func(*manifest.OCIRepository, *Fetcher)
	}
}

func testFetcherCacheIsolatesSourcesCase(t *testing.T, testCaseInput testFetcherCacheIsolatesSourcesCaseInputs) {
	t.Helper()
	current := *(*testCaseInput.repo)
	fetcher := &Fetcher{Network: newTestNetwork(t), Cache: (*testCaseInput.c)}
	testCaseInput.tc.
		change(&current, fetcher)
	got, err := (&operation{Fetcher: fetcher}).fetchLayer(
		t.Context(),
		&current, new((*testCaseInput.request)), sourcecache.Policy{MaxAge: current.SourceInterval()},
	)
	assert.NoError(t, err)

	if previous, ok := (*testCaseInput.identities)[got.Key]; ok {
		t.Fatalf("%s reused %s acquisition", testCaseInput.tc.name, previous)
	}
	(*testCaseInput.identities)[got.Key] = testCaseInput.tc.name
	{
		got := mustReadFile(t, got.FS, "app.yaml")
		assert.FatalIf(t, got != (*testCaseInput.body), "variant content=%q, want %q", got, (*testCaseInput.body))
	}
}

type testFetcherInsecureRegistryCaseCallbackInputs struct {
	t         **testing.T
	tags      *atomic.Int32
	heads     *atomic.Int32
	manifests *atomic.Int32
	layers    *atomic.Int32
	handler   *http.Handler
}

func testFetcherInsecureRegistryCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherInsecureRegistryCaseCallbackInputs,
) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		testCaseInput.tags.
			Add(1)
		_, _ = io.WriteString(w, `{"name":"charts/app","tags":["1.0.0"]}`)
		return
	case strings.Contains(r.URL.Path, "/manifests/"):
		if testCaseInput.tags.Load() == 0 {
			(*testCaseInput.t).
				Error("content requested before semver tags")
		}
		if r.Method == http.MethodHead {
			testCaseInput.heads.
				Add(1)
		} else {
			testCaseInput.manifests.
				Add(1)
		}
	case strings.Contains(r.URL.Path, "/blobs/"):
		testCaseInput.layers.
			Add(1)
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}

type testFetcherFreshnessCaseCallbackInputs struct {
	resolves   *atomic.Int32
	pulls      *atomic.Int32
	blockBlobs *atomic.Bool
	handler    *http.Handler
}

func testFetcherFreshnessCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherFreshnessCaseCallbackInputs,
) {
	if strings.Contains(r.URL.Path, "/manifests/") {
		testCaseInput.resolves.
			Add(1)
	}
	if strings.Contains(r.URL.Path, "/blobs/") {
		testCaseInput.pulls.
			Add(1)
		if testCaseInput.blockBlobs.Load() {
			http.Error(w, "blob pulls disabled", http.StatusServiceUnavailable)
			return
		}
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}

type testFetcherRevalidationCaseCallbackInputs struct {
	heads       *atomic.Int32
	manifests   *atomic.Int32
	layers      *atomic.Int32
	failResolve *atomic.Bool
	handler     *http.Handler
}

func testFetcherRevalidationCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherRevalidationCaseCallbackInputs,
) {
	switch {
	case strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead:
		testCaseInput.heads.
			Add(1)
		if testCaseInput.failResolve.Load() {
			http.Error(w, "resolution failed", http.StatusBadRequest)
			return
		}
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

type testFetcherRevalidationCaseCallback2Inputs struct {
	phase   *string
	payload *[]byte
	found   *int
}

func testFetcherRevalidationCaseCallback2(
	path string,
	entry fs.DirEntry,
	err error,
	testCaseInput testFetcherRevalidationCaseCallback2Inputs,
) error {
	if err != nil || !entry.Type().IsRegular() {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, (*testCaseInput.payload)) {
		return err
	}
	(*testCaseInput.found)++
	if (*testCaseInput.phase) == "missing" {
		return os.Remove(path)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	return os.WriteFile(path, []byte("truncated"), 0o600)
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
