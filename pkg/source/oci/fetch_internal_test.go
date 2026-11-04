package oci

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func newTestNetwork(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}

func TestFetcher_InsecureRegistry(t *testing.T) {
	t.Parallel()
	for _, chart := range []bool{false, true} {
		name := "OCIRepository"
		if chart {
			name = "HelmChart"
		}
		t.Run(
			name,
			func(t *testing.T) { t.Parallel(); testFetcherInsecureRegistryCase(t, &chart) },
		)
	}
}

type registryListener struct {
	net.Listener

	firstRead func([]byte)
}

func (l registryListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &registryConnection{Conn: c, firstRead: l.firstRead}, nil
}

type registryConnection struct {
	net.Conn

	firstRead func([]byte)
}

func (c *registryConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n != 0 && c.firstRead != nil {
		observe := c.firstRead
		c.firstRead = nil
		observe(p[:n])
	}
	return n, err
}

func TestFetcher_InsecureRegistryForeignMismatch(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"redirect", "token realm"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			foreign := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(foreign.Close)
			target := "https://" + mustURL(t, foreign.URL).Host + "/token"
			var plain atomic.Int32
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "redirect" {
					http.Redirect(w, r, target, http.StatusTemporaryRedirect)
					return
				}
				w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q,service="registry"`, target))
				w.WriteHeader(http.StatusUnauthorized)
			}))
			srv.Listener = registryListener{Listener: srv.Listener, firstRead: func(p []byte) {
				if p[0] != 0x16 {
					plain.Add(1)
				}
			}}
			srv.StartTLS()
			t.Cleanup(srv.Close)
			repo := &manifest.OCIRepository{
				Name:      "foreign",
				Namespace: "ns",
				URL:       "oci://" + mustURL(t, srv.URL).Host + "/app",
				Insecure:  true,
				Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
			}
			f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), &sourcecache.Policy{})
			requestErr, ok := errors.AsType[*url.Error](err)
			if got != nil || !errors.Is(err, http.ErrSchemeMismatch) || !ok ||
				mustURL(t, requestErr.URL).Host != mustURL(t, foreign.URL).Host ||
				plain.Load() != 0 {
				t.Fatalf("foreign mismatch: artifact=%v error=%v HTTP attempts=%d", got, err, plain.Load())
			}
		})
	}
}

func TestFetcher_InsecureRegistryErrors(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"response", "TLS", "reset", "cancel mismatch", "stalled HTTPS"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFetcherInsecureRegistryErrorsCase(t, &mode)
		},
		)
	}
}

func TestFetcher_InsecureRegistryCanceledMismatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var connections, plain atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		plain.Add(1)
	}))
	srv.Listener = registryListener{Listener: srv.Listener, firstRead: func(p []byte) {
		if connections.Add(1) == 2 {
			close(entered)
			<-release
		}
		if p[0] != 0x16 {
			plain.Add(1)
		}
	}}
	srv.Start()
	t.Cleanup(srv.Close)
	endpoint := "https://" + mustURL(t, srv.URL).Host + "/v2/app/manifests/latest"
	r, err := http.NewRequestWithContext(t.Context(), http.MethodHead, endpoint, http.NoBody)
	assert.NoError(t, err)

	_, mismatch := srv.Client().Do(r)
	assert.FatalIf(t, !errors.Is(mismatch, http.ErrSchemeMismatch), "initial registry mismatch: %v", mismatch)
	repo := &manifest.OCIRepository{
		Name:          "canceled",
		Namespace:     "ns",
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy},
	}
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}
	result := make(chan error, 1)
	go func() {
		got, err := f.Fetch(ctx, repo, (testInputs{}).readSet(repo), &sourcecache.Policy{})
		if got != nil {
			t.Error("canceled acquisition returned an artifact")
		}
		result <- err
	}()
	<-entered
	cancel(mismatch)
	unblock()
	if err := <-result; !errors.Is(err, mismatch) || !errors.Is(err, http.ErrSchemeMismatch) ||
		ctx.Err() != context.Canceled ||
		plain.Load() != 0 ||
		connections.Load() != 2 {
		t.Fatalf(
			"canceled mismatch: error=%v context=%v HTTP attempts=%d connections=%d",
			err,
			ctx.Err(),
			plain.Load(),
			connections.Load(),
		)
	}
}

func TestFetcher_InsecureRegistryAmbient(t *testing.T) {
	t.Parallel()
	art := newFakeOCIArtifact(t, map[string]string{"app.yaml": "ambient\n"})
	registry := newMutableFakeRegistry(t, art)

	handler := registry.server.Config.Handler
	var requests atomic.Int32
	registry.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if user, pass, ok := r.BasicAuth(); !ok || user != "alice" || pass != "password" {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	})
	registry.server.Start()
	host := mustURL(t, registry.server.URL).Host
	config := fmt.Sprintf(
		`{"auths":{%q:{"auth":%q}}}`,
		host,
		base64.StdEncoding.EncodeToString([]byte("alice:password")),
	)
	path := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, path, config)
	repo := &manifest.OCIRepository{
		Name:      "ambient",
		Namespace: "ns",
		URL:       "oci://" + host + "/app",
		Insecure:  true,
		SecretRef: &manifest.LocalObjectReference{Name: "missing"},
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	f := &Fetcher{
		Cache:          cachetest.New(t, t.TempDir(), sourcecache.New),
		Network:        newTestNetwork(t),
		RegistryConfig: path,
	}
	_, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), &sourcecache.Policy{MaxAge: time.Hour})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil || requests.Load() != 0 {
		t.Fatalf("ambient admission: error=%v requests=%d", err, requests.Load())
	}
	got, err := missing.RetryAmbient(t.Context())
	assert.FatalIf(
		t,
		err != nil || got == nil || mustReadFile(t, got.FS, "app.yaml") != "ambient\n",
		"ambient HTTP acquisition: artifact=%v error=%v",
		got,
		err,
	)
	before := requests.Load()
	warm, err := missing.RetryAmbient(t.Context())
	if err != nil || warm.Key != got.Key || requests.Load() != before {
		t.Fatalf("ambient warm acquisition: artifact=%v error=%v requests=%d", warm, err, requests.Load()-before)
	}
}

func TestFetcher_InsecureRegistryConcurrent(t *testing.T) {
	t.Parallel()
	art := newFakeOCIArtifact(t, map[string]string{"app.yaml": "concurrent\n"})
	registry := newMutableFakeRegistry(t, art)

	handler := registry.server.Config.Handler
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var initial, fail atomic.Bool
	initial.Store(true)
	fail.Store(true)
	var requests, tlsConnections atomic.Int32
	registry.server.Listener = registryListener{Listener: registry.server.Listener, firstRead: func(p []byte) {
		if p[0] == 0x16 {
			tlsConnections.Add(1)
		}
	}}
	registry.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method == http.MethodHead && initial.Load() {
			arrived <- struct{}{}
			<-release
		}
		if strings.HasPrefix(r.URL.Path, "/v2/bad/") && fail.Load() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		handler.ServeHTTP(w, r)
	})
	registry.server.Start()
	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Network: newTestNetwork(t)}

	done := make(chan insecureRegistryResult, 2)
	repos := make(map[string]*manifest.OCIRepository)
	for _, name := range []string{"good", "bad"} {
		repo := &manifest.OCIRepository{
			Name:      name,
			Namespace: "ns",
			URL:       "oci://" + mustURL(t, registry.server.URL).Host + "/" + name,
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
		}
		repos[name] = repo
		go func() {
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), &sourcecache.Policy{MaxAge: time.Hour})
			done <- insecureRegistryResult{name: name, artifact: got, err: err}
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case result := <-done:
			t.Fatalf("acquisition returned before concurrent producers: %s: %v", result.name, result.err)
		}
	}
	testFetcherInsecureRegistryConcurrentTail(
		t,
		&testFetcherInsecureRegistryConcurrentTailState{
			release:        &release,
			unblock:        &unblock,
			initial:        &initial,
			fail:           &fail,
			requests:       &requests,
			tlsConnections: &tlsConnections,
			f:              &f,
			done:           &done,
			repos:          &repos,
		},
	)
}

func TestFetcher_ExtractsLayerWithoutTitleAnnotation(t *testing.T) {
	t.Parallel()

	names := []string{"gotk-components.yaml", "kustomization.yaml", "namespace.yaml"}
	bodies := []string{
		"kind: ConfigMap\n",
		"resources: [gotk-components.yaml, namespace.yaml]\n",
		"kind: Namespace\nmetadata: {name: flux-system}\n",
	}
	files := make(map[string]string, len(names))
	for i, name := range names {
		files[name] = bodies[i]
	}
	reversed := make(map[string]string, len(files))
	for _, name := range slices.Backward(names) {
		reversed[name] = files[name]
	}
	layerBytes := testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)
	{
		other := testutil.TarGz(t, reversed, 0o644, gzip.DefaultCompression)
		assert.FatalIf(t, !bytes.Equal(layerBytes, other), "%s", "equal file maps produced different gzip bytes")
	}

	gr, err := gzip.NewReader(bytes.NewReader(layerBytes))
	assert.NoError(t, err)

	tr := tar.NewReader(gr)
	for _, name := range names {
		header, nextErr := tr.Next()
		assert.NoError(t, nextErr)

		assert.FatalIf(
			t,
			header.Name != name || header.Typeflag != tar.TypeReg || header.Mode != 0o644,
			"tar header=%+v, want regular %s mode 0644",
			header,
			name,
		)
	}
	{
		header, nextErr2 := tr.Next()
		assert.FatalIf(t, !errors.Is(nextErr2, io.EOF), "extra tar entry=%+v, error=%v", header, nextErr2)
	}
	{
		closeErr := gr.Close()
		assert.NoError(t, closeErr)
	}

	configBytes := []byte(`{"created":"2026-01-01T00:00:00Z"}`)
	manifestBytes := mustManifestJSON(t,
		manifestBlob{data: configBytes, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layerBytes, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)

	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name:      "flux-manifests",
		Namespace: "flux-system",
		URL:       fmt.Sprintf("oci://%s/fluxcd/flux-manifests", mustURL(t, srv.URL).Host),
		// httptest.NewTLSServer issues a self-signed cert; flate
		// maps spec.insecure to TLS InsecureSkipVerify.
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v2.8.8"},
	}

	art, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.FatalIf(t, err != nil, "Fetch: %v", err)
	assert.FatalIf(t, art == nil || art.FS == nil, "%s", "Fetch returned no artifact")

	got, err := fs.ReadFile(art.FS, "gotk-components.yaml")
	if err != nil {
		t.Fatalf("expected extracted gotk-components.yaml under %s: %v\nslot contents: %v",
			art.Key, err, art.FS)
	}
	{
		want := "kind: ConfigMap\n"
		assert.ErrorIf(t, string(got) != want, "gotk-components.yaml = %q, want %q", got, want)
	}

	for _, name := range []string{"blobs", "index.json", "oci-layout"} {
		{
			_, err := fs.Stat(art.FS, name)
			assert.ErrorIf(
				t,
				!errors.Is(err, fs.ErrNotExist),
				"leftover OCI layout artifact in slot: %s (err: %v)",
				name,
				err,
			)
		}
	}
}

// TestFetcher_ExtractCollidesWithOCILayoutName guards the staging
// step in applyLayerSelector: a tarball whose entries collide with
// OCI Image Layout well-known names (blobs/, index.json, oci-layout)
// must still extract cleanly. The staging dance moves the layer out
// of the layout subtree before the wipe, then extracts onto a slot
// with no surviving layout dirs to collide against.
func TestFetcher_ExtractCollidesWithOCILayoutName(t *testing.T) {
	t.Parallel()

	layerBytes := testutil.TarGz(t, map[string]string{
		"blobs/should-survive.yaml": "kind: ConfigMap\nmetadata:\n  name: survives\n",
		"index.json":                `{"user": "data"}`,
		"oci-layout":                "user-owned content",
		"kustomization.yaml":        "resources:\n- blobs/should-survive.yaml\n",
	}, 0o644, gzip.DefaultCompression)
	configBytes := []byte(`{}`)
	manifestBytes := mustManifestJSON(t,
		manifestBlob{data: configBytes, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layerBytes, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)
	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name:      "collider",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/collider", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
	}

	art, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.FatalIf(t, err != nil, "Fetch: %v", err)

	// User's blobs/should-survive.yaml must be intact, not wiped by
	// the OCI layout cleanup.
	got, err := fs.ReadFile(art.FS, "blobs/should-survive.yaml")
	assert.FatalIf(t, err != nil, "user's blobs/should-survive.yaml was wiped by cleanup: %v", err)
	assert.ErrorIf(t, !strings.Contains(string(got), "name: survives"), "survives.yaml content lost: %q", got)
	// User's index.json + oci-layout files must also survive.
	for _, name := range []string{"index.json", "oci-layout", "kustomization.yaml"} {
		{
			_, err := fs.Stat(art.FS, name)
			assert.ErrorIf(t, err != nil, "user's %s was wiped: %v", name, err)
		}
	}
}

func TestFetcher_Freshness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                            string
		interval                        time.Duration
		pinned, mixed, advance, offline bool
		failRefresh, blockBlobs         bool
	}{
		{name: "mutable tag refresh", advance: true},
		{name: "fresh interval", interval: time.Hour, advance: true, offline: true},
		{name: "warm tag hit", interval: time.Hour, blockBlobs: true},
		{name: "failed refresh", offline: true, failRefresh: true},
		{name: "digest pinned offline", pinned: true, offline: true},
		{name: "mixed digest pinned offline", pinned: true, mixed: true, offline: true},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) { t.Parallel(); testFetcherFreshnessCase(t, &tt) },
		)
	}
}

func TestFetcher_Revalidation(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{sourcev1.OCILayerExtract, sourcev1.OCILayerCopy} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testFetcherRevalidationCase(t, &operation)
		},
		)
	}
}

type testFetcherInsecureRegistryConcurrentTailState struct {
	release        *chan struct{}
	unblock        *sync.Once
	initial        *atomic.Bool
	fail           *atomic.Bool
	requests       *atomic.Int32
	tlsConnections *atomic.Int32
	f              **Fetcher
	done           *chan insecureRegistryResult
	repos          *map[string]*manifest.OCIRepository
}

func testFetcherInsecureRegistryConcurrentTail(t *testing.T, scope *testFetcherInsecureRegistryConcurrentTailState) {
	t.Helper()
	scope.unblock.Do(func() { close((*scope.release)) })
	for range 2 {
		result := <-(*scope.done)
		if result.name == "bad" {
			response, ok := errors.AsType[*errcode.ErrorResponse](result.err)
			if result.artifact != nil || !ok || response.StatusCode != http.StatusBadRequest {
				t.Fatalf("failed producer: artifact=%v error=%v", result.artifact, result.err)
			}
		} else if result.err != nil || result.artifact == nil || mustReadFile(t, result.artifact.FS, "app.yaml") != "concurrent\n" {
			t.Fatalf("successful producer: artifact=%v error=%v", result.artifact, result.err)
		}
	}
	if scope.tlsConnections.Load() != 2 {
		t.Fatalf("initial HTTPS acquisitions=%d, want 2", scope.tlsConnections.Load())
	}
	scope.initial.Store(false)
	scope.fail.Store(false)
	recovered, err := (*scope.f).Fetch(
		t.Context(),
		(*scope.repos)["bad"],
		(testInputs{}).readSet((*scope.repos)["bad"]),
		&sourcecache.Policy{MaxAge: time.Hour},
	)
	if err != nil || recovered == nil || mustReadFile(t, recovered.FS, "app.yaml") != "concurrent\n" ||
		scope.tlsConnections.Load() != 3 {
		t.Fatalf(
			"failed producer recovery: artifact=%v error=%v HTTPS attempts=%d",
			recovered,
			err,
			scope.tlsConnections.Load(),
		)
	}
	before := scope.requests.Load()
	secure := *(*scope.repos)["good"]
	secure.Insecure = false
	got, err := (*scope.f).Fetch(t.Context(), &secure, (testInputs{}).readSet(&secure), &sourcecache.Policy{})
	if got != nil || !errors.Is(err, http.ErrSchemeMismatch) || scope.requests.Load() != before ||
		scope.tlsConnections.Load() != 4 {
		t.Fatalf(
			"protocol selection leaked: artifact=%v error=%v HTTP requests=%d HTTPS attempts=%d",
			got,
			err,
			scope.requests.Load()-before,
			scope.tlsConnections.Load(),
		)
	}
}

type insecureRegistryResult struct {
	name     string
	artifact *store.SourceArtifact
	err      error
}
