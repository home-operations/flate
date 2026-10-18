package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
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
	"testing/iotest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

// TestFetcher_ExtractsLayerWithoutTitleAnnotation regresses the silent
// no-op that Zariel/home-ops hit on flux-manifests: every blob in a
// Flux OCIRepository artifact lacks `org.opencontainers.image.title`.
// orasfile's default in-memory fallback used to swallow them, leaving
// the slot empty and `kustomize build` rendering zero manifests
// without failing. With content/oci.Store (see oci.go) blobs land at
// `slot/blobs/<algo>/<hex>` regardless of annotations.
func TestFetcher_ExtractsLayerWithoutTitleAnnotation(t *testing.T) {
	t.Parallel()

	layerBytes := mustTarGz(t, map[string]string{
		"gotk-components.yaml": "kind: ConfigMap\n",
	})
	configBytes := []byte(`{"created":"2026-01-01T00:00:00Z"}`)
	manifestBytes := mustManifestJSON(t,
		manifestBlob{data: configBytes, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layerBytes, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)

	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "flux-manifests",
		Namespace: "flux-system",
		URL:       fmt.Sprintf("oci://%s/fluxcd/flux-manifests", mustURL(t, srv.URL).Host),
		// httptest.NewTLSServer issues a self-signed cert; flate
		// maps spec.insecure to TLS InsecureSkipVerify.
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v2.8.8"},
	}

	art, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art == nil || art.FS == nil {
		t.Fatal("Fetch returned no artifact")
	}

	// Observable proof that the layer made it to disk and got extracted.
	got, err := fs.ReadFile(art.FS, "gotk-components.yaml")
	if err != nil {
		t.Fatalf("expected extracted gotk-components.yaml under %s: %v\nslot contents: %v",
			art.Key, err, art.FS)
	}
	if want := "kind: ConfigMap\n"; string(got) != want {
		t.Errorf("gotk-components.yaml = %q, want %q", got, want)
	}

	// OCI layout artifacts should be wiped after extract so kustomize /
	// downstream consumers see only the artifact's own files.
	for _, name := range []string{"blobs", "index.json", "oci-layout"} {
		if _, err := fs.Stat(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("leftover OCI layout artifact in slot: %s (err: %v)", name, err)
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

	layerBytes := mustTarGz(t, map[string]string{
		"blobs/should-survive.yaml": "kind: ConfigMap\nmetadata:\n  name: survives\n",
		"index.json":                `{"user": "data"}`,
		"oci-layout":                "user-owned content",
		"kustomization.yaml":        "resources:\n- blobs/should-survive.yaml\n",
	})
	configBytes := []byte(`{}`)
	manifestBytes := mustManifestJSON(t,
		manifestBlob{data: configBytes, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layerBytes, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)
	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "collider",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/collider", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
	}

	art, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// User's blobs/should-survive.yaml must be intact, not wiped by
	// the OCI layout cleanup.
	got, err := fs.ReadFile(art.FS, "blobs/should-survive.yaml")
	if err != nil {
		t.Fatalf("user's blobs/should-survive.yaml was wiped by cleanup: %v", err)
	}
	if !strings.Contains(string(got), "name: survives") {
		t.Errorf("survives.yaml content lost: %q", got)
	}
	// User's index.json + oci-layout files must also survive.
	for _, name := range []string{"index.json", "oci-layout", "kustomization.yaml"} {
		if _, err := fs.Stat(art.FS, name); err != nil {
			t.Errorf("user's %s was wiped: %v", name, err)
		}
	}
}

func TestFetcher_Freshness(t *testing.T) {
	tests := []struct {
		name                     string
		interval                 time.Duration
		pinned, advance, offline bool
		failRefresh, blockBlobs  bool
	}{
		{name: "mutable tag refresh", advance: true},
		{name: "fresh interval", interval: time.Hour, advance: true, offline: true},
		{name: "warm tag hit", interval: time.Hour, blockBlobs: true},
		{name: "failed refresh", offline: true, failRefresh: true},
		{name: "digest pinned offline", pinned: true, offline: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
			v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
			srv, setArtifact := startMutableFakeRegistry(t, v1, v2)
			var resolves, pulls atomic.Int32
			var blockBlobs atomic.Bool
			handler := srv.Config.Handler
			srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/manifests/") {
					resolves.Add(1)
				}
				if strings.Contains(r.URL.Path, "/blobs/") {
					pulls.Add(1)
					if blockBlobs.Load() {
						http.Error(w, "blob pulls disabled", http.StatusServiceUnavailable)
						return
					}
				}
				handler.ServeHTTP(w, r)
			})
			f := &Fetcher{Cache: newSourceCache(t)}
			repo := &manifest.OCIRepository{
				Name: "freshness", Namespace: "test",
				URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
				Interval:  metav1.Duration{Duration: tt.interval},
				Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
			}
			if tt.pinned {
				repo.Reference = &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest}
			}
			first, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if got := mustReadFile(t, first.FS, "app.yaml"); got != "version: v1\n" {
				t.Fatalf("first content = %q", got)
			}
			beforeResolve, beforePull := resolves.Load(), pulls.Load()
			if beforeResolve == 0 || beforePull == 0 {
				t.Fatalf("first fetch: resolves=%d pulls=%d", beforeResolve, beforePull)
			}
			if tt.advance {
				setArtifact(1)
			}
			blockBlobs.Store(tt.blockBlobs)
			if tt.offline {
				srv.Close()
			}
			second, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
			if tt.failRefresh {
				if err == nil {
					t.Fatal("expected refresh to fail after registry shutdown")
				}
				if got := mustReadFile(t, first.FS, "app.yaml"); got != "version: v1\n" {
					t.Fatalf("failed refresh damaged held content: %q", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := "version: v1\n"
			if tt.advance && !tt.offline {
				want = "version: v2\n"
				if second.Digest == first.Digest || second.Revision == first.Revision || resolves.Load() <= beforeResolve || pulls.Load() <= beforePull {
					t.Fatalf("mutable tag was not refreshed: digest=%s revision=%s resolves=%d pulls=%d", second.Digest, second.Revision, resolves.Load()-beforeResolve, pulls.Load()-beforePull)
				}
			} else if second.Digest != first.Digest || second.Revision != first.Revision || resolves.Load() != beforeResolve || pulls.Load() != beforePull {
				t.Fatalf("cache hit changed digest/revision or requested content: resolves=%d pulls=%d", resolves.Load()-beforeResolve, pulls.Load()-beforePull)
			}
			if second.Key != first.Key {
				t.Fatalf("acquisition key changed: %q -> %q", first.Key, second.Key)
			}
			if got := mustReadFile(t, second.FS, "app.yaml"); got != want {
				t.Fatalf("second content = %q, want %q", got, want)
			}
		})
	}
}

// startFakeRegistry serves the minimum subset of the OCI Distribution
// API that oras.Copy needs: a /v2/ probe, manifests by tag, and blobs
// by digest. httptest.NewTLSServer's self-signed cert pairs with the
// caller's spec.insecure to bypass verification.
func startFakeRegistry(t *testing.T, manifestBytes, configBytes, layerBytes []byte) *httptest.Server {
	t.Helper()
	configDigest := sha256Digest(configBytes)
	layerDigest := sha256Digest(layerBytes)
	manifestDigest := sha256Digest(manifestBytes)

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			// Distribution v2 probe.
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write(manifestBytes)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, digest, _ := strings.CutLast(r.URL.Path, "/")
			switch digest {
			case configDigest:
				_, _ = w.Write(configBytes)
			case layerDigest:
				_, _ = w.Write(layerBytes)
			default:
				http.NotFound(w, r)
			}
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
	layer := mustTarGz(t, files)
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

func startMutableFakeRegistry(t *testing.T, artifacts ...fakeOCIArtifact) (*httptest.Server, func(int)) {
	t.Helper()
	if len(artifacts) == 0 {
		t.Fatal("no OCI artifacts")
	}
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
	srv := httptest.NewTLSServer(mux)
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

func mustManifestJSON(t *testing.T, config, layer manifestBlob) []byte {
	t.Helper()
	m := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: config.mediaType,
			Digest:    digest.Digest(sha256Digest(config.data)),
			Size:      int64(len(config.data)),
		},
		Layers: []ocispec.Descriptor{{
			MediaType: layer.mediaType,
			Digest:    digest.Digest(sha256Digest(layer.data)),
			Size:      int64(len(layer.data)),
		}},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sha256Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func mustReadFile(t *testing.T, input fs.FS, path string) string {
	t.Helper()
	b, err := fs.ReadFile(input, path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func newSourceCache(t *testing.T) *sourcecache.Cache {
	t.Helper()
	c, err := sourcecache.New(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

func TestFetcher_CacheIsolatesSources(t *testing.T) {
	c := newSourceCache(t)
	f := &Fetcher{Cache: c}
	var keys []string
	for _, body := range []string{"first", "second"} {
		v := newFakeOCIArtifact(t, map[string]string{"app.yaml": body})
		srv := startFakeRegistry(t, v.manifest, v.config, v.layer)
		repo := &manifest.OCIRepository{URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Digest: v.manifestDigest}}
		art, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
		if err != nil {
			t.Fatal(err)
		}
		if got := mustReadFile(t, art.FS, "app.yaml"); got != body {
			t.Fatalf("source collision: %q, want %q", got, body)
		}
		keys = append(keys, art.Key)
		if body != "first" {
			continue
		}
		client, material, err := f.newRepoClient(t.Context(), repo, nil)
		if err != nil {
			t.Fatal(err)
		}
		identities := map[string]string{art.Key: "original"}
		for _, tc := range []struct {
			name   string
			change func(*manifest.OCIRepository, *Fetcher)
		}{
			{"ref", func(r *manifest.OCIRepository, _ *Fetcher) { r.Reference = &manifest.OCIRepositoryRef{Tag: "latest"} }},
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
				current := *repo
				fetcher := &Fetcher{Cache: c}
				tc.change(&current, fetcher)
				got, err := fetcher.fetch(t.Context(), &current, registryAcquisition{client, material}, sourcecache.RetryConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if previous, ok := identities[got.Key]; ok {
					t.Fatalf("%s reused %s acquisition", tc.name, previous)
				}
				identities[got.Key] = tc.name
				if got := mustReadFile(t, got.FS, "app.yaml"); got != body {
					t.Fatalf("variant content=%q, want %q", got, body)
				}
			})
		}
	}
	if keys[0] == keys[1] {
		t.Fatal("distinct sources share their published identity")
	}
}

func TestFetcher_LayerDigestMismatchPublishesNothing(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		for _, corruption := range []string{"digest", "gzip header", "gzip trailer"} {
			t.Run(backing+"/"+corruption, func(t *testing.T) {
				honest := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
				broken := honest
				payload := mustTarGz(t, map[string]string{"app.yaml": "version: v2\n"})
				if corruption != "digest" {
					if corruption == "gzip header" {
						payload = []byte{0x1f, 0x8b}
					} else {
						payload = honest.layer[:len(honest.layer)-4]
					}
					broken.layerDigest = sha256Digest(payload)
					broken.manifest = mustManifestJSON(t,
						manifestBlob{data: honest.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
						manifestBlob{data: payload, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
					)
					broken.manifestDigest = sha256Digest(broken.manifest)
				} else if len(payload) != len(honest.layer) {
					t.Fatal("fixture lengths differ")
				}
				var repaired atomic.Bool
				var layers atomic.Int32
				srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					current := broken
					if repaired.Load() {
						current = honest
					}
					switch {
					case strings.Contains(r.URL.Path, "/manifests/"):
						w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
						w.Header().Set("Docker-Content-Digest", current.manifestDigest)
						_, _ = w.Write(current.manifest)
					case strings.HasSuffix(r.URL.Path, current.layerDigest):
						layers.Add(1)
						if repaired.Load() {
							_, _ = w.Write(honest.layer)
						} else {
							_, _ = w.Write(payload)
						}
					case strings.HasSuffix(r.URL.Path, current.configDigest):
						_, _ = w.Write(current.config)
					}
				}))
				t.Cleanup(srv.Close)
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c, err := sourcecache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				f := &Fetcher{Cache: c}
				repo := &manifest.OCIRepository{Name: "tampered", Namespace: "test",
					URL: "oci://" + mustURL(t, srv.URL).Host + "/chart", Insecure: true,
					Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}, Interval: metav1.Duration{Duration: time.Hour}}
				for attempt := range 3 {
					if attempt == 2 && backing == "disk" {
						if err := c.Close(); err != nil {
							t.Fatal(err)
						}
						c, err = sourcecache.New(t.Context(), root)
						if err != nil {
							t.Fatal(err)
						}
						f.Cache = c
					}
					_, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
					terminal, _ := errors.AsType[*sourcecache.TerminalError](err)
					if err == nil || terminal != nil && !terminal.Attempted || layers.Load() != int32(attempt+1) {
						t.Fatalf("integrity failure suppressed: layers=%d terminal=%+v error=%v", layers.Load(), terminal, err)
					}
					if corruption != "digest" && !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("lost gzip cause: %v", err)
					}
				}
				repaired.Store(true)
				art, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if got := mustReadFile(t, art.FS, "app.yaml"); got != "version: v1\n" {
					t.Fatalf("published %q", got)
				}
			})
		}
	}
}

func TestFetcher_FailureSourceAndOperation(t *testing.T) {
	for _, layerOperation := range []string{sourcev1.OCILayerCopy, sourcev1.OCILayerExtract} {
		for _, tc := range []struct {
			operation string
			status    int
			attempts  int32
			replay    bool
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
			operation := tc.operation
			if operation == "layer header" && layerOperation == sourcev1.OCILayerCopy {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s/%d", layerOperation, operation, tc.status), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				art := newFakeOCIArtifact(t, map[string]string{"file": "x"})
				var requests, failures atomic.Int32
				srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					switch {
					case strings.HasSuffix(r.URL.Path, "/tags/list"):
						failures.Add(1)
						http.Error(w, "registry failure", tc.status)
					case strings.Contains(r.URL.Path, "/manifests/"):
						if operation == "resolve" || operation == "fetch manifest" && strings.HasSuffix(r.URL.Path, art.manifestDigest) && r.Method == http.MethodGet {
							failures.Add(1)
							http.Error(w, "registry failure", tc.status)
							return
						}
						w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
						w.Header().Set("Docker-Content-Digest", art.manifestDigest)
						_, _ = w.Write(art.manifest)
					case strings.HasSuffix(r.URL.Path, art.layerDigest):
						if operation == "fetch layer" {
							failures.Add(1)
							http.Error(w, "registry failure", tc.status)
							return
						}
						_, _ = w.Write(art.layer)
					}
				}))
				t.Cleanup(srv.Close)
				repo := &manifest.OCIRepository{Name: "failure", Namespace: "test", URL: "oci://" + mustURL(t, srv.URL).Host + "/chart",
					Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
					LayerSelector: &sourcev1.OCILayerSelector{Operation: layerOperation}}
				if operation == "list tags" {
					repo.Reference = &sourcev1.OCIRepositoryRef{SemVer: ">=1"}
				}
				root := t.TempDir()
				c, err := sourcecache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				f := &Fetcher{Cache: c}
				t.Cleanup(func() {
					if err := f.Cache.Close(); err != nil {
						t.Error(err)
					}
				})
				client, material, err := f.newRepoClient(ctx, repo, nil)
				if err != nil {
					t.Fatal(err)
				}
				var original error
				if strings.HasSuffix(operation, "body") || operation == "layer header" {
					response := &errcode.ErrorResponse{Method: http.MethodGet, URL: mustURL(t, srv.URL+"/body"), StatusCode: tc.status}
					original = response
					if operation == "neutral body" {
						original = &fetcherr.ResponseError{StatusCode: response.StatusCode, Op: response.Method, Err: response}
					}
					if operation == "canceled body" {
						original = context.Canceled
					}
					client.Client = &http.Client{Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
						resp, err := srv.Client().Transport.RoundTrip(req)
						if err != nil || req.Method != http.MethodGet {
							return resp, err
						}
						manifestBody := operation == "manifest body" && strings.Contains(req.URL.Path, "/manifests/")
						layerBody := operation != "manifest body" && strings.HasSuffix(req.URL.Path, art.layerDigest)
						if manifestBody || layerBody {
							failures.Add(1)
							_ = resp.Body.Close()
							var prefix []byte
							if layerBody && operation != "layer header" {
								prefix = art.layer[:10]
							}
							resp.Body = io.NopCloser(io.MultiReader(bytes.NewReader(prefix), iotest.ErrReader(original)))
							if operation == "canceled body" {
								cancel()
							}
						}
						return resp, nil
					})}
				}
				retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
				_, err = f.fetch(ctx, repo, registryAcquisition{client, material}, retry)
				if failures.Load() != tc.attempts {
					t.Fatalf("producer failures: got %d, want %d: %v", failures.Load(), tc.attempts, err)
				}
				t.Logf("live producer failures=%d; total requests=%d", failures.Load(), requests.Load())
				prefix := operation
				switch operation {
				case "list tags":
					prefix = "semver: list tags:"
				case "manifest body":
					prefix = "fetch manifest:"
				case "layer header":
					prefix = "gzip:"
				case "layer body", "neutral body", "canceled body":
					prefix = ""
				}
				if err == nil || !strings.HasPrefix(err.Error(), ociID(repo)+" "+prefix) || operation != "canceled body" && strings.Contains(err.Error(), "cache:") {
					t.Fatalf("failure identity: %v", err)
				}
				if !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("failure classification: %v", err)
				}
				neutral, ok := errors.AsType[*fetcherr.ResponseError](err)
				if tc.status == http.StatusNotFound {
					if ok || !errors.Is(err, errdef.ErrNotFound) {
						t.Fatalf("ORAS not-found mapping changed: %v", err)
					}
					return
				}
				if operation == "canceled body" {
					if ok || !errors.Is(err, context.Canceled) {
						t.Fatalf("caller cancellation changed: %v", err)
					}
					return
				}
				response, typed := errors.AsType[*errcode.ErrorResponse](err)
				method := http.MethodGet
				if operation == "resolve" {
					method = http.MethodHead
				}
				if !ok || !typed || neutral.StatusCode != tc.status || neutral.Op != method || response.StatusCode != tc.status || response.Method != method || neutral.Error() != neutral.Err.Error() {
					t.Fatalf("lost response facts: neutral=%+v response=%+v error=%v", neutral, response, err)
				}
				if original == nil {
					if !errors.Is(response, neutral.Err) {
						t.Fatalf("original ORAS response changed: %v", err)
					}
				} else if !errors.Is(err, original) {
					t.Fatalf("original body failure changed: %v", err)
				}
				if operation == "neutral body" && !errors.Is(original, neutral) {
					t.Fatalf("neutral body failure wrapped twice: %v", err)
				}
				if !tc.replay {
					return
				}
				terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
				if !ok || !terminal.Attempted {
					t.Fatalf("initial exhaustion: %+v, %v", terminal, err)
				}
				before := requests.Load()
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				f.Cache, err = sourcecache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				if f.Cache == c {
					t.Fatal("reopen reused the closed owner")
				}
				_, err = f.fetch(ctx, repo, registryAcquisition{client, material}, retry)
				if requests.Load() != before || failures.Load() != tc.attempts {
					t.Fatalf("replay fetched: requests=%d, want %d; failures=%d, want %d", requests.Load(), before, failures.Load(), tc.attempts)
				}
				terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
				neutral, neutralOK := errors.AsType[*fetcherr.ResponseError](err)
				response, typed = errors.AsType[*errcode.ErrorResponse](err)
				if !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) || !neutralOK || !typed || neutral.StatusCode != tc.status || neutral.Op != method || response.StatusCode != tc.status || response.Method != method {
					t.Fatalf("replayed response: terminal=%+v neutral=%+v response=%+v error=%v", terminal, neutral, response, err)
				}
				t.Logf("attempts=%d; requests before/after replay=%d/%d; status=%d operation=%s", tc.attempts, before, requests.Load(), neutral.StatusCode, neutral.Op)
			})
		}
	}
}

type registryRoundTripper func(*http.Request) (*http.Response, error)

func (rt registryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}

func TestFetcher_LayerSelectionAndIgnoreFlavor(t *testing.T) {
	for _, name := range []string{"unmatched", "empty", "defaults", "digest selectors"} {
		t.Run(name, func(t *testing.T) {
			art := newFakeOCIArtifact(t, map[string]string{"file.yaml": "x", ".git/config": "git", "image.png": "image"})
			if name == "empty" {
				var m ocispec.Manifest
				if err := json.Unmarshal(art.manifest, &m); err != nil {
					t.Fatal(err)
				}
				m.Layers = nil
				var err error
				art.manifest, err = json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
			}
			srv := startFakeRegistry(t, art.manifest, art.config, art.layer)
			repo := &manifest.OCIRepository{URL: "oci://" + mustURL(t, srv.URL).Host + "/artifact", Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}}
			if name == "unmatched" {
				repo.LayerSelector = &sourcev1.OCILayerSelector{MediaType: "unmatched/type"}
			}
			if name == "digest selectors" {
				repo.Reference = &sourcev1.OCIRepositoryRef{Digest: art.manifestDigest}
			}
			f := &Fetcher{Cache: newSourceCache(t)}
			got, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
			if name == "unmatched" {
				if err == nil {
					t.Fatal("unmatched mediaType accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if name == "empty" {
				entries, err := fs.ReadDir(got.FS, ".")
				if err != nil || len(entries) != 0 {
					t.Fatalf("empty artifact: entries=%v err=%v", entries, err)
				}
				return
			}
			if name == "digest selectors" {
				copyRepo := *repo
				copyRepo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
				copied, err := f.Fetch(t.Context(), &copyRepo, sourcecache.RetryConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if data, err := fs.ReadFile(copied.FS, "layer.tar.gz"); err != nil || !bytes.Equal(data, art.layer) {
					t.Fatalf("copy artifact changed layer bytes: %v", err)
				}
				if copied.Key == got.Key {
					t.Fatalf("different layer selectors reused cache key %q", got.Key)
				}
			}
			for _, name := range []string{".git/config", "image.png"} {
				if _, err := fs.Stat(got.FS, name); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("default ignore retained %s: %v", name, err)
				}
			}
			if got := mustReadFile(t, got.FS, "file.yaml"); got != "x" {
				t.Fatalf("content=%q", got)
			}
		})
	}
}

func TestFetcher_NetworkRoutes(t *testing.T) {
	for _, route := range []string{"registry", "token", "redirect", "tags", "manifest", "layer", "copy", "fallback"} {
		t.Run(route, func(t *testing.T) {
			var network source.Network
			var bypass, requests atomic.Int32
			target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				bypass.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(target.Close)
			cause := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
			tr, _ := source.NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			seedDials := 0
			tr.DialContext = func(context.Context, string, string) (net.Conn, error) { seedDials++; return nil, cause }
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target.URL, nil)
			_, err := (&http.Client{Transport: network.HTTPTransport(tr)}).Do(req)
			first, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !first.Attempted || seedDials != 1 {
				t.Fatalf("seed: %v dials=%d", err, seedDials)
			}
			art := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap\n"})
			registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch {
				case route == "token":
					w.Header().Set("Www-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="registry"`, target.URL))
					w.WriteHeader(http.StatusUnauthorized)
				case route == "redirect",
					route == "tags" && strings.HasSuffix(r.URL.Path, "/tags/list"),
					route == "manifest" && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/"),
					strings.Contains(r.URL.Path, "/blobs/"):
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
				default:
					w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
					w.Header().Set("Docker-Content-Digest", art.manifestDigest)
					w.Header().Set("Content-Length", fmt.Sprint(len(art.manifest)))
					if r.Method != http.MethodHead {
						_, _ = w.Write(art.manifest)
					}
				}
			}))
			t.Cleanup(registry.Close)
			registryURL := registry.URL
			if route == "registry" || route == "fallback" {
				registryURL = target.URL
			}
			repo := &manifest.OCIRepository{Name: "network", Namespace: "test", URL: "oci://" + mustURL(t, registryURL).Host + "/test/chart", Insecure: true,
				Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"}}
			if route == "tags" {
				repo.Reference = &sourcev1.OCIRepositoryRef{SemVer: ">=1"}
			}
			if route == "copy" {
				repo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
			}
			f := &Fetcher{Cache: newSourceCache(t), Network: &network}
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
			if route == "fallback" {
				repo.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
				f.Secrets = func(_, _ string) *manifest.Secret { return nil }
				f.RegistryConfig = filepath.Join(t.TempDir(), "config.json")
				testutil.WriteFileAt(t, f.RegistryConfig, fmt.Sprintf(`{"auths":{%q:{"auth":"dXNlcjpwYXNz"}}}`, mustURL(t, registryURL).Host))
				_, err = f.Fetch(t.Context(), repo, retry)
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryWithRegistryConfig == nil {
					t.Fatalf("fallback: %v", err)
				}
				_, err = missing.RetryWithRegistryConfig(t.Context(), f.RegistryConfig)
			} else {
				_, err = f.Fetch(t.Context(), repo, retry)
			}
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || terminal.Attempted || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) || bypass.Load() != 0 {
				t.Fatalf("route=%s error=%v terminal=%+v bypass=%d", route, err, terminal, bypass.Load())
			}
			want := int32(1)
			if route == "layer" || route == "copy" {
				want = 3
			}
			if route == "manifest" {
				want = 2
			}
			if route == "registry" || route == "fallback" {
				want = 0
			}
			if got := requests.Load(); got != want {
				t.Fatalf("registry requests=%d want=%d", got, want)
			}
		})
	}
}
