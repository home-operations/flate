package oci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source"
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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	manifestBytes := mustManifestJSON(t, configBytes, layerBytes,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.flux.content.v1.tar+gzip",
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
	got, err := fs.ReadFile(art.FS, "gotk-components.yaml") //nolint:gosec // inside t.TempDir-rooted slot
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
	manifestBytes := mustManifestJSON(t, configBytes, layerBytes,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.flux.content.v1.tar+gzip",
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
	got, err := fs.ReadFile(art.FS, "blobs/should-survive.yaml") //nolint:gosec // inside t.TempDir-rooted slot
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

func TestFetcher_MutableTagRefreshesCache(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := startMutableFakeRegistry(t, v1, v2)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "mutable",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/mutable", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}

	art1, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	if got := mustReadFile(t, art1.FS, "app.yaml"); !strings.Contains(got, "v1") {
		t.Fatalf("first fetch content = %q", got)
	}

	setArtifact(1)
	art2, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second Fetch: %v", err)
	}
	if art2.Digest == art1.Digest {
		t.Fatalf("mutable tag reused stale digest %s", art2.Digest)
	}
	if art2.Key != art1.Key || art2.Revision == art1.Revision {
		t.Fatalf("refresh identity: key=%q/%q revision=%q/%q", art1.Key, art2.Key, art1.Revision, art2.Revision)
	}
	if got := mustReadFile(t, art2.FS, "app.yaml"); !strings.Contains(got, "v2") {
		t.Fatalf("second fetch content = %q", got)
	}
}

func TestFetcher_MutableTagUsesFreshIntervalResolveCache(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := startMutableFakeRegistry(t, v1, v2)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "mutable",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/mutable", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Interval:  metav1.Duration{Duration: time.Hour},
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}

	art1, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	setArtifact(1)
	srv.Close()
	art2, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second Fetch should use fresh resolve cache without registry: %v", err)
	}
	if art2.Digest != art1.Digest {
		t.Fatalf("fresh interval cache should reuse digest %s, got %s", art1.Digest, art2.Digest)
	}
	if got := mustReadFile(t, art2.FS, "app.yaml"); !strings.Contains(got, "v1") {
		t.Fatalf("fresh interval cache content = %q", got)
	}
}

func TestFetcher_TagCacheHitSkipsSecondPull(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	srv, failBlobs := startFakeRegistryWithBlobGate(t, v1)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "stable-tag",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/stable-tag", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Interval:  metav1.Duration{Duration: time.Hour},
		Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
	}

	art1, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	failBlobs()
	art2, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second Fetch should use cached tag digest without pulling blobs: %v", err)
	}
	if art2.Digest != art1.Digest {
		t.Fatalf("digest changed on stable tag cache hit: %s -> %s", art1.Digest, art2.Digest)
	}
	if got := mustReadFile(t, art2.FS, "app.yaml"); !strings.Contains(got, "v1") {
		t.Fatalf("cached tag content = %q", got)
	}
}

func TestFetcher_MutableRefreshFailureKeepsPreviousSlot(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	srv, _ := startMutableFakeRegistry(t, v1)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "mutable",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/mutable", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}

	art, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	srv.Close()
	if _, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{}); err == nil {
		t.Fatal("expected refresh to fail after registry shutdown")
	}
	if got := mustReadFile(t, art.FS, "app.yaml"); !strings.Contains(got, "v1") {
		t.Fatalf("failed mutable refresh damaged previous slot; app.yaml = %q", got)
	}
}

func TestFetcher_DigestRefUsesCachedSlot(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	srv := startFakeRegistry(t, v1.manifest, v1.config, v1.layer)

	f := &Fetcher{Cache: newSourceCache(t)}
	repo := &manifest.OCIRepository{
		Name:      "pinned",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/pinned", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest},
	}
	if _, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	srv.Close()
	if _, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{}); err != nil {
		t.Fatalf("digest-pinned cache hit should not need registry: %v", err)
	}
}

func TestFetcher_DigestCacheKeyIncludesLayerSelector(t *testing.T) {
	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	srv := startFakeRegistry(t, v1.manifest, v1.config, v1.layer)
	f := &Fetcher{Cache: newSourceCache(t)}

	copyRepo := &manifest.OCIRepository{
		Name:      "copy",
		Namespace: "test",
		URL:       fmt.Sprintf("oci://%s/app", mustURL(t, srv.URL).Host),
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest},
		LayerSelector: &sourcev1.OCILayerSelector{
			Operation: sourcev1.OCILayerCopy,
		},
	}
	copyArt, err := f.Fetch(t.Context(), copyRepo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("copy Fetch: %v", err)
	}
	if got, err := fs.ReadFile(copyArt.FS, "layer.tar.gz"); err != nil || !bytes.Equal(got, v1.layer) {
		t.Fatalf("copy artifact changed layer bytes: %v", err)
	}

	extractRepo := &manifest.OCIRepository{
		Name:      "extract",
		Namespace: "test",
		URL:       copyRepo.URL,
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest},
	}
	extractArt, err := f.Fetch(t.Context(), extractRepo, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("extract Fetch: %v", err)
	}
	if extractArt.Key == copyArt.Key {
		t.Fatalf("different layer selectors reused cache slot %s", extractArt.Key)
	}
	if got := mustReadFile(t, extractArt.FS, "app.yaml"); !strings.Contains(got, "v1") {
		t.Fatalf("extract artifact content = %q", got)
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

func startFakeRegistryWithBlobGate(t *testing.T, art fakeOCIArtifact) (*httptest.Server, func()) {
	t.Helper()
	var (
		mu         sync.RWMutex
		blobsFail  bool
		configBody = art.config
		layerBody  = art.layer
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", art.manifestDigest)
			_, _ = w.Write(art.manifest)
		case strings.Contains(r.URL.Path, "/blobs/"):
			mu.RLock()
			fail := blobsFail
			mu.RUnlock()
			if fail {
				http.Error(w, "blob pulls disabled", http.StatusServiceUnavailable)
				return
			}
			_, digest, _ := strings.CutLast(r.URL.Path, "/")
			switch digest {
			case art.configDigest:
				_, _ = w.Write(configBody)
			case art.layerDigest:
				_, _ = w.Write(layerBody)
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	failBlobs := func() {
		mu.Lock()
		defer mu.Unlock()
		blobsFail = true
	}
	return srv, failBlobs
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
	manifest := mustManifestJSON(t, config, layer,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.flux.content.v1.tar+gzip",
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

// mustManifestJSON builds an OCI image manifest pointing at the given
// config + single layer with no title annotations — the shape that
// surfaces the regression this test covers.
func mustManifestJSON(t *testing.T, configBytes, layerBytes []byte, configMT, layerMT string) []byte {
	t.Helper()
	m := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: configMT,
			Digest:    digest.Digest(sha256Digest(configBytes)),
			Size:      int64(len(configBytes)),
		},
		Layers: []ocispec.Descriptor{{
			MediaType: layerMT,
			Digest:    digest.Digest(sha256Digest(layerBytes)),
			Size:      int64(len(layerBytes)),
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
	b, err := fs.ReadFile(input, path) //nolint:gosec // test-owned path
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
		tlsCfg, err := f.resolveTLS(repo)
		if err != nil {
			t.Fatal(err)
		}
		client, err := f.newRepoClient(repo, nil, tlsCfg, nil)
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
				got, err := fetcher.fetch(t.Context(), &current, client, sourcecache.RetryConfig{})
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
		t.Run(backing, func(t *testing.T) {
			honest := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
			tampered := mustTarGz(t, map[string]string{"app.yaml": "version: v2\n"})
			if len(tampered) != len(honest.layer) {
				t.Fatal("fixture lengths differ")
			}
			var repaired atomic.Bool
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "/manifests/"):
					w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
					w.Header().Set("Docker-Content-Digest", honest.manifestDigest)
					_, _ = w.Write(honest.manifest)
				case strings.HasSuffix(r.URL.Path, honest.layerDigest):
					if repaired.Load() {
						_, _ = w.Write(honest.layer)
					} else {
						_, _ = w.Write(tampered)
					}
				case strings.HasSuffix(r.URL.Path, honest.configDigest):
					_, _ = w.Write(honest.config)
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
			if _, err := f.Fetch(t.Context(), repo, sourcecache.RetryConfig{}); err == nil {
				t.Fatal("tampered layer accepted")
			}
			repaired.Store(true)
			if backing == "disk" {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				c, err = sourcecache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				f.Cache = c
			}
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

func TestFetcher_FailureSourceAndOperation(t *testing.T) {
	for _, operation := range []string{"resolve", "fetch manifest", "fetch layer"} {
		t.Run(operation, func(t *testing.T) {
			art := newFakeOCIArtifact(t, map[string]string{"file": "x"})
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "/manifests/"):
					if operation == "resolve" || operation == "fetch manifest" && strings.HasSuffix(r.URL.Path, art.manifestDigest) && r.Method == http.MethodGet {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
					w.Header().Set("Docker-Content-Digest", art.manifestDigest)
					_, _ = w.Write(art.manifest)
				case strings.HasSuffix(r.URL.Path, art.layerDigest):
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(srv.Close)
			c, err := sourcecache.New(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			repo := &manifest.OCIRepository{Name: "failure", Namespace: "test", URL: "oci://" + mustURL(t, srv.URL).Host + "/chart",
				Insecure: true, Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"}}
			_, err = (&Fetcher{Cache: c}).Fetch(t.Context(), repo, sourcecache.RetryConfig{})
			if err == nil || !strings.HasPrefix(err.Error(), ociID(repo)+" "+operation) || strings.Contains(err.Error(), "cache:") {
				t.Fatalf("failure identity: %v", err)
			}
			if !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("failure classification: %v", err)
			}
		})
	}
}

func TestFetcher_LayerSelectionAndIgnoreFlavor(t *testing.T) {
	for _, name := range []string{"unmatched", "empty", "defaults"} {
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
			got, err := (&Fetcher{Cache: newSourceCache(t)}).Fetch(t.Context(), repo, sourcecache.RetryConfig{})
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
	for _, route := range []string{"registry", "token", "redirect", "layer", "fallback"} {
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
				case route == "redirect" || strings.Contains(r.URL.Path, "/blobs/"):
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
			if route == "layer" {
				want = 3
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
