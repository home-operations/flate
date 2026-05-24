package oci

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// TestFetcher_ExtractsLayerWithoutTitleAnnotation regresses the silent
// no-op that Zariel/home-ops hit on flux-manifests: every blob in a
// Flux OCIRepository artifact lacks `org.opencontainers.image.title`,
// orasfile's default in-memory fallback swallowed them, the slot was
// left holding only `.flate-digest`, and `kustomize build` rendered
// zero manifests rather than failing. The flat-layout fallback (see
// fallback.go) writes the manifest + config + layer to `slot/<hex>`,
// at which point applyLayerSelector finds them and extracts the
// content layer.
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
	hostport := mustURL(t, srv.URL).Host

	f := &Fetcher{Cache: source.NewCache(t.TempDir())}
	repo := &manifest.OCIRepository{
		Name:      "flux-manifests",
		Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL: fmt.Sprintf("oci://%s/fluxcd/flux-manifests", hostport),
			// httptest.NewTLSServer issues a self-signed cert; flate
			// maps spec.insecure to TLS InsecureSkipVerify.
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Tag: "v2.8.8"},
		},
	}

	art, err := f.Fetch(t.Context(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art == nil || art.LocalPath == "" {
		t.Fatal("Fetch returned no artifact")
	}

	// The regression target: pre-fix the slot held only .flate-digest
	// because the layer blob went to memory; the extracted file is the
	// observable proof that fallback storage now writes to disk.
	got, err := os.ReadFile(filepath.Join(art.LocalPath, "gotk-components.yaml")) //nolint:gosec // inside t.TempDir-rooted slot
	if err != nil {
		t.Fatalf("expected extracted gotk-components.yaml under %s: %v\nslot contents: %v",
			art.LocalPath, err, slotEntries(t, art.LocalPath))
	}
	if want := "kind: ConfigMap\n"; string(got) != want {
		t.Errorf("gotk-components.yaml = %q, want %q", got, want)
	}
}

// TestFlatFallbackStorage exercises the storage contract directly so
// regressions in Push / Fetch / Exists don't depend on the full
// httptest-registry round-trip catching them.
func TestFlatFallbackStorage(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	s := &flatFallbackStorage{root: root}
	payload := []byte("hello flate")
	desc := ocispec.Descriptor{
		MediaType: "application/vnd.test",
		Digest:    digest.Digest(sha256Digest(payload)),
		Size:      int64(len(payload)),
	}

	// Exists is false before Push.
	if ok, err := s.Exists(t.Context(), desc); err != nil || ok {
		t.Fatalf("Exists before Push = (%v, %v), want (false, nil)", ok, err)
	}

	if err := s.Push(t.Context(), desc, strings.NewReader(string(payload))); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Push lands at <root>/<hex> — applyLayerSelector / cleanupBlobs
	// both rely on this layout.
	_, hexDigest, _ := strings.Cut(desc.Digest.String(), ":")
	if _, err := os.Stat(filepath.Join(root, hexDigest)); err != nil {
		t.Fatalf("blob not at <root>/<hex>: %v", err)
	}

	if ok, err := s.Exists(t.Context(), desc); err != nil || !ok {
		t.Fatalf("Exists after Push = (%v, %v), want (true, nil)", ok, err)
	}

	rc, err := s.Fetch(t.Context(), desc)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("Fetch = %q, want %q", got, payload)
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
			switch r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:] {
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

// mustManifestJSON builds an OCI image manifest pointing at the given
// config + single layer (no title annotations — that's the whole
// point of the regression).
func mustManifestJSON(t *testing.T, configBytes, layerBytes []byte, configMT, layerMT string) []byte {
	t.Helper()
	m := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
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

func slotEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []string{"<unreadable: " + err.Error() + ">"}
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

