package oci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// TestFetcher_ExtractsLayerWithoutTitleAnnotation regresses the silent
// no-op that Zariel/home-ops hit on flux-manifests: orasfile's default
// fallback storage is in-memory, so blobs without an
// `org.opencontainers.image.title` annotation (the shape of every Flux
// `vnd.cncf.flux.content.v1.tar+gzip` artifact, plus its config + the
// image manifest itself) never landed on disk. applyLayerSelector then
// silently returned because the manifest file wasn't present, leaving
// the slot empty — `kustomize build` then rendered zero manifests
// rather than failing. The fix wires a flat-layout fallback so the
// manifest + config + layer blobs all land at `slot/<hex>`.
func TestFetcher_ExtractsLayerWithoutTitleAnnotation(t *testing.T) {
	// Build a single-file gzipped tarball as the "content" layer.
	// Reuses mustTarGz from layer_test.go.
	layerBytes := mustTarGz(t, map[string]string{
		"gotk-components.yaml": "kind: ConfigMap\n",
	})
	layerDigest := sha256Digest(layerBytes)

	configBytes := []byte(`{"created":"2026-01-01T00:00:00Z"}`)
	configDigest := sha256Digest(configBytes)

	manifestObj := ocispec.Manifest{
		Versioned: ocispec.Manifest{}.Versioned,
		MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: "application/vnd.cncf.flux.config.v1+json",
			Digest:    digest.Digest(configDigest),
			Size:      int64(len(configBytes)),
		},
		Layers: []ocispec.Descriptor{
			{
				MediaType: "application/vnd.cncf.flux.content.v1.tar+gzip",
				Digest:    digest.Digest(layerDigest),
				Size:      int64(len(layerBytes)),
			},
		},
	}
	manifestObj.SchemaVersion = 2
	manifestBytes, err := json.Marshal(manifestObj)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256Digest(manifestBytes)

	// Tiny OCI Distribution API server: responds to manifests by tag or
	// digest, and to blobs by digest. Enough to satisfy oras.Copy.
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/" || r.URL.Path == "/v2":
			// Distribution v2 probe.
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(manifestBytes)
		case strings.Contains(r.URL.Path, "/blobs/"):
			parts := strings.Split(r.URL.Path, "/")
			d := parts[len(parts)-1]
			switch d {
			case configDigest:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(configBytes)
			case layerDigest:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(layerBytes)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	// TLS server: flate's spec.insecure maps to TLS InsecureSkipVerify
	// (it doesn't downshift to PlainHTTP), so the test server has to
	// speak TLS for the fetcher's standard credential path to apply.
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	hostport := mustURL(t, srv.URL).Host

	f := &Fetcher{Cache: source.NewCache(t.TempDir())}
	repo := &manifest.OCIRepository{
		Name:      "flux-manifests",
		Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:      fmt.Sprintf("oci://%s/fluxcd/flux-manifests", hostport),
			// httptest.NewTLSServer issues a self-signed cert; spec.insecure
			// maps to TLS InsecureSkipVerify.
			Insecure: true,
			Reference: &sourcev1.OCIRepositoryRef{
				Tag: "v2.8.8",
			},
		},
	}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art == nil || art.LocalPath == "" {
		t.Fatal("Fetch returned no artifact")
	}

	// Slot must contain the extracted tarball entries, not just the
	// `.flate-digest` sentinel — that's exactly the regression: pre-fix
	// the slot had only the sentinel because the blobs were dropped in
	// memory.
	extracted := filepath.Join(art.LocalPath, "gotk-components.yaml")
	got, err := os.ReadFile(extracted) //nolint:gosec // inside t.TempDir-rooted slot
	if err != nil {
		entries, _ := os.ReadDir(art.LocalPath)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected extracted gotk-components.yaml under %s; slot contents: %v; err: %v",
			art.LocalPath, names, err)
	}
	if want := "kind: ConfigMap\n"; string(got) != want {
		t.Errorf("gotk-components.yaml = %q, want %q", got, want)
	}
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
