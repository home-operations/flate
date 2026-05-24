package oci

import (
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
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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

	f := &Fetcher{Cache: source.NewCache(t.TempDir())}
	repo := &manifest.OCIRepository{
		Name:      "flux-manifests",
		Namespace: "flux-system",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL: fmt.Sprintf("oci://%s/fluxcd/flux-manifests", mustURL(t, srv.URL).Host),
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

	// Observable proof that the layer made it to disk and got extracted.
	got, err := os.ReadFile(filepath.Join(art.LocalPath, "gotk-components.yaml")) //nolint:gosec // inside t.TempDir-rooted slot
	if err != nil {
		t.Fatalf("expected extracted gotk-components.yaml under %s: %v\nslot contents: %v",
			art.LocalPath, err, slotEntries(t, art.LocalPath))
	}
	if want := "kind: ConfigMap\n"; string(got) != want {
		t.Errorf("gotk-components.yaml = %q, want %q", got, want)
	}

	// OCI layout artifacts should be wiped after extract so kustomize /
	// downstream consumers see only the artifact's own files.
	for _, name := range ociLayoutArtifacts {
		if _, err := os.Stat(filepath.Join(art.LocalPath, name)); !os.IsNotExist(err) {
			t.Errorf("leftover OCI layout artifact in slot: %s (err: %v)", name, err)
		}
	}
}

// TestFetcher_PartialSlotInvalidated guards against a corrupt cache
// hit: a prior fetch that crashed AFTER oras.Copy populated `blobs/`
// but BEFORE writeCachedDigest finalized the `.flate-digest` sentinel
// would otherwise be served back as a valid artifact (cache.Slot
// reports non-empty dir → exists=true). Treat the missing sentinel
// as an invalidated slot and re-pull.
func TestFetcher_PartialSlotInvalidated(t *testing.T) {
	t.Parallel()

	layerBytes := mustTarGz(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: x\nversion: 0.1.0\n"})
	configBytes := []byte(`{}`)
	manifestBytes := mustManifestJSON(t, configBytes, layerBytes,
		"application/vnd.cncf.flux.config.v1+json",
		"application/vnd.cncf.helm.chart.content.v1.tar+gzip",
	)
	srv := startFakeRegistry(t, manifestBytes, configBytes, layerBytes)

	cache := source.NewCache(t.TempDir())
	f := &Fetcher{Cache: cache}
	repo := &manifest.OCIRepository{
		Name:      "partial",
		Namespace: "test",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:       fmt.Sprintf("oci://%s/partial", mustURL(t, srv.URL).Host),
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
		},
	}

	// First fetch populates the slot fully.
	art, err := f.Fetch(t.Context(), repo)
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	digestPath := filepath.Join(art.LocalPath, ".flate-digest")
	if _, err := os.Stat(digestPath); err != nil {
		t.Fatalf("first fetch did not produce .flate-digest: %v", err)
	}

	// Simulate the crash: remove the sentinel but leave stray content
	// (a leftover dir is closer to the real crash shape than an empty
	// slot, which Slot() would treat as fresh).
	if err := os.Remove(digestPath); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(art.LocalPath, "blobs", "sha256"), 0o750); err != nil {
		t.Fatal(err)
	}

	// Second fetch must NOT return the corrupt slot. With the fix it
	// resets and re-pulls; the .flate-digest comes back.
	art2, err := f.Fetch(t.Context(), repo)
	if err != nil {
		t.Fatalf("second Fetch (after partial-slot reset): %v", err)
	}
	if _, err := os.Stat(filepath.Join(art2.LocalPath, ".flate-digest")); err != nil {
		t.Errorf("second fetch did not re-populate .flate-digest after partial-slot reset: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art2.LocalPath, "Chart.yaml")); err != nil {
		t.Errorf("second fetch did not re-extract Chart.yaml: %v", err)
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

	f := &Fetcher{Cache: source.NewCache(t.TempDir())}
	repo := &manifest.OCIRepository{
		Name:      "collider",
		Namespace: "test",
		OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			URL:       fmt.Sprintf("oci://%s/collider", mustURL(t, srv.URL).Host),
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Tag: "v1"},
		},
	}

	art, err := f.Fetch(t.Context(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// User's blobs/should-survive.yaml must be intact, not wiped by
	// the OCI layout cleanup.
	got, err := os.ReadFile(filepath.Join(art.LocalPath, "blobs", "should-survive.yaml")) //nolint:gosec // inside t.TempDir-rooted slot
	if err != nil {
		t.Fatalf("user's blobs/should-survive.yaml was wiped by cleanup: %v", err)
	}
	if !strings.Contains(string(got), "name: survives") {
		t.Errorf("survives.yaml content lost: %q", got)
	}
	// User's index.json + oci-layout files must also survive.
	for _, name := range []string{"index.json", "oci-layout", "kustomization.yaml"} {
		if _, err := os.Stat(filepath.Join(art.LocalPath, name)); err != nil {
			t.Errorf("user's %s was wiped: %v", name, err)
		}
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
// config + single layer with no title annotations — the shape that
// surfaces the regression this test covers.
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
