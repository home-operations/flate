package helmchart

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func httpRepo(url string) *manifest.HelmRepository {
	return &manifest.HelmRepository{
		Name: "repo", Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: url},
	}
}

func newHTTPFetcher(t *testing.T, r *manifest.HelmRepository) *Fetcher {
	t.Helper()
	return newHTTPFetcherWithSecrets(t, r, nil)
}

func newHTTPFetcherWithSecrets(t *testing.T, r *manifest.HelmRepository, secrets source.SecretGetter) *Fetcher {
	t.Helper()
	layout := cacheroot.New(t.TempDir())
	f, err := New(secrets, func(_, _ string) *manifest.HelmRepository { return r }, nil, source.NewCache(layout), layout)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func TestFetchHTTPChart(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, hits := startHelmRepo(t, chartBytes, chartDigest(chartBytes))
	f := newHTTPFetcher(t, httpRepo(srv.URL))

	art, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art.Kind != manifest.KindHelmChart {
		t.Errorf("art.Kind = %q, want HelmChart", art.Kind)
	}
	if art.Digest != chartDigest(chartBytes) {
		t.Errorf("art.Digest = %q, want %q", art.Digest, chartDigest(chartBytes))
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "chart.tgz")); err != nil {
		t.Errorf("chart.tgz not at LocalPath %s: %v", art.LocalPath, err)
	}

	// A second fetch of the same digest-pinned chart must short-circuit on the
	// content-addressed blob (chartArtifactByDigest -> Cache.BlobByDigest) and
	// NOT re-download — the dedup the *hits counter exists to pin.
	if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err != nil {
		t.Fatalf("second Fetch: %v", err)
	}
	if *hits != 1 {
		t.Errorf("chart downloaded %d times, want 1 (second fetch should dedup via the blob CAS)", *hits)
	}
}

func TestFetchHTTPChart_DigestMismatch(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, _ := startHelmRepo(t, chartBytes, "deadbeefdeadbeef") // index advertises a wrong digest
	f := newHTTPFetcher(t, httpRepo(srv.URL))

	if _, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); err == nil {
		t.Fatal("expected digest-mismatch error, got nil")
	}
}

func TestFetchHTTPChart_ChartNotFound(t *testing.T) {
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	srv, _ := startHelmRepo(t, chartBytes, "")
	f := newHTTPFetcher(t, httpRepo(srv.URL))

	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "9.9.9"))
	if !errors.Is(err, manifest.ErrObjectNotFound) {
		t.Fatalf("err = %v, want ErrObjectNotFound for a missing version", err)
	}
}

func TestFetchHTTPChart_MissingSecret(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	f := newHTTPFetcher(t, r) // Secrets is nil
	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"))
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Fatalf("err = %v, want ErrMissingSecret", err)
	}
}

// TestFetchHTTPChart_CertSecretFailsLoud pins the security scope of
// ErrMissingSecret: certSecretRef carries TLS trust material, so an unwired
// SecretGetter (a config/wiring bug) must fail LOUD, NOT wrap ErrMissingSecret
// — otherwise --allow-missing-secrets would silently soft-skip a TLS failure.
// Mirrors source.ResolveCertSecret. (Contrast TestFetchHTTPChart_MissingSecret,
// where an auth secretRef legitimately soft-skips.)
func TestFetchHTTPChart_CertSecretFailsLoud(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcher(t, r) // Secrets is nil → wiring bug
	_, err := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0"))
	if err == nil {
		t.Fatal("expected a loud certSecretRef error, got nil")
	}
	if errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("certSecretRef nil-getter wrapped ErrMissingSecret (would soft-skip); want loud: %v", err)
	}
}

// TestFetchHTTPChart_CertSecretNotFoundSoftSkips covers the other half: a
// certSecretRef whose Secret is genuinely absent IS the --allow-missing-secrets
// case (cert materialized live, not in git), so it wraps ErrMissingSecret —
// same sentinel git/oci/bucket use via source.ResolveCertSecret.
func TestFetchHTTPChart_CertSecretNotFoundSoftSkips(t *testing.T) {
	r := httpRepo("https://charts.example")
	r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	f := newHTTPFetcherWithSecrets(t, r, func(_, _ string) *manifest.Secret { return nil }) // getter wired, secret absent
	if _, ferr := f.Fetch(context.Background(), helmChart("repo", "app-template", "1.0.0")); !errors.Is(ferr, manifest.ErrMissingSecret) {
		t.Errorf("certSecretRef not-found should soft-skip (ErrMissingSecret); got: %v", ferr)
	}
}

// startHelmRepo serves a single-version index.yaml + the chart tarball.
// Returns the server and a download-hit counter.
func startHelmRepo(t *testing.T, chartBytes []byte, indexDigest string) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/index.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(helmRepoIndex(indexDigest)))
	})
	mux.HandleFunc("/app-template-1.0.0.tgz", func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(chartBytes)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &hits
}

func helmRepoIndex(digest string) string {
	digestLine := ""
	if digest != "" {
		digestLine = fmt.Sprintf("      digest: %s\n", digest)
	}
	return fmt.Sprintf(`apiVersion: v1
entries:
  app-template:
    - name: app-template
      version: 1.0.0
%s      urls:
        - app-template-1.0.0.tgz
`, digestLine)
}

func chartDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func buildChartTarGz(t *testing.T, name, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	files := map[string]string{
		name + "/Chart.yaml":             "apiVersion: v2\nname: " + name + "\nversion: " + version + "\n",
		name + "/templates/_helpers.tpl": "",
	}
	for path, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: path, Typeflag: tar.TypeReg, Size: int64(len(body)), Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, body); err != nil {
			t.Fatal(err)
		}
	}
	_ = tw.Close()
	_ = gw.Close()
	return buf.Bytes()
}
