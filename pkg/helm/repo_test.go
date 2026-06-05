package helm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/getter"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

const indexYAMLFixture = `apiVersion: v1
entries:
  app-template:
    - name: app-template
      version: 5.0.0
      urls:
        - app-template-5.0.0.tgz
`

// TestFetchIndex_CachesAcrossCalls confirms that the index.yaml is
// downloaded once across N calls with the same cache key. Two HRs
// pointing at the same HelmRepository previously each downloaded
// the full index — now the second call hits the in-memory cache.
func TestFetchIndex_CachesAcrossCalls(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(indexYAMLFixture))
	}))
	defer srv.Close()

	c, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	url := srv.URL + "/index.yaml"
	key := "default/test-repo@" + url

	idx1, err := c.fetchIndex(context.Background(), key, url, []getter.Option{})
	if err != nil {
		t.Fatalf("fetchIndex 1: %v", err)
	}
	idx2, err := c.fetchIndex(context.Background(), key, url, []getter.Option{})
	if err != nil {
		t.Fatalf("fetchIndex 2: %v", err)
	}
	if idx1 != idx2 {
		t.Errorf("expected same *IndexFile pointer on cache hit; got distinct")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("expected 1 HTTP fetch, got %d", got)
	}
}

// TestFetchIndex_DistinctKeysFetchSeparately: two HelmRepository CRs
// with different (ns, name) keys are kept separate even if they
// happen to point at the same URL — the cache is keyed by CR
// identity so private feeds with different credentials don't share
// a cached index that was fetched under another auth context.
func TestFetchIndex_DistinctKeysFetchSeparately(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(indexYAMLFixture))
	}))
	defer srv.Close()

	c, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	url := srv.URL + "/index.yaml"
	if _, err := c.fetchIndex(context.Background(), "team-a/repo@"+url, url, nil); err != nil {
		t.Fatalf("fetchIndex A: %v", err)
	}
	if _, err := c.fetchIndex(context.Background(), "team-b/repo@"+url, url, nil); err != nil {
		t.Fatalf("fetchIndex B: %v", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected 2 HTTP fetches (one per CR identity), got %d", got)
	}
}

func TestLocateHelmRepoChart_IndexDigestInvalidatesPersistentCache(t *testing.T) {
	charts := [][]byte{
		buildChartTarGz(t, "app-template", "1.0.0"),
		buildChartTarGz(t, "app-template", "1.0.1"),
	}
	srv, setChart, hits := startMutableHelmRepo(t, true, charts...)
	cacheRoot := t.TempDir()

	c1, hr1 := newHelmRepoClient(t, cacheRoot, srv.URL)
	if _, err := c1.locateHelmRepoChart(context.Background(), hr1); err != nil {
		t.Fatalf("first locateHelmRepoChart: %v", err)
	}
	setChart(1)
	c2, hr2 := newHelmRepoClient(t, cacheRoot, srv.URL)
	if _, err := c2.locateHelmRepoChart(context.Background(), hr2); err != nil {
		t.Fatalf("second locateHelmRepoChart: %v", err)
	}
	if got := hits(); got != 2 {
		t.Fatalf("chart downloads = %d, want 2 after index digest changed", got)
	}
}

func TestLocateHelmRepoChart_NoDigestDoesNotPersistMutableVersion(t *testing.T) {
	charts := [][]byte{
		buildChartTarGz(t, "app-template", "1.0.0"),
		buildChartTarGz(t, "app-template", "1.0.1"),
	}
	srv, setChart, hits := startMutableHelmRepo(t, false, charts...)
	cacheRoot := t.TempDir()

	c1, hr1 := newHelmRepoClient(t, cacheRoot, srv.URL)
	if _, err := c1.locateHelmRepoChart(context.Background(), hr1); err != nil {
		t.Fatalf("first locateHelmRepoChart: %v", err)
	}
	setChart(1)
	c2, hr2 := newHelmRepoClient(t, cacheRoot, srv.URL)
	if _, err := c2.locateHelmRepoChart(context.Background(), hr2); err != nil {
		t.Fatalf("second locateHelmRepoChart: %v", err)
	}
	if got := hits(); got != 2 {
		t.Fatalf("chart downloads = %d, want 2 when index has no digest", got)
	}
}

func TestSynthesizeOCIRepository(t *testing.T) {
	r := &manifest.HelmRepository{
		Name:      "truecharts",
		Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{
			URL:           "oci://oci.trueforge.org/truecharts/",
			Type:          manifest.RepoTypeOCI,
			Provider:      sourcev1.AmazonOCIProvider,
			SecretRef:     &manifest.LocalObjectReference{Name: "regcred"},
			CertSecretRef: &manifest.LocalObjectReference{Name: "tls"},
			Insecure:      true,
		},
	}
	syn := SynthesizeOCIRepository(r, "kromgo", "3.0.0")

	if got, want := syn.URL, "oci://oci.trueforge.org/truecharts/kromgo"; got != want {
		t.Errorf("URL = %q, want %q", got, want)
	}
	if syn.Namespace != "flux-system" {
		t.Errorf("Namespace = %q, want flux-system", syn.Namespace)
	}
	if syn.Provider != sourcev1.AmazonOCIProvider {
		t.Errorf("Provider = %q, want %q", syn.Provider, sourcev1.AmazonOCIProvider)
	}
	if syn.Reference == nil || syn.Reference.Tag != "3.0.0" || syn.Reference.Digest != "" {
		t.Errorf("Reference = %+v, want tag 3.0.0", syn.Reference)
	}
	// HelmRepository auth / TLS / insecure must be lifted so the OCI
	// fetcher honors them on the source-controller pull.
	if syn.SecretRef == nil || syn.SecretRef.Name != "regcred" {
		t.Errorf("SecretRef = %+v, want regcred", syn.SecretRef)
	}
	if syn.CertSecretRef == nil || syn.CertSecretRef.Name != "tls" {
		t.Errorf("CertSecretRef = %+v, want tls", syn.CertSecretRef)
	}
	if !syn.Insecure {
		t.Error("Insecure not lifted")
	}
	// Name is deterministic and hash-suffixed off (url, version).
	sum := sha256.Sum256([]byte("oci://oci.trueforge.org/truecharts/kromgo@3.0.0"))
	if want := "truecharts-kromgo-" + hex.EncodeToString(sum[:])[:7]; syn.Name != want {
		t.Errorf("Name = %q, want %q", syn.Name, want)
	}
}

// TestSynthesizeOCIRepository_DigestAndVersionIdentity covers the digest-ref
// branch and the version-disambiguation guarantee: two HelmReleases pulling
// different versions of the same chart from the same repo must get distinct
// Store identities so they don't clobber each other's artifact.
func TestSynthesizeOCIRepository_DigestAndVersionIdentity(t *testing.T) {
	r := &manifest.HelmRepository{
		Name: "tc", Namespace: "fs",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "oci://reg/tc", Type: manifest.RepoTypeOCI},
	}
	if d := SynthesizeOCIRepository(r, "app", "sha256:abc"); d.Reference == nil ||
		d.Reference.Digest != "sha256:abc" || d.Reference.Tag != "" {
		t.Errorf("digest version → Reference = %+v, want digest sha256:abc", d.Reference)
	}
	v1 := SynthesizeOCIRepository(r, "app", "1.0.0")
	v2 := SynthesizeOCIRepository(r, "app", "2.0.0")
	if v1.Name == v2.Name {
		t.Errorf("distinct versions collided on Store name %q", v1.Name)
	}
	// Empty version (chart version omitted on the HR) → no Reference.
	if v := SynthesizeOCIRepository(r, "app", ""); v.Reference != nil {
		t.Errorf("empty version → Reference = %+v, want nil", v.Reference)
	}
	// A trailing slash on the repo URL is normalized, so it yields the same
	// chart URL and Store identity as the slashless form.
	rSlash := &manifest.HelmRepository{
		Name: "tc", Namespace: "fs",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{URL: "oci://reg/tc/", Type: manifest.RepoTypeOCI},
	}
	if a, b := v1, SynthesizeOCIRepository(rSlash, "app", "1.0.0"); a.URL != b.URL || a.Name != b.Name {
		t.Errorf("trailing-slash mismatch: %q/%q vs %q/%q", a.URL, a.Name, b.URL, b.Name)
	}
}

func TestOCIPullRef(t *testing.T) {
	const repo = "oci://ghcr.io/bjw-s-labs/helm/app-template"
	for _, tc := range []struct {
		name    string
		version string
		want    string
	}{
		{"empty version", "", repo},
		{"semver tag", "1.2.3", repo + ":1.2.3"},
		{"named tag", "latest", repo + ":latest"},
		{"sha256 digest", "sha256:70a7cb6766eb468068c2c1700c8450253070dc671a9fbbd1a6346a66545e2b2b",
			repo + "@sha256:70a7cb6766eb468068c2c1700c8450253070dc671a9fbbd1a6346a66545e2b2b"},
		{"sha512 digest", "sha512:deadbeef", repo + "@sha512:deadbeef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ociPullRef(repo, tc.version); got != tc.want {
				t.Errorf("ociPullRef(%q, %q) = %q, want %q", repo, tc.version, got, tc.want)
			}
		})
	}
}

func startMutableHelmRepo(t *testing.T, includeDigest bool, charts ...[]byte) (*httptest.Server, func(int32), func() int32) {
	t.Helper()
	var current atomic.Int32
	var hits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/index.yaml", func(w http.ResponseWriter, _ *http.Request) {
		idx := int(current.Load())
		digest := ""
		if includeDigest {
			digest = chartDigest(charts[idx])
		}
		w.Header().Set("Content-Type", "application/x-yaml")
		_, _ = w.Write([]byte(helmRepoIndex(digest)))
	})
	mux.HandleFunc("/app-template-1.0.0.tgz", func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		idx := int(current.Load())
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(charts[idx])
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, func(i int32) { current.Store(i) }, hits.Load
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

func chartDigest(chart []byte) string {
	sum := sha256.Sum256(chart)
	return hex.EncodeToString(sum[:])
}

func newHelmRepoClient(t *testing.T, cacheRoot, repoURL string) (*Client, *manifest.HelmRelease) {
	t.Helper()
	c, err := NewClient(cacheroot.New(cacheRoot))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	st.AddObject(&manifest.HelmRepository{
		Name:      "repo",
		Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{
			URL: repoURL,
		},
	})
	c.SetSourceResolver(NewStoreSourceResolver(st))
	return c, &manifest.HelmRelease{
		Name:      "app",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "app-template",
			Version:       "1.0.0",
			RepoName:      "repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindHelmRepository,
		},
	}
}
