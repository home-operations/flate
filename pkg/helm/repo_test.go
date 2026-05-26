package helm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

func TestPullHelmRepoOCI_PreservesProvider(t *testing.T) {
	c, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	wantErr := errors.New("stop")
	var pulledFor *manifest.OCIRepository
	c.SetOCIPuller(stubPuller{
		fetch: func(_ context.Context, r *manifest.OCIRepository) (*store.SourceArtifact, error) {
			pulledFor = r
			return nil, wantErr
		},
	})

	_, err = c.pullHelmRepoOCI(context.Background(), &manifest.HelmRepository{
		Name:      "repo",
		Namespace: "flux-system",
		HelmRepositorySpec: sourcev1.HelmRepositorySpec{
			URL:      "oci://example.com/charts",
			Type:     manifest.RepoTypeOCI,
			Provider: sourcev1.AmazonOCIProvider,
		},
	}, &manifest.HelmRelease{
		Name:      "app",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name:    "app-template",
			Version: "1.0.0",
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("pullHelmRepoOCI err = %v, want %v", err, wantErr)
	}
	if pulledFor == nil {
		t.Fatal("puller was not invoked")
	}
	if pulledFor.Provider != sourcev1.AmazonOCIProvider {
		t.Fatalf("synthetic OCIRepository provider = %q, want %q", pulledFor.Provider, sourcev1.AmazonOCIProvider)
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
