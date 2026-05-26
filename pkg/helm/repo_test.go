package helm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"helm.sh/helm/v4/pkg/getter"
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

	c, err := NewClient(t.TempDir(), t.TempDir())
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

	c, err := NewClient(t.TempDir(), t.TempDir())
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
