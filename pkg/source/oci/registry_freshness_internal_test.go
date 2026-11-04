package oci

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherFreshnessCase(t *testing.T, tt *struct {
	name        string
	interval    time.Duration
	pinned      bool
	mixed       bool
	advance     bool
	offline     bool
	failRefresh bool
	blockBlobs  bool
},
) {
	t.Helper()

	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := newMutableFakeRegistry(t, v1, v2)
	var resolves, pulls atomic.Int32
	var blockBlobs atomic.Bool
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherFreshnessCaseCallback(
			w,
			r,
			testFetcherFreshnessCaseCallbackInputs{
				resolves:   &resolves,
				pulls:      &pulls,
				blockBlobs: &blockBlobs,
				handler:    &handler,
			},
		)
	},
	)
	srv.StartTLS()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name: "freshness", Namespace: "test",
		URL: "oci://" + mustURL(t, srv.URL).Host + "/app", Insecure: true,
		Interval:  &metav1.Duration{Duration: time.Hour},
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	if tt.pinned {
		repo.Reference = &sourcev1.OCIRepositoryRef{Digest: v1.manifestDigest}
		if tt.mixed {
			repo.Reference.Tag = "latest"
			repo.Reference.SemVer = "1.x"
		}
	}
	first, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: tt.interval},
	)
	assert.NoError(t, err)

	{
		got := mustReadFile(t, first.FS, "app.yaml")
		assert.FatalIf(t, got != "version: v1\n", "first content = %q", got)
	}
	beforeResolve, beforePull := resolves.Load(), pulls.Load()
	assert.FatalIf(
		t,
		beforeResolve == 0 || beforePull == 0,
		"first fetch: resolves=%d pulls=%d",
		beforeResolve,
		beforePull,
	)
	if tt.advance {
		setArtifact(1)
	}
	blockBlobs.Store(tt.blockBlobs)
	if tt.offline {
		srv.Close()
	}
	shared := &Fetcher{Network: newTestNetwork(t), Cache: f.Cache}
	second, err := shared.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: tt.interval},
	)
	if tt.failRefresh {
		assert.FatalIf(t, err == nil, "%s", "expected refresh to fail after registry shutdown")

		{
			got := mustReadFile(t, first.FS, "app.yaml")
			assert.FatalIf(t, got != "version: v1\n", "failed refresh damaged held content: %q", got)
		}
		return
	}
	assert.NoError(t, err)

	want := "version: v1\n"
	if tt.advance && !tt.offline {
		want = "version: v2\n"
		if second.Digest == first.Digest || second.Revision == first.Revision ||
			resolves.Load() <= beforeResolve ||
			pulls.Load() <= beforePull {
			t.Fatalf(
				"mutable tag was not refreshed: digest=%s revision=%s resolves=%d pulls=%d",
				second.Digest,
				second.Revision,
				resolves.Load()-beforeResolve,
				pulls.Load()-beforePull,
			)
		}
	} else if second.Digest != first.Digest || second.Revision != first.Revision || resolves.Load() != beforeResolve || pulls.Load() != beforePull {
		t.Fatalf(
			"cache hit changed digest/revision or requested content: resolves=%d pulls=%d",
			resolves.Load()-beforeResolve,
			pulls.Load()-beforePull,
		)
	}
	if second.Key != first.Key {
		t.Fatalf("acquisition key changed: %q -> %q", first.Key, second.Key)
	}
	{
		got := mustReadFile(t, second.FS, "app.yaml")
		assert.FatalIf(t, got != want, "second content = %q, want %q", got, want)
	}
}

type testFetcherFreshnessCaseCallbackInputs struct {
	resolves   *atomic.Int32
	pulls      *atomic.Int32
	blockBlobs *atomic.Bool
	handler    *http.Handler
}

func testFetcherFreshnessCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherFreshnessCaseCallbackInputs,
) {
	if strings.Contains(r.URL.Path, "/manifests/") {
		testCaseInput.resolves.
			Add(1)
	}
	if strings.Contains(r.URL.Path, "/blobs/") {
		testCaseInput.pulls.
			Add(1)
		if testCaseInput.blockBlobs.Load() {
			http.Error(w, "blob pulls disabled", http.StatusServiceUnavailable)
			return
		}
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}
