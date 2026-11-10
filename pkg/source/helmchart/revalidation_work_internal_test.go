package helmchart

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testHelmRevalidationWork(t *testing.T) {
	t.Parallel()
	charts := map[string][]byte{}
	endpoints := map[string]*revalidationEndpoint{}
	var index strings.Builder
	index.WriteString("apiVersion: v1\nentries:\n")
	for _, name := range []string{"app-template", "other"} {
		charts[name] = buildChartTarGz(t, name, "1.0.0")
		endpoint := new(revalidationEndpoint)
		endpoint.reply.Store(&revalidationReply{body: charts[name], etag: `"` + name + `"`})
		endpoints["/"+name+"-1.0.0.tgz"] = endpoint
		fmt.Fprintf(&index, "  %s:\n  - name: %s\n    version: 1.0.0\n    digest: %s\n    urls: [%s-1.0.0.tgz]\n",
			name, name, chartDigest(charts[name]), name)
	}
	indexEndpoint := new(revalidationEndpoint)
	indexEndpoint.reply.Store(&revalidationReply{body: []byte(index.String()), etag: `"index"`})
	endpoints["/index.yaml"] = indexEndpoint
	synctest.Test(t, func(t *testing.T) {
		owner := cachetest.New(t, "", sourcecache.New)
		f := New(nil, Config{Cache: owner})
		gate := make(chan struct{})
		var hold atomic.Bool
		r := &httpRepository{HelmRepository: httpRepo("https://charts.test")}
		r.client = &http.Client{Transport: chartTransport(func(req *http.Request) (*http.Response, error) {
			if hold.Load() && req.URL.Path != "/index.yaml" {
				select {
				case <-gate:
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			}
			response := httptest.NewRecorder()
			endpoints[req.URL.Path].ServeHTTP(response, req)
			return response.Result(), nil
		})}
		for _, wave := range []string{"cold", "source-warm", "source-stale"} {
			maxAge := time.Hour
			if wave == "source-stale" {
				maxAge = 0
				hold.Store(true)
			}
			work, artifacts, join := startHelmRevalidationConsumers(t, f, r, maxAge)
			synctest.Wait()
			if wave == "source-stale" {
				close(gate)
			}
			join()
			assertHTTPChartWork(t, work, artifacts, wave)
		}
		assert.Equal(t, indexEndpoint.requests.Load(), int32(1))
		for _, path := range []string{"/app-template-1.0.0.tgz", "/other-1.0.0.tgz"} {
			endpoint := endpoints[path]
			assert.Equal(t, endpoint.requests.Load(), int32(2))
			assert.Equal(t, endpoint.bodies.Load(), int32(1))
			assert.Equal(t, endpoint.unchanged.Load(), int32(1))
		}
	})
}

func startHelmRevalidationConsumers(
	t *testing.T,
	f *Fetcher,
	r *httpRepository,
	maxAge time.Duration,
) (*[2][2]manifest.WorkRecorder, *[2][2]*store.SourceArtifact, func()) {
	t.Helper()
	var work [2][2]manifest.WorkRecorder
	var artifacts [2][2]*store.SourceArtifact
	var consumers sync.WaitGroup
	for cluster, clusterName := range []manifest.ClusterID{"alpha", "omega"} {
		for chart, name := range []string{"app-template", "other"} {
			work[cluster][chart].Key = manifest.WorkKey{
				Tree: manifest.TreeID{Cluster: clusterName, Side: manifest.TreeHead},
				Owner: testutil.HelmReleaseID(
					"flux-system",
					name,
				),
				Source: helmChart("repo", name, "1.0.0").Named(),
			}
			consumers.Go(func() {
				accounting := manifest.AcquisitionWork{Scope: work[cluster][chart].Scope()}
				indexWork := manifest.AcquisitionWork{Scope: accounting.Scope}
				artifact, err := f.fetchHTTPChart(
					t.Context(),
					r,
					helmChart("repo", name, "1.0.0"),
					&sourcecache.Policy{
						MaxAge: maxAge, Work: &accounting, HistoryWork: &indexWork,
					},
				)
				if err != nil {
					t.Errorf("fetch %s: %v", name, err)
					return
				}
				artifacts[cluster][chart] = artifact
				assertHTTPChartArtifact(t, artifact, name, buildChartTarGz(t, name, "1.0.0"))
			})
		}
	}
	return &work, &artifacts, consumers.Wait
}
