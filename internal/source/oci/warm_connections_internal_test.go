package oci

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_Freshness_WarmConnections(t *testing.T) {
	t.Parallel()
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	registry := newMutableFakeRegistry(t, artifact)

	var heads, manifests, layers, connections atomic.Int32
	handler := registry.server.Config.Handler
	registry.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherFreshnessWarmConnectionsCallback(
			w,
			r,
			testFetcherFreshnessWarmConnectionsCallbackInputs{
				heads:     &heads,
				manifests: &manifests,
				layers:    &layers,
				handler:   &handler,
			},
		)
	},
	)
	registry.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	registry.server.StartTLS()
	root := t.TempDir()
	repo := &manifest.OCIRepository{
		Name: "first", Namespace: "test", URL: "oci://" + mustURL(t, registry.server.URL).Host + "/app", Insecure: true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	for _, invocation := range []string{"cold", "reopened"} {
		f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New), Network: newTestNetwork(t)}
		beforeConnections := connections.Load()
		for operation := range 2 {
			beforeHeads, beforeManifests, beforeLayers := heads.Load(), manifests.Load(), layers.Load()
			request := *repo
			request.Name = fmt.Sprintf("source-%d", operation)
			art, err := fetchtest.Fetch(
				t.Context(),
				f.Fetch,
				&request,
				(testInputs{}).readSet(&request),
				&sourcecache.Policy{MaxAge: request.SourceInterval()},
				nil,
			)
			assert.NoError(t, err)

			{
				got := mustReadFile(t, art.FS, "app.yaml")
				assert.FatalIf(t, got != "version: v1\n", "content=%q", got)
			}
			h, m, l := heads.Load()-beforeHeads, manifests.Load()-beforeManifests, layers.Load()-beforeLayers
			t.Logf(
				"%s operation=%d: tag_HEAD=%d manifest_GET=%d layer_GET=%d accepted_connections=%d",
				invocation,
				operation,
				h,
				m,
				l,
				connections.Load()-beforeConnections,
			)
			wantLayers := int32(0)
			if invocation == "cold" && operation == 0 {
				wantLayers = 1
			}
			assert.ErrorIf(
				t,
				h != 1 || m != wantLayers || l != wantLayers,
				"%s operation=%d: HEAD/manifest GET/layer GET=%d/%d/%d, want 1/%d/%d",
				invocation,
				operation,
				h,
				m,
				l,
				wantLayers,
				wantLayers,
			)
		}
		{
			got := connections.Load() - beforeConnections
			assert.ErrorIf(
				t,
				got != 1,
				"%s: compatible sequential operations accepted %d connections, want 1",
				invocation,
				got,
			)
		}
		{
			err := f.Cache.Close()
			assert.NoError(t, err)
		}
	}
}

type testFetcherFreshnessWarmConnectionsCallbackInputs struct {
	heads     *atomic.Int32
	manifests *atomic.Int32
	layers    *atomic.Int32
	handler   *http.Handler
}

func testFetcherFreshnessWarmConnectionsCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherFreshnessWarmConnectionsCallbackInputs,
) {
	switch {
	case strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead:
		testCaseInput.heads.
			Add(1)
	case strings.Contains(r.URL.Path, "/manifests/"):
		testCaseInput.manifests.
			Add(1)
	case strings.Contains(r.URL.Path, "/blobs/"):
		testCaseInput.layers.
			Add(1)
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}
