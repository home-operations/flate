package helmchart

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetchHTTPChartCancellationDoesNotPublishCase(
	t *testing.T, backing *string,
	endpoint *string,
) {
	t.Helper()
	chartBytes := buildChartTarGz(t, "app-template", "1.0.0")
	started, canceled := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == (*endpoint) && attempts.Add(1) == 1 {
			_, _ = io.WriteString(w, "partial")
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
			close(canceled)
			return
		}
		if r.URL.Path == "/index.yaml" {
			_, _ = io.WriteString(w, helmRepoIndex(chartDigest(chartBytes)))
			return
		}
		_, _ = w.Write(chartBytes)
	}))
	t.Cleanup(srv.Close)
	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, sourcecache.New)
	r := httpRepo(srv.URL)
	fInputs := testInputs{repos: func(_, _ string) *manifest.HelmRepository { return r }}
	f := New(nil, Config{Cache: c, Network: newTestNetwork(t)})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := f.Fetch(
			ctx,
			helmChart("repo", "app-template", "1.0.0"),
			fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
			&sourcecache.Policy{},
		)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	select {
	case err := <-result:
		assert.FatalIf(
			t,
			!errors.Is(err, context.Canceled) || !errors.Is(err, manifest.ErrFlux),
			"canceled fetch: %v",
			err,
		)
	case <-time.After(5 * time.Second):
		t.Fatal("fetch ignored cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request outlived fetch")
	}
	art, err := f.Fetch(
		t.Context(),
		helmChart("repo", "app-template", "1.0.0"),
		fInputs.readSet(helmChart("repo", "app-template", "1.0.0")),
		&sourcecache.Policy{},
	)
	assert.NoError(t, err)

	data, err := fs.ReadFile(art.FS, "chart.tgz")
	if err != nil || !bytes.Equal(data, chartBytes) || attempts.Load() != 2 {
		t.Fatalf("retry: bytes=%d attempts=%d err=%v", len(data), attempts.Load(), err)
	}
}
