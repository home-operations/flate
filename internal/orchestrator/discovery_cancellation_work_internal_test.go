package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderTrees_DiscoveryCancellationWork(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml",
		fmt.Sprintf("configMapGenerator: [{name: remote, envs: [%s]}]\n", server.URL))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	var runs []Rendered
	go func() {
		var err error
		runs, err = renderTreesForTest(ctx, t, []Tree{{
			RepoRoot: root, Path: root,
			ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		}},
			Config{RecordWork: true, Concurrency: 2})
		done <- err
	}()
	<-entered
	cancel()
	assert.Equal(t, errors.Is(<-done, context.Canceled), true)
	assert.Equal(t, len(runs), 1)
	var acquisition manifest.WorkCounts
	for _, record := range runs[0].Result.Work {
		if record.Key.Phase == manifest.WorkAcquisition {
			acquisition.Add(&record.Counts)
		}
	}
	assert.Equal(t, acquisition.Failures, uint64(1))
	assert.Equal(t, acquisition.Attempts, acquisition.Completed+acquisition.Failures)
}
