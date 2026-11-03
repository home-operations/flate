package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrapCase(
	t *testing.T,
	testCaseInput testRenderTreesCancellationJoinsBeforeCacheCloseHTTPBootstrapCaseInputs,
) {
	t.Helper()
	active, wantOrigErrors := 2, 1
	if testCaseInput.tc.headOnly {
		active, wantOrigErrors = 1, 0
	}
	entered, completed := make(chan struct{}, 2), make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		entered <- struct{}{}
		<-r.Context().Done()
		completed <- struct{}{}
	}))
	defer server.Close()
	var trees [2]Tree
	for i := range trees {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"flux.yaml",
			testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"),
		)
		testutil.WriteFile(
			t,
			root,
			"apps/kustomization.yaml",
			fmt.Sprintf("configMapGenerator: [{name: remote, envs: [%s/%d]}]\n", server.URL, i),
		)
		trees[i] = Tree{RepoRoot: root}
	}
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, HeadOnly: testCaseInput.tc.headOnly, Tree: Tree{}}
	if testCaseInput.tc.injected {
		var err error
		cfg.Cache, err = sourcecache.New(t.Context(), cfg.CacheDir)
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, cfg.Cache.Close()) })
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := RenderTrees(
			ctx,
			[]Tree{
				{
					RepoRoot:        trees[0].RepoRoot,
					Path:            trees[0].Path,
					SelfURLs:        trees[0].SelfURLs,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:           cfg.BaseInput,
					BaseInput:       cfg.Input,
					ExternalChanges: cfg.ExternalChanges,
				},
				{
					RepoRoot:        trees[1].RepoRoot,
					Path:            trees[1].Path,
					SelfURLs:        trees[1].SelfURLs,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
					ExternalChanges: cfg.ExternalChanges,
				},
			},
			cfg.Config,
		)
		done <- err
	}()
	for range active {
		<-entered
	}
	cancel()
	{
		{
			err := <-done
			assert.FatalIf(t, !errors.Is(err, context.Canceled) || errors.Is(err, fs.ErrClosed) ||
				strings.Count(err.Error(), "orig snapshot:") != wantOrigErrors ||
				strings.Count(
					err.Error(),
					"current snapshot:",
				) != 1, "HTTP bootstrap teardown lost a side or closed backing early: %v", err)
		}
	}
	for range active {
		<-completed
	}
	if testCaseInput.tc.injected {
		_, _, _, err := cfg.Cache.Blob(
			t.Context(),
			sourcecache.BlobKey{Content: "after-http-bootstrap"},
			sourcecache.Policy{},
			func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				_, err := io.WriteString(w, "live")
				return sourcecache.ProducerResult{}, err
			},
		)
		assert.NoError(t, err)

		assert.NoError(t, cfg.Cache.Close())
	}
	res, err := sourcecache.Sweep(t.Context(), cfg.CacheDir, sourcecache.SweepOpts{})
	{
		assert.FatalIf(
			t,
			err != nil || len(res.Errors) != 0,
			"HTTP bootstrap retained backing after both sides joined: %+v %v",
			res,
			err,
		)
	}
}
